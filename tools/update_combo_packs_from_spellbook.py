from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


SPELLBOOK_VARIANTS_V1_VERSION = "commander_spellbook_variants_v1"
TWO_CARD_COMBOS_V2_VERSION = "two_card_combos_v2"


def _stable_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _canonical_card_key(value: Any) -> str | None:
    if isinstance(value, dict):
        oracle_id = _nonempty_str(value.get("oracle_id"))
        if oracle_id is not None:
            return oracle_id.lower()
        card_name = _nonempty_str(value.get("name"))
        if card_name is not None:
            return " ".join(card_name.split()).lower()
        return None

    token = _nonempty_str(value)
    if token is None:
        return None
    return " ".join(token.split()).lower()


def _fetch_json(url: str, *, timeout_seconds: int) -> Any:
    request = Request(url=url, headers={"Accept": "application/json", "User-Agent": "mtg-engine-combo-updater/1.0"})
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def _extract_rows_and_next(payload: Any) -> Tuple[List[Any], str | None]:
    if isinstance(payload, list):
        return list(payload), None

    if not isinstance(payload, dict):
        return [], None

    rows_raw = payload.get("results")
    if not isinstance(rows_raw, list):
        rows_raw = payload.get("data") if isinstance(payload.get("data"), list) else []

    next_url = payload.get("next")
    if not isinstance(next_url, str) or next_url.strip() == "":
        next_url = None

    return list(rows_raw), next_url


def _normalize_variant(row: Any, *, index: int) -> Dict[str, Any] | None:
    if not isinstance(row, dict):
        return None

    variant_id = _nonempty_str(row.get("variant_id")) or _nonempty_str(row.get("id"))
    if variant_id is None:
        variant_id = f"SPELLBOOK_VARIANT_{index + 1:07d}"

    cards_raw = row.get("cards")
    if not isinstance(cards_raw, list):
        cards_raw = row.get("uses") if isinstance(row.get("uses"), list) else []

    cards = sorted({
        key
        for key in (_canonical_card_key(item) for item in cards_raw)
        if key is not None
    })
    if len(cards) < 2:
        return None

    normalized: Dict[str, Any] = {
        "variant_id": variant_id,
        "cards": cards,
    }

    result_value = _nonempty_str(row.get("result")) or _nonempty_str(row.get("produces"))
    if result_value is not None:
        normalized["result"] = result_value

    tags_raw = row.get("tags")
    if isinstance(tags_raw, list):
        tags = sorted({
            token
            for token in (_nonempty_str(item) for item in tags_raw)
            if token is not None
        })
        if len(tags) > 0:
            normalized["tags"] = tags

    return normalized


def _build_variants_payload(rows: List[Any], *, generated_from: str) -> Dict[str, Any]:
    normalized_rows: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        normalized = _normalize_variant(row, index=index)
        if normalized is not None:
            normalized_rows.append(normalized)

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in normalized_rows:
        row_key = str(row.get("variant_id") or "")
        deduped[row_key] = row

    variants = [deduped[key] for key in sorted(deduped.keys())]

    return {
        "version": SPELLBOOK_VARIANTS_V1_VERSION,
        "source": "commander_spellbook_api",
        "generated_from": generated_from,
        "variants": variants,
    }


def _build_two_card_payload(variants_payload: Dict[str, Any]) -> Dict[str, Any]:
    variants = variants_payload.get("variants") if isinstance(variants_payload.get("variants"), list) else []

    grouped_variant_ids: Dict[Tuple[str, str], set[str]] = {}
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        variant_id = _nonempty_str(variant.get("variant_id"))
        cards_raw = variant.get("cards")
        if variant_id is None or not isinstance(cards_raw, list):
            continue

        cards = sorted({
            key
            for key in (_canonical_card_key(item) for item in cards_raw)
            if key is not None
        })
        if len(cards) != 2:
            continue

        pair_key = (cards[0], cards[1])
        grouped_variant_ids.setdefault(pair_key, set()).add(variant_id)

    pairs = [
        {
            "a": pair_key[0],
            "b": pair_key[1],
            "variant_ids": sorted(variant_ids),
        }
        for pair_key, variant_ids in sorted(grouped_variant_ids.items(), key=lambda item: item[0])
    ]

    return {
        "version": TWO_CARD_COMBOS_V2_VERSION,
        "pairs": pairs,
    }


def _read_file_text(path: Path) -> str | None:
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent), newline="\n") as tmp:
        tmp.write(text)
        tmp_path = Path(tmp.name)

    os.replace(str(tmp_path), str(path))


def _commit_outputs(
    outputs_by_path: Dict[Path, str],
    *,
    allow_partial_write: bool,
) -> None:
    previous_by_path: Dict[Path, str | None] = {path: _read_file_text(path) for path in outputs_by_path.keys()}

    written_paths: List[Path] = []
    try:
        for path in sorted(outputs_by_path.keys(), key=lambda item: str(item)):
            _write_atomic(path, outputs_by_path[path])
            written_paths.append(path)
    except Exception:
        if not allow_partial_write:
            for path in reversed(written_paths):
                previous = previous_by_path.get(path)
                if previous is None:
                    if path.exists():
                        path.unlink()
                else:
                    _write_atomic(path, previous)
        raise


def _normalize_endpoint(api_base: str, endpoint: str) -> str:
    base = api_base.rstrip("/") + "/"
    route = endpoint.lstrip("/")
    return urljoin(base, route)


def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline updater: fetch Commander Spellbook variants and deterministically write local combo packs."
    )
    parser.add_argument("--api-base", default="https://backend.commanderspellbook.com")
    parser.add_argument("--endpoint", default="/variants/")
    parser.add_argument("--output-dir", default=str(Path(__file__).resolve().parents[1] / "api" / "engine" / "data" / "combos"))
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-partial-write", action="store_true")
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))

    start_url = _normalize_endpoint(args.api_base, args.endpoint)

    rows: List[Any] = []
    visited_urls: set[str] = set()
    next_url: str | None = start_url

    try:
        for _ in range(max(1, int(args.max_pages))):
            if next_url is None:
                break
            if next_url in visited_urls:
                break

            visited_urls.add(next_url)
            payload = _fetch_json(next_url, timeout_seconds=max(1, int(args.timeout_seconds)))
            page_rows, page_next = _extract_rows_and_next(payload)
            rows.extend(page_rows)
            next_url = page_next

        if len(rows) == 0:
            raise RuntimeError("Commander Spellbook API returned zero variants rows")
    except (RuntimeError, ValueError, HTTPError, URLError, json.JSONDecodeError) as exc:
        print(f"ERROR: unable to fetch Commander Spellbook variants: {exc}", file=sys.stderr)
        return 1

    variants_payload = _build_variants_payload(rows, generated_from=args.endpoint)
    two_card_payload = _build_two_card_payload(variants_payload)

    output_dir = Path(args.output_dir)
    variants_path = output_dir / "commander_spellbook_variants_v1.json"
    two_card_path = output_dir / "two_card_combos_v2.json"

    variants_text = _stable_json_dumps(variants_payload) + "\n"
    two_card_text = _stable_json_dumps(two_card_payload) + "\n"

    previous_variants = _read_file_text(variants_path)
    previous_two_card = _read_file_text(two_card_path)

    new_variants_hash = _sha256_text(variants_text)
    new_two_card_hash = _sha256_text(two_card_text)
    old_variants_hash = _sha256_text(previous_variants) if previous_variants is not None else None
    old_two_card_hash = _sha256_text(previous_two_card) if previous_two_card is not None else None

    if not args.dry_run:
        try:
            _commit_outputs(
                {
                    variants_path: variants_text,
                    two_card_path: two_card_text,
                },
                allow_partial_write=bool(args.allow_partial_write),
            )
        except Exception as exc:
            print(f"ERROR: failed to write combo packs: {exc}", file=sys.stderr)
            return 1

    variants_count = len(variants_payload.get("variants") or [])
    two_card_count = len(two_card_payload.get("pairs") or [])

    print(f"variants_count={variants_count}")
    print(f"two_card_pairs_count={two_card_count}")
    print(
        "commander_spellbook_variants_v1.json "
        f"old_sha256={old_variants_hash or 'none'} new_sha256={new_variants_hash} "
        f"changed={old_variants_hash != new_variants_hash}"
    )
    print(
        "two_card_combos_v2.json "
        f"old_sha256={old_two_card_hash or 'none'} new_sha256={new_two_card_hash} "
        f"changed={old_two_card_hash != new_two_card_hash}"
    )
    print(f"dry_run={bool(args.dry_run)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
