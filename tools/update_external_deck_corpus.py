"""External deck corpus ingestion (Phase 5a.2.1 — Archidekt single-source MVP).

Fetches public Archidekt decks, normalizes each to the corpus pack shape, and
writes the deterministic JSON pack at
``repo/api/engine/data/calibration/external_decks_v1.json``.

The corpus is a **calibration-only** artifact per
``05_VALIDATION/CALIBRATION_BOUNDARY.md``: the runtime engine never reads it
directly. The pack's manifest entry has ``calibration_only: true`` so the
runtime loader (``iter_runtime_pack_entries``) skips it.

Single source for the MVP (Archidekt); 5a.3+ will extend to additional sources.

Manifest auto-refresh (sub-task 5a.2.2): on successful pack rewrite, also
refresh ``external_decks_v1.sha256`` in
``repo/api/engine/data/packs/curated_pack_manifest_v1.json``. Four guards
skip: dry-run, all-fetches-failed, byte-identical-pack, --no-update-manifest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


EXTERNAL_DECKS_V1_VERSION = "external_decks_v1"
EXTERNAL_DECKS_PACK_ID = "external_decks_v1"
EXTERNAL_DECKS_PACK_FILENAME = "external_decks_v1.json"

CURATED_PACK_MANIFEST_V1_REL_PATH = (
    Path("api") / "engine" / "data" / "packs" / "curated_pack_manifest_v1.json"
)
CURATED_PACK_MANIFEST_V1_DEFAULT_PATH = (
    Path(__file__).resolve().parents[1] / CURATED_PACK_MANIFEST_V1_REL_PATH
)

ARCHIDEKT_API_BASE_DEFAULT = "https://archidekt.com"
ARCHIDEKT_DECK_ENDPOINT_DEFAULT = "/api/decks/"
ARCHIDEKT_REQUEST_DELAY_SECONDS_DEFAULT = 0.5
ARCHIDEKT_TIMEOUT_SECONDS_DEFAULT = 30
ARCHIDEKT_MAX_DECKS_DEFAULT = 500

ENGINE_VERSION_AT_INGEST_DEFAULT = "post-2D"

EXIT_SUCCESS = 0
EXIT_PARTIAL_FAILURE = 1
EXIT_TOTAL_FAILURE = 2


def _stable_json_dumps(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _utcnow_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _nonempty_str(value):
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _canonical_oracle_id(value):
    token = _nonempty_str(value)
    if token is None:
        return None
    return token.lower()


def _fetch_json(url, *, timeout_seconds):
    request = Request(
        url=url,
        headers={
            "Accept": "application/json",
            "User-Agent": "mtg-engine-external-deck-corpus/1.0",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def _extract_archidekt_deck_id(payload):
    raw = payload.get("id")
    if isinstance(raw, int) and not isinstance(raw, bool):
        return str(raw)
    return _nonempty_str(raw)


def _extract_archidekt_in_deck_categories(payload):
    cats_raw = payload.get("categories")
    if not isinstance(cats_raw, list):
        return {"Commander"}
    out = set()
    for entry in cats_raw:
        if not isinstance(entry, dict):
            continue
        if entry.get("includedInDeck") is True:
            name_token = _nonempty_str(entry.get("name"))
            if name_token is not None:
                out.add(name_token)
    out.add("Commander")
    return out


def _extract_archidekt_bracket(payload):
    raw = payload.get("edhBracket")
    if isinstance(raw, int) and not isinstance(raw, bool) and 1 <= raw <= 5:
        return f"B{raw}"
    return None


def _compute_dedup_hash(*, commander_oracle_id, deck_oracle_ids, self_reported_bracket):
    canonical = _stable_json_dumps({
        "bracket": self_reported_bracket,
        "commander_oracle_id": commander_oracle_id,
        "deck_oracle_ids": sorted(deck_oracle_ids),
    })
    return _sha256_text(canonical)


def _normalize_archidekt_deck(payload, *, fetched_at_utc, engine_version_at_ingest, snapshot_id=None, resolver_fn=None):
    _ = snapshot_id, resolver_fn
    if not isinstance(payload, dict):
        return None, []
    deck_id = _extract_archidekt_deck_id(payload)
    if deck_id is None:
        return None, []
    in_deck_categories = _extract_archidekt_in_deck_categories(payload)
    raw_cards = payload.get("cards")
    if not isinstance(raw_cards, list) or len(raw_cards) == 0:
        return None, []
    unresolved = []
    commander_oracle_ids = []
    deck_oracle_ids = []
    original_names = set()
    for entry in raw_cards:
        if not isinstance(entry, dict):
            continue
        cats_raw = entry.get("categories")
        cats = {c for c in cats_raw if isinstance(c, str)} if isinstance(cats_raw, list) else set()
        if not (cats & in_deck_categories):
            continue
        card_obj = entry.get("card")
        oracle_card = card_obj.get("oracleCard") if isinstance(card_obj, dict) else None
        oid = None
        card_name = None
        if isinstance(oracle_card, dict):
            oid = _canonical_oracle_id(oracle_card.get("uid"))
            card_name = _nonempty_str(oracle_card.get("name"))
        if oid is None:
            unresolved.append(card_name or f"missing_oracle_id_in_card_entry_{entry.get('id', '?')}")
            continue
        if card_name is not None:
            original_names.add(card_name)
        qty_raw = entry.get("quantity", 1)
        qty = qty_raw if isinstance(qty_raw, int) and not isinstance(qty_raw, bool) and qty_raw >= 1 else 1
        if "Commander" in cats:
            commander_oracle_ids.append(oid)
        else:
            for _ in range(qty):
                deck_oracle_ids.append(oid)
    if len(unresolved) > 0 or len(commander_oracle_ids) == 0 or len(deck_oracle_ids) == 0:
        return None, sorted(set(unresolved))
    self_reported_bracket = _extract_archidekt_bracket(payload)
    primary_commander_oracle_id = sorted(commander_oracle_ids)[0]
    dedup_hash = _compute_dedup_hash(
        commander_oracle_id=primary_commander_oracle_id,
        deck_oracle_ids=deck_oracle_ids,
        self_reported_bracket=self_reported_bracket,
    )
    name_token = _nonempty_str(payload.get("name"))
    description_token = _nonempty_str(payload.get("description"))
    description_excerpt = description_token[:280] if description_token is not None else None
    source_url = f"https://archidekt.com/decks/{deck_id}/"
    source_metadata = {
        "archidekt_deck_id": deck_id,
        "engine_version_at_ingest": engine_version_at_ingest,
        "original_card_names": sorted(original_names),
    }
    if name_token is not None:
        source_metadata["name"] = name_token
    if description_excerpt is not None:
        source_metadata["description_excerpt"] = description_excerpt
    normalized = {
        "commander_oracle_ids": sorted(commander_oracle_ids),
        "dedup_hash": dedup_hash,
        "deck_id": f"ARCHIDEKT_{deck_id}",
        "deck_oracle_ids": sorted(deck_oracle_ids),
        "engine_assigned_bracket": None,
        "fetched_at_utc": fetched_at_utc,
        "self_reported_bracket": self_reported_bracket,
        "source": "archidekt",
        "source_metadata": source_metadata,
        "source_url": source_url,
    }
    return normalized, []


def _build_decks_payload(decks, *, source, generated_from):
    deduped = {}
    for deck in decks:
        if not isinstance(deck, dict):
            continue
        dedup_hash = _nonempty_str(deck.get("dedup_hash"))
        if dedup_hash is None:
            continue
        if dedup_hash not in deduped:
            deduped[dedup_hash] = deck
    sorted_decks = [deduped[key] for key in sorted(deduped.keys())]
    return {
        "decks": sorted_decks,
        "generated_from": generated_from,
        "pack_id": EXTERNAL_DECKS_PACK_ID,
        "source": source,
        "version": EXTERNAL_DECKS_V1_VERSION,
    }


def _read_file_text(path):
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def _refresh_manifest_sha256_v1(*, manifest_path, pack_id, new_sha256):
    raw = manifest_path.read_text(encoding="utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise KeyError(f"manifest at {manifest_path} is not a JSON object")
    packs = parsed.get("packs")
    if not isinstance(packs, list):
        raise KeyError(f"manifest at {manifest_path} missing 'packs' list")
    matches = [
        (i, e) for i, e in enumerate(packs)
        if isinstance(e, dict) and e.get("pack_id") == pack_id
    ]
    if len(matches) == 0:
        raise KeyError(f"no manifest entry with pack_id={pack_id!r}")
    if len(matches) > 1:
        raise KeyError(f"expected exactly 1 manifest entry with pack_id={pack_id!r}, found {len(matches)}")
    index, entry = matches[0]
    old_sha = entry.get("sha256")
    if old_sha == new_sha256:
        return new_sha256, False
    entry["sha256"] = new_sha256
    packs[index] = entry
    text = json.dumps(parsed, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    _write_atomic(manifest_path, text)
    return old_sha if isinstance(old_sha, str) else None, True


def _write_atomic(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent), newline="\n") as tmp:
        tmp.write(text)
        tmp_path = Path(tmp.name)
    os.replace(str(tmp_path), str(path))


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-base", default=ARCHIDEKT_API_BASE_DEFAULT)
    parser.add_argument("--endpoint", default=ARCHIDEKT_DECK_ENDPOINT_DEFAULT)
    parser.add_argument("--output-dir", default=str(Path(__file__).resolve().parents[1] / "api" / "engine" / "data" / "calibration"))
    parser.add_argument("--deck-ids", nargs="*", default=[])
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--engine-version-pin", default=ENGINE_VERSION_AT_INGEST_DEFAULT)
    parser.add_argument("--request-delay-seconds", type=float, default=ARCHIDEKT_REQUEST_DELAY_SECONDS_DEFAULT)
    parser.add_argument("--timeout-seconds", type=int, default=ARCHIDEKT_TIMEOUT_SECONDS_DEFAULT)
    parser.add_argument("--max-decks", type=int, default=ARCHIDEKT_MAX_DECKS_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-update-manifest", action="store_true")
    parser.add_argument("--manifest-path", default=str(CURATED_PACK_MANIFEST_V1_DEFAULT_PATH))
    return parser.parse_args(argv)


def _fetch_deck_payloads(*, deck_ids, api_base, endpoint, request_delay_seconds, timeout_seconds, max_decks):
    base = api_base.rstrip("/") + "/"
    route = endpoint.lstrip("/").rstrip("/") + "/"
    payloads = []
    fetch_failures = []
    capped_ids = deck_ids[:max_decks]
    for index, deck_id in enumerate(capped_ids):
        if index > 0 and request_delay_seconds > 0:
            time.sleep(request_delay_seconds)
        url = urljoin(urljoin(base, route), str(deck_id) + "/")
        try:
            payload = _fetch_json(url, timeout_seconds=timeout_seconds)
        except (HTTPError, URLError) as exc:
            fetch_failures.append(f"{deck_id}: {exc}")
            continue
        except Exception as exc:
            fetch_failures.append(f"{deck_id}: {exc}")
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
        else:
            fetch_failures.append(f"{deck_id}: unexpected response shape")
    return payloads, fetch_failures


def main(argv):
    args = _parse_args(argv)
    fetched_at_utc = _utcnow_iso()
    output_path = Path(args.output_dir) / EXTERNAL_DECKS_PACK_FILENAME
    if len(args.deck_ids) == 0:
        if not args.dry_run and not output_path.is_file():
            initial = _build_decks_payload([], source="none", generated_from="initial-empty-pack-no-live-fetch")
            text = _stable_json_dumps(initial) + "\n"
            _write_atomic(output_path, text)
        print(f"deck_ids=0 dry_run={args.dry_run} output={output_path} exit=0")
        return EXIT_SUCCESS
    try:
        payloads, fetch_failures = _fetch_deck_payloads(
            deck_ids=args.deck_ids,
            api_base=args.api_base,
            endpoint=args.endpoint,
            request_delay_seconds=args.request_delay_seconds,
            timeout_seconds=args.timeout_seconds,
            max_decks=args.max_decks,
        )
        normalized = []
        per_deck_unresolved = []
        for payload in payloads:
            normalized_deck, unresolved = _normalize_archidekt_deck(
                payload,
                fetched_at_utc=fetched_at_utc,
                engine_version_at_ingest=args.engine_version_pin,
                snapshot_id=args.snapshot_id,
            )
            if normalized_deck is None:
                per_deck_unresolved.append((str(payload.get("id") or ""), unresolved))
                continue
            normalized.append(normalized_deck)
        bare_source = "archidekt"
        bare_generated_from = args.endpoint
        wrapped = _build_decks_payload(normalized, source=bare_source, generated_from=bare_generated_from)
        text = _stable_json_dumps(wrapped) + "\n"
        previous_text = _read_file_text(output_path)
        sha_before = _sha256_text(previous_text) if previous_text is not None else "none"
        sha_after = _sha256_text(text)
        changed = sha_before != sha_after
        had_failures = len(fetch_failures) > 0 or len(per_deck_unresolved) > 0
        skip_write_to_preserve = (len(normalized) == 0 and had_failures and previous_text is not None)
        if not args.dry_run and not skip_write_to_preserve:
            _write_atomic(output_path, text)
        manifest_old_sha = None
        manifest_changed = False
        manifest_skipped_reason = None
        if args.dry_run:
            manifest_skipped_reason = "dry_run"
        elif skip_write_to_preserve:
            manifest_skipped_reason = "all_fetches_failed_pack_preserved"
        elif args.no_update_manifest:
            manifest_skipped_reason = "no_update_manifest_flag"
        else:
            manifest_old_sha, manifest_changed = _refresh_manifest_sha256_v1(
                manifest_path=Path(args.manifest_path),
                pack_id=EXTERNAL_DECKS_PACK_ID,
                new_sha256=sha_after,
            )
        partial_failure = had_failures
        print(
            f"deck_ids={len(args.deck_ids)} "
            f"normalized={len(normalized)} "
            f"fetch_failures={len(fetch_failures)} "
            f"normalize_drops={len(per_deck_unresolved)} "
            f"old_sha256={sha_before} new_sha256={sha_after} changed={changed} "
            f"dry_run={args.dry_run} "
            f"manifest_old_sha={manifest_old_sha if manifest_old_sha is not None else 'none'} "
            f"manifest_new_sha={sha_after} "
            f"manifest_changed={manifest_changed} "
            f"manifest_skipped_reason={manifest_skipped_reason if manifest_skipped_reason is not None else 'none'}"
        )
        if partial_failure:
            return EXIT_PARTIAL_FAILURE
        return EXIT_SUCCESS
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_TOTAL_FAILURE


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
