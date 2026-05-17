"""Stage 1.5 generator: build commander_spellbook_combo_outcomes_v1.json.

Fetches the Commander Spellbook public variants API, filters to the variant_ids
present in repo/api/engine/data/combos/two_card_combos_v2.json, and writes an
SHA-pinnable outcome pack used by the (forthcoming) Stage 2 COMBO_ENABLER
engine layer.

Run once locally to materialize the data file; commit both the script and the
emitted pack. The pack records the script's own SHA-256 so that any future
re-generation can be audited against the committed source.

Closed-world rule: this script is the ONLY network boundary. The Stage 2 engine
layer never makes network calls; it loads the committed pack at import time.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


COMBO_OUTCOMES_V1_VERSION = "commander_spellbook_combo_outcomes_v1"
DEFAULT_API_BASE = "https://backend.commanderspellbook.com"
DEFAULT_ENDPOINT = "/variants/?limit=100"
DEFAULT_BULK_URL = "https://json.commanderspellbook.com/variants.json"
DEFAULT_USER_AGENT = "mtg-engine-stage15-combo-outcomes/1.0"
MIN_MATCH_RATIO = 0.80
MAX_BYTES = 5 * 1024 * 1024  # 5 MB hard ceiling


def _script_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def _load_target_variant_ids(v2_path: Path) -> Tuple[Set[str], int]:
    parsed = json.loads(v2_path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{v2_path} root is not an object")
    pairs = parsed.get("pairs")
    if not isinstance(pairs, list):
        raise RuntimeError(f"{v2_path} 'pairs' is not a list")
    target: Set[str] = set()
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        for vid in pair.get("variant_ids") or ():
            if isinstance(vid, str) and vid.strip():
                target.add(vid.strip())
    return target, len(pairs)


def _fetch_page(url: str, *, user_agent: str, timeout: int, retries: int) -> Dict[str, Any]:
    last_exc: Optional[BaseException] = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
            obj = json.loads(body)
            if not isinstance(obj, dict):
                raise RuntimeError(f"non-object response at {url}")
            return obj
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            retry_after_hdr = exc.headers.get("Retry-After") if exc.headers else None
            try:
                retry_after = float(retry_after_hdr) if retry_after_hdr else None
            except ValueError:
                retry_after = None
            if exc.code == 429:
                wait = retry_after if retry_after is not None else (10.0 * (2 ** attempt))
            else:
                wait = retry_after if retry_after is not None else (0.5 * (2 ** attempt))
            print(f"  retry attempt={attempt+1} after {wait:.1f}s (HTTP {exc.code})", file=sys.stderr)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            time.sleep(0.5 * (2 ** attempt))
    raise RuntimeError(f"unreachable retry exhaust at {url}: {last_exc!r}")


def _extract_outcome(variant: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    variant_id = variant.get("id")
    if not isinstance(variant_id, str) or not variant_id.strip():
        return None

    feature_names: List[str] = []
    seen_features: Set[str] = set()
    for prod in variant.get("produces") or ():
        if not isinstance(prod, dict):
            continue
        feature = prod.get("feature")
        if not isinstance(feature, dict):
            continue
        name = feature.get("name")
        if isinstance(name, str):
            token = " ".join(name.split())
            if token and token not in seen_features:
                seen_features.add(token)
                feature_names.append(token)

    label = "; ".join(feature_names) if feature_names else ""
    if not label:
        desc = variant.get("description")
        if isinstance(desc, str):
            first_line = desc.strip().splitlines()[0] if desc.strip() else ""
            if first_line:
                label = first_line[:200]
    if not label:
        return None

    oracle_ids: List[str] = []
    seen_oids: Set[str] = set()
    for use in variant.get("uses") or ():
        if not isinstance(use, dict):
            continue
        card = use.get("card")
        if not isinstance(card, dict):
            continue
        oid = card.get("oracleId")
        if isinstance(oid, str) and oid and oid not in seen_oids:
            seen_oids.add(oid)
            oracle_ids.append(oid)

    record: Dict[str, Any] = {
        "label": label,
        "partner_oracle_ids": sorted(oracle_ids),
    }

    for prereq_key in ("notablePrerequisites", "easyPrerequisites", "manaNeeded"):
        val = variant.get(prereq_key)
        if isinstance(val, str) and val.strip():
            record.setdefault("prerequisites", {})[prereq_key] = val.strip()

    return {"id": variant_id, "record": record}


def _iter_variants_paginated(
    api_base: str,
    endpoint: str,
    *,
    user_agent: str,
    timeout: int,
    request_delay: float,
    max_pages: int,
) -> Iterable[Dict[str, Any]]:
    next_url: Optional[str] = api_base.rstrip("/") + "/" + endpoint.lstrip("/")
    pages_seen = 0
    while next_url and pages_seen < max_pages:
        pages_seen += 1
        page = _fetch_page(next_url, user_agent=user_agent, timeout=timeout, retries=3)
        results = page.get("results")
        if not isinstance(results, list):
            raise RuntimeError(f"paginated response page {pages_seen} has no results list")
        for variant in results:
            if isinstance(variant, dict):
                yield variant
        nxt = page.get("next")
        next_url = nxt if isinstance(nxt, str) and nxt.strip() else None
        if next_url and request_delay > 0:
            time.sleep(request_delay)


def _iter_variants_bulk(bulk_url: str, *, user_agent: str, timeout: int) -> Iterable[Dict[str, Any]]:
    print(f"  bulk: GET {bulk_url}", file=sys.stderr)
    req = urllib.request.Request(bulk_url, headers={"User-Agent": user_agent, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    print(f"  bulk: downloaded {len(body)} bytes; parsing JSON", file=sys.stderr)
    obj = json.loads(body)
    if not isinstance(obj, dict):
        raise RuntimeError("bulk JSON root is not an object")
    variants = obj.get("variants")
    if not isinstance(variants, list):
        raise RuntimeError("bulk JSON missing variants list")
    print(f"  bulk: {len(variants)} variants in dump", file=sys.stderr)
    for variant in variants:
        if isinstance(variant, dict):
            yield variant


def _generate(
    *,
    api_base: str,
    endpoint: str,
    bulk_url: Optional[str],
    v2_path: Path,
    out_path: Path,
    timeout: int,
    request_delay: float,
    max_pages: int,
    user_agent: str,
    dry_run: bool,
) -> int:
    target_ids, pair_count = _load_target_variant_ids(v2_path)
    target_count = len(target_ids)
    print(f"target: {target_count} unique variant_ids across {pair_count} two-card pairs", file=sys.stderr)
    if target_count == 0:
        print("ERROR: empty target set; check v2 file", file=sys.stderr)
        return 2

    outcomes: Dict[str, Dict[str, Any]] = {}
    variants_seen = 0
    t0 = time.time()
    if bulk_url:
        source_url_recorded = bulk_url
        variant_iter: Iterable[Dict[str, Any]] = _iter_variants_bulk(
            bulk_url, user_agent=user_agent, timeout=timeout
        )
    else:
        source_url_recorded = api_base.rstrip("/") + "/" + endpoint.lstrip("/").split("?", 1)[0]
        variant_iter = _iter_variants_paginated(
            api_base, endpoint,
            user_agent=user_agent, timeout=timeout,
            request_delay=request_delay, max_pages=max_pages,
        )

    for variant in variant_iter:
        variants_seen += 1
        vid = variant.get("id")
        if not isinstance(vid, str) or vid not in target_ids:
            continue
        extracted = _extract_outcome(variant)
        if extracted is None:
            continue
        outcomes[extracted["id"]] = extracted["record"]
        if len(outcomes) >= target_count:
            break
        if variants_seen % 10000 == 0:
            elapsed = time.time() - t0
            print(
                f"  scanned={variants_seen} matched={len(outcomes)}/{target_count} elapsed={elapsed:.1f}s",
                file=sys.stderr,
            )

    matched = len(outcomes)
    ratio = matched / target_count if target_count else 0.0
    elapsed = time.time() - t0
    print(
        f"DONE: variants_seen={variants_seen} matched={matched}/{target_count} "
        f"ratio={ratio:.4f} elapsed={elapsed:.1f}s",
        file=sys.stderr,
    )

    if ratio < MIN_MATCH_RATIO:
        print(
            f"HALT: match ratio {ratio:.4f} below floor {MIN_MATCH_RATIO}; will not write file",
            file=sys.stderr,
        )
        return 4

    payload: Dict[str, Any] = {
        "version": COMBO_OUTCOMES_V1_VERSION,
        "source_url": source_url_recorded,
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "generator_script_sha": _script_sha256(),
        "match_stats": {
            "target_variant_ids": target_count,
            "matched_variant_ids": matched,
            "match_ratio": round(ratio, 6),
            "two_card_pairs_in_v2": pair_count,
        },
        "outcomes": {vid: outcomes[vid] for vid in sorted(outcomes)},
    }

    text = _stable_json(payload)
    size = len(text.encode("utf-8"))
    if size > MAX_BYTES:
        print(f"HALT: emitted size {size} exceeds ceiling {MAX_BYTES}; revisit filter scope", file=sys.stderr)
        return 5

    if dry_run:
        print(f"DRY-RUN: would write {size} bytes to {out_path}", file=sys.stderr)
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload_bytes = text.encode("utf-8")
    out_path.write_bytes(payload_bytes)
    digest = hashlib.sha256(payload_bytes).hexdigest()
    print(f"WROTE: {out_path}  bytes={len(payload_bytes)}  sha256={digest}", file=sys.stderr)
    return 0


def _parse_args(argv: List[str]) -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[3]
    default_v2 = repo_root / "api" / "engine" / "data" / "combos" / "two_card_combos_v2.json"
    default_out = repo_root / "api" / "engine" / "data" / "combos" / "commander_spellbook_combo_outcomes_v1.json"
    parser = argparse.ArgumentParser(description="Generate commander_spellbook_combo_outcomes_v1 data pack.")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--bulk-url", default=DEFAULT_BULK_URL,
                        help="Bulk JSON dump URL; preferred over paginated API. Pass empty string to force pagination.")
    parser.add_argument("--v2-path", default=str(default_v2))
    parser.add_argument("--out-path", default=str(default_out))
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--request-delay", type=float, default=0.0)
    parser.add_argument("--max-pages", type=int, default=2000)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(list(argv if argv is not None else sys.argv[1:]))
    bulk_url = args.bulk_url.strip() if isinstance(args.bulk_url, str) else None
    if not bulk_url:
        bulk_url = None
    try:
        return _generate(
            api_base=args.api_base,
            endpoint=args.endpoint,
            bulk_url=bulk_url,
            v2_path=Path(args.v2_path),
            out_path=Path(args.out_path),
            timeout=args.timeout,
            request_delay=args.request_delay,
            max_pages=args.max_pages,
            user_agent=args.user_agent,
            dry_run=args.dry_run,
        )
    except (RuntimeError, OSError, urllib.error.URLError, urllib.error.HTTPError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
