"""
scryfall_sets_watcher_v1 — Mega-task v3 Phase 1.

Scryfall API client + new-set detector. Reads the public /sets endpoint
once per scheduled run, filters out sets whose 3-letter code is already
in our known_set_codes ledger, returns any whose `released_at` is past
today's date. The orchestrating CLI (`tools/check_new_sets.py`) exits 1
when one or more new sets are detected so a parent scheduler can
trigger the ingestion path.

Rate limit policy (per Scryfall guidelines):
  - max 10 req/s; we use 1 req per 100ms to be conservative.
  - exponential backoff on 429 / 5xx: 1s, 2s, 4s up to 3 retries.

Public API:
  - fetch_set_index() -> list[dict]
  - find_new_sets(known_codes: set[str], today_iso: str) -> list[dict]
  - load_known_set_codes() -> set[str]
  - save_known_set_codes(codes: set[str]) -> None
  - initialize_known_set_codes_from_corpus(db_path) -> set[str]
    (used once, on first install, to seed the ledger from cards_raw)
  - KNOWN_SET_CODES_PATH — canonical ledger location
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


SCRYFALL_SETS_URL = "https://api.scryfall.com/sets"
USER_AGENT = "mtg-engine-mega-task-v3-watcher/1.0"

KNOWN_SET_CODES_PATH = (
    Path(__file__).resolve().parents[1]
    / "data" / "scripts" / "known_set_codes_v1.json"
)

_RATE_LIMIT_DELAY_S = 0.1
_RETRY_BACKOFFS_S = (1.0, 2.0, 4.0)


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


# ============================================================
# Scryfall API client.
# ============================================================


def fetch_set_index(
    url: str = SCRYFALL_SETS_URL,
    http_get: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Fetch the Scryfall /sets list. Returns the `data` array.

    `http_get` lets tests inject a mocked requests.get callable.
    """
    if http_get is None:
        import requests
        http_get = requests.get

    last_exc: Optional[Exception] = None
    for backoff in (0.0,) + _RETRY_BACKOFFS_S:
        if backoff > 0:
            time.sleep(backoff)
        try:
            resp = http_get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
            if hasattr(resp, "status_code") and resp.status_code == 429:
                last_exc = RuntimeError("429 rate limited")
                continue
            if hasattr(resp, "status_code") and 500 <= resp.status_code < 600:
                last_exc = RuntimeError(f"{resp.status_code} server error")
                continue
            if hasattr(resp, "raise_for_status"):
                resp.raise_for_status()
            data = resp.json()
            return list(data.get("data") or [])
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("fetch_set_index unreachable")


# ============================================================
# New-set detection.
# ============================================================


def find_new_sets(
    set_index: List[Dict[str, Any]],
    known_codes: Set[str],
    today_iso: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return sets whose code is not in `known_codes` and whose
    `released_at` is past today.

    Set entries with no `released_at` (still-unreleased preview sets)
    are skipped — we don't ingest cards before their official release.
    """
    today = today_iso or _today_iso()
    out: List[Dict[str, Any]] = []
    for s in set_index:
        code = (s.get("code") or "").strip().lower()
        released = (s.get("released_at") or "").strip()
        if not code or not released:
            continue
        if code in known_codes:
            continue
        # released_at format is ISO YYYY-MM-DD.
        if released > today:
            continue
        out.append(s)
    return out


# ============================================================
# Known-codes ledger.
# ============================================================


def load_known_set_codes(
    path: Optional[Path] = None,
) -> Set[str]:
    """Load the known-set-codes ledger. Returns an empty set if missing."""
    p = path or KNOWN_SET_CODES_PATH
    if not p.is_file():
        return set()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        codes = data.get("known_codes") if isinstance(data, dict) else data
        if isinstance(codes, list):
            return {c.strip().lower() for c in codes if isinstance(c, str)}
    except (json.JSONDecodeError, OSError):
        pass
    return set()


def save_known_set_codes(
    codes: Set[str],
    path: Optional[Path] = None,
) -> None:
    """Write the ledger atomically (write to temp + rename)."""
    p = path or KNOWN_SET_CODES_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    payload = {
        "version": "scryfall_sets_watcher_v1.0",
        "written_at": datetime.now(timezone.utc).isoformat(),
        "known_codes": sorted(c.strip().lower() for c in codes if c),
    }
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(p)


def initialize_known_set_codes_from_corpus(
    db_path: Path,
) -> Set[str]:
    """Read all set codes present in cards_raw and return as a set."""
    con = sqlite3.connect(str(db_path))
    try:
        rows = con.execute(
            "SELECT DISTINCT json_extract(json, '$.set') FROM cards_raw "
            "WHERE json IS NOT NULL"
        ).fetchall()
    finally:
        con.close()
    return {
        (r[0] or "").strip().lower()
        for r in rows
        if r[0]
    }
