"""
scryfall_set_ingest_v1 — Mega-task v3 Phase 2.

Given a Scryfall 3-letter set code, fetch the set's cards via the
Scryfall API, diff them against the current corpus, and append new
cards to the cards + cards_raw tables.

Public API:
  - fetch_set_cards(set_code, http_get=None) -> list[dict]
  - diff_against_corpus(cards, db_path, target_snapshot_id) -> dict
  - ingest_new_set(set_code, db_path, target_snapshot_id, ...) -> dict

The ingestion is atomic per snapshot — either all rows insert or the
transaction rolls back. Idempotent: re-running on the same set yields
no new rows (everything is in the reprints/errata buckets).
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


SCRYFALL_SEARCH_URL = "https://api.scryfall.com/cards/search"
USER_AGENT = "mtg-engine-mega-task-v3-ingest/1.0"

_RATE_LIMIT_DELAY_S = 0.1
_RETRY_BACKOFFS_S = (1.0, 2.0, 4.0)


def _serialize_list(value: Any) -> str:
    """JSON-serialize a list value to match the cards table convention."""
    if value is None:
        return ""
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)


# ============================================================
# Scryfall card-search client (paginated).
# ============================================================


def fetch_set_cards(
    set_code: str,
    http_get: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Fetch all cards for a set via paginated Scryfall search.

    Uses `q=set:<code>&unique=cards` so we get one entry per oracle_id.
    Follows the `has_more` / `next_page` chain to retrieve all pages.
    Honors the rate-limit policy (100ms between requests, 1s/2s/4s
    exponential backoff on 429 / 5xx).
    """
    if http_get is None:
        import requests
        http_get = requests.get

    cards: List[Dict[str, Any]] = []
    url: Optional[str] = SCRYFALL_SEARCH_URL
    params: Optional[Dict[str, Any]] = {
        "q": f"set:{set_code.lower()}",
        "unique": "cards",
    }
    while url:
        last_exc: Optional[Exception] = None
        page_data: Optional[Dict[str, Any]] = None
        for backoff in (0.0,) + _RETRY_BACKOFFS_S:
            if backoff > 0:
                time.sleep(backoff)
            try:
                resp = http_get(
                    url, params=params,
                    headers={"User-Agent": USER_AGENT}, timeout=30,
                )
                if hasattr(resp, "status_code"):
                    if resp.status_code == 404:
                        return []   # set doesn't exist
                    if resp.status_code == 429:
                        last_exc = RuntimeError("429 rate limited")
                        continue
                    if 500 <= resp.status_code < 600:
                        last_exc = RuntimeError(f"{resp.status_code} server error")
                        continue
                if hasattr(resp, "raise_for_status"):
                    resp.raise_for_status()
                page_data = resp.json()
                break
            except Exception as exc:
                last_exc = exc
                continue
        if page_data is None:
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(f"fetch_set_cards failed for {set_code}")
        cards.extend(page_data.get("data") or [])
        if page_data.get("has_more"):
            url = page_data.get("next_page")
            params = None  # next_page URL has params encoded
            time.sleep(_RATE_LIMIT_DELAY_S)
        else:
            url = None
    return cards


# ============================================================
# Corpus diff.
# ============================================================


def diff_against_corpus(
    cards: List[Dict[str, Any]],
    db_path: Path,
    target_snapshot_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Diff the given cards against the target snapshot.

    Buckets:
      - new_cards: oracle_id not present at all in cards table
      - reprints: oracle_id present with same oracle_text
      - errata: oracle_id present with different oracle_text

    Returns: {new_cards: [...], reprints: [...], errata: [...]}
    """
    oracle_ids = [
        c.get("oracle_id") or "" for c in cards
        if c.get("oracle_id")
    ]
    if not oracle_ids:
        return {"new_cards": [], "reprints": [], "errata": []}

    con = sqlite3.connect(str(db_path))
    try:
        # Look up existing oracle_text by oracle_id for the target snapshot.
        existing: Dict[str, str] = {}
        for i in range(0, len(oracle_ids), 500):
            chunk = oracle_ids[i:i + 500]
            qmarks = ",".join("?" * len(chunk))
            rows = con.execute(
                f"SELECT oracle_id, oracle_text FROM cards "
                f"WHERE snapshot_id=? AND oracle_id IN ({qmarks})",
                tuple([target_snapshot_id] + chunk),
            ).fetchall()
            for oid, otext in rows:
                existing[oid] = otext or ""
    finally:
        con.close()

    new_cards: List[Dict[str, Any]] = []
    reprints: List[Dict[str, Any]] = []
    errata: List[Dict[str, Any]] = []
    for card in cards:
        oid = card.get("oracle_id") or ""
        new_text = card.get("oracle_text") or ""
        if oid not in existing:
            new_cards.append(card)
        elif existing[oid] == new_text:
            reprints.append(card)
        else:
            errata.append(card)
    return {"new_cards": new_cards, "reprints": reprints, "errata": errata}


# ============================================================
# Ingestion.
# ============================================================


def _insert_cards_rows(
    con: sqlite3.Connection,
    cards: List[Dict[str, Any]],
    target_snapshot_id: str,
) -> int:
    """Insert rows into the cards table. Returns insert count."""
    rows = []
    seen = set()  # de-dupe within this call by oracle_id
    for c in cards:
        oid = c.get("oracle_id")
        if not oid or oid in seen:
            continue
        seen.add(oid)
        rows.append((
            target_snapshot_id,
            oid,
            c.get("name") or "",
            c.get("mana_cost") or "",
            float(c.get("cmc") or 0),
            c.get("type_line") or "",
            c.get("oracle_text") or "",
            _serialize_list(c.get("colors")),
            _serialize_list(c.get("color_identity")),
            _serialize_list(c.get("produced_mana")),
            _serialize_list(c.get("keywords")),
            json.dumps(c.get("legalities") or {}),
            "[]",   # primitives_json — Phase 3 fills via extractor
            json.dumps(c.get("image_uris") or {}),
            json.dumps(c.get("card_faces") or []),
            c.get("image_status") or "",
            c.get("released_at") or "",
        ))
    if rows:
        con.executemany(
            """
            INSERT OR REPLACE INTO cards (
                snapshot_id, oracle_id, name, mana_cost, cmc, type_line,
                oracle_text, colors, color_identity, produced_mana,
                keywords, legalities_json, primitives_json,
                image_uris_json, card_faces_json, image_status,
                released_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def _insert_cards_raw_rows(
    con: sqlite3.Connection,
    cards: List[Dict[str, Any]],
    target_snapshot_id: str,
) -> int:
    """Insert rows into cards_raw. Returns insert count."""
    rows = []
    for c in cards:
        sid = c.get("id") or c.get("scryfall_id") or ""
        oid = c.get("oracle_id") or ""
        if not (sid and oid):
            continue
        rows.append((
            target_snapshot_id,
            sid,
            oid,
            c.get("lang") or "en",
            c.get("name") or "",
            json.dumps(c),
        ))
    if rows:
        con.executemany(
            "INSERT OR REPLACE INTO cards_raw "
            "(snapshot_id, scryfall_id, oracle_id, lang, name, json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
    return len(rows)


def ingest_new_set(
    set_code: str,
    db_path: Path,
    target_snapshot_id: str,
    cards: Optional[List[Dict[str, Any]]] = None,
    update_ledger: bool = True,
) -> Dict[str, Any]:
    """Fetch (or accept) `cards` for `set_code`, diff vs the target
    snapshot, write new + errata rows to cards and cards_raw inside a
    single transaction. Returns an ingestion summary dict.

    Args:
      set_code: Scryfall 3-letter code.
      db_path: Path to mtg.sqlite.
      target_snapshot_id: Snapshot to write into. The caller picks the
        snapshot (typically the active one).
      cards: Optional pre-fetched card list. When None, fetched via
        `fetch_set_cards`.
      update_ledger: When True, append `set_code` to
        `known_set_codes_v1.json` on successful ingest.

    Returns:
      {set_code, fetched, new_cards_count, reprints_count, errata_count,
       cards_inserted, cards_raw_inserted, status, elapsed_s}
    """
    t0 = time.perf_counter()
    if cards is None:
        cards = fetch_set_cards(set_code)
    if not cards:
        return {
            "set_code": set_code, "fetched": 0,
            "new_cards_count": 0, "reprints_count": 0, "errata_count": 0,
            "cards_inserted": 0, "cards_raw_inserted": 0,
            "status": "no_cards_returned",
            "elapsed_s": round(time.perf_counter() - t0, 2),
        }

    diff = diff_against_corpus(cards, db_path, target_snapshot_id)
    # Ingest the new_cards + errata (both need to land); skip pure reprints
    # since the data they'd produce is identical to what's already there.
    to_write = diff["new_cards"] + diff["errata"]

    con = sqlite3.connect(str(db_path))
    try:
        con.isolation_level = None   # manual transaction control
        con.execute("BEGIN")
        try:
            n_cards = _insert_cards_rows(con, to_write, target_snapshot_id)
            n_raw = _insert_cards_raw_rows(con, to_write, target_snapshot_id)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()

    if update_ledger:
        try:
            from api.engine.integrations.scryfall_sets_watcher_v1 import (
                load_known_set_codes, save_known_set_codes,
            )
            known = load_known_set_codes()
            known.add(set_code.strip().lower())
            save_known_set_codes(known)
        except Exception:
            pass

    return {
        "set_code": set_code,
        "fetched": len(cards),
        "new_cards_count": len(diff["new_cards"]),
        "reprints_count": len(diff["reprints"]),
        "errata_count": len(diff["errata"]),
        "cards_inserted": n_cards,
        "cards_raw_inserted": n_raw,
        "status": "ok",
        "elapsed_s": round(time.perf_counter() - t0, 2),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
