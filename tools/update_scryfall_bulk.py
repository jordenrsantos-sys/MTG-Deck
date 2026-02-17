import argparse
import json
import sqlite3
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests
from tqdm import tqdm


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_schema(schema_path: Path) -> str:
    return schema_path.read_text(encoding="utf-8")


def connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    return con


def apply_schema(con: sqlite3.Connection, schema_sql: str):
    con.executescript(schema_sql)
    con.commit()


def fetch_bulk_index() -> Dict[str, Any]:
    r = requests.get("https://api.scryfall.com/bulk-data", timeout=60)
    r.raise_for_status()
    return r.json()


def pick_oracle_cards(bulk_index: Dict[str, Any]) -> Dict[str, Any]:
    for item in bulk_index.get("data", []):
        if item.get("type") == "oracle_cards":
            return item
    raise RuntimeError("oracle_cards bulk data not found")


def download_file(url: str, out_path: Path):
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", "0")) or None
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc="Downloading") as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                pbar.update(len(chunk))


def ingest_cards(con: sqlite3.Connection, snapshot_id: str, json_path: Path):
    data = json.loads(json_path.read_text(encoding="utf-8"))

    cur = con.cursor()
    raw_rows = []
    norm_rows = []

    for card in tqdm(data, desc="Preparing rows"):
        scry_id = card.get("id")
        oracle_id = card.get("oracle_id")
        lang = card.get("lang")
        name = card.get("name")

        raw_rows.append((
            snapshot_id,
            scry_id,
            oracle_id,
            lang,
            name,
            json.dumps(card, ensure_ascii=False),
        ))

        norm_rows.append((
            snapshot_id,
            oracle_id,
            name,
            card.get("mana_cost"),
            card.get("cmc"),
            card.get("type_line"),
            card.get("oracle_text"),
            json.dumps(card.get("colors", [])),
            json.dumps(card.get("color_identity", [])),
            json.dumps(card.get("produced_mana", [])),
            json.dumps(card.get("keywords", [])),
            json.dumps(card.get("legalities", {})),
        ))

    cur.execute("BEGIN;")
    cur.executemany("""
        INSERT OR REPLACE INTO cards_raw
        (snapshot_id, scryfall_id, oracle_id, lang, name, json)
        VALUES (?, ?, ?, ?, ?, ?)
    """, raw_rows)

    cur.executemany("""
        INSERT OR REPLACE INTO cards
        (snapshot_id, oracle_id, name, mana_cost, cmc, type_line, oracle_text,
         colors, color_identity, produced_mana, keywords, legalities_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, norm_rows)

    con.commit()


def insert_snapshot(con: sqlite3.Connection, snapshot_id: str, oracle_meta: Dict[str, Any], manifest: Dict[str, Any]):
    con.execute("""
        INSERT INTO snapshots (snapshot_id, created_at, source, scryfall_bulk_uri, scryfall_bulk_updated_at, manifest_json)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        snapshot_id,
        utc_now_iso(),
        "scryfall_bulk_oracle_cards",
        oracle_meta.get("download_uri"),
        oracle_meta.get("updated_at"),
        json.dumps(manifest, ensure_ascii=False),
    ))
    con.commit()


def main():
    ap = argparse.ArgumentParser(description="Update Mode: ingest Scryfall oracle_cards bulk data into SQLite snapshot.")
    ap.add_argument("--db", required=True, help="Path to SQLite DB (e.g., E:\\mtg-engine\\data\\mtg.sqlite)")
    ap.add_argument("--schema", required=True, help="Path to schema.sql (e.g., schemas\\schema.sql)")
    ap.add_argument("--out", required=True, help="Where to download bulk JSON (e.g., E:\\mtg-engine\\snapshots)")
    ap.add_argument("--snapshot-id", default=None, help="Optional snapshot id; default uses UTC timestamp.")
    args = ap.parse_args()

    db_path = Path(args.db)
    schema_path = Path(args.schema)
    out_dir = Path(args.out)

    snapshot_id = args.snapshot_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    bulk_index = fetch_bulk_index()
    oracle_meta = pick_oracle_cards(bulk_index)

    json_path = out_dir / f"scryfall_oracle_cards_{snapshot_id}.json"

    print(f"[1/5] Downloading oracle_cards bulk to: {json_path}")
    download_file(oracle_meta["download_uri"], json_path)

    print("[2/5] Hashing download...")
    file_hash = sha256_file(json_path)

    manifest = {
        "snapshot_id": snapshot_id,
        "created_at": utc_now_iso(),
        "scryfall_bulk_type": oracle_meta.get("type"),
        "scryfall_bulk_updated_at": oracle_meta.get("updated_at"),
        "scryfall_download_uri": oracle_meta.get("download_uri"),
        "download_sha256": file_hash,
        "tool": "update_scryfall_bulk.py",
        "tool_version": "0.1.0",
    }

    print(f"[3/5] Opening DB: {db_path}")
    con = connect(db_path)
    try:
        print("[4/5] Applying schema...")
        apply_schema(con, load_schema(schema_path))

        print(f"[5/5] Ingesting cards for snapshot_id={snapshot_id} ...")
        insert_snapshot(con, snapshot_id, oracle_meta, manifest)
        ingest_cards(con, snapshot_id, json_path)
    finally:
        con.close()

    print("\nDONE.")
    print(f"snapshot_id: {snapshot_id}")
    print(f"db: {db_path}")
    print(f"file_sha256: {file_hash}")


if __name__ == "__main__":
    main()
