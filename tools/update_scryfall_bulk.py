import argparse
import json
import sqlite3
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests
from tqdm import tqdm


def extract_primitives(card_dict: dict) -> list[str]:
    type_line_l = (card_dict.get("type_line") or "").lower()
    text_l = (card_dict.get("oracle_text") or "").lower()
    keywords = card_dict.get("keywords") or []

    if isinstance(keywords, str):
        keywords_l = [keywords.lower()]
    else:
        try:
            keywords_l = [str(k).lower() for k in keywords]
        except TypeError:
            keywords_l = []

    tags = set()

    # Mana / economy
    if "add {" in text_l:
        tags.add("RAMP_MANA")

    produced_mana = card_dict.get("produced_mana") or []
    if produced_mana:
        tags.add("RAMP_MANA")
        tags.add("MANA_FIXING")

    if "search your library for" in text_l and "land" in text_l:
        if "onto the battlefield" in text_l:
            tags.add("RAMP_LAND")
        elif "into your hand" in text_l:
            tags.add("TUTOR")

    if "creature" in type_line_l and "add {" in text_l:
        tags.add("MANA_DORK")

    if "artifact" in type_line_l and "{t}:" in text_l and "add" in text_l:
        tags.add("MANA_ROCK")

    if "treasure token" in text_l or ("create" in text_l and "treasure" in text_l):
        tags.add("TREASURE_PRODUCTION")

    if "cost" in text_l and ("less to cast" in text_l or "costs {1} less" in text_l):
        tags.add("COST_REDUCTION")

    # Cards / selection
    if "draw a card" in text_l or "draw two cards" in text_l or "draw x cards" in text_l:
        tags.add("CARD_DRAW")

    if "look at the top" in text_l or "scry" in text_l or "surveil" in text_l or ("discard" in text_l and "draw" in text_l):
        tags.add("CARD_SELECTION")

    if "search your library for" in text_l and ("put it into your hand" in text_l or "reveal it" in text_l):
        tags.add("TUTOR")

    # Interaction
    if "counter target" in text_l:
        tags.add("COUNTERSPELL")

    if "destroy target" in text_l or "exile target" in text_l:
        tags.add("REMOVAL_SINGLE")

    if (
        "destroy target artifact" in text_l
        or "destroy target enchantment" in text_l
        or "exile target artifact" in text_l
        or "exile target enchantment" in text_l
    ):
        tags.add("REMOVAL_ARTIFACT_ENCHANTMENT")

    if ("destroy all" in text_l or "exile all" in text_l) and (
        "creatures" in text_l or "artifacts" in text_l or "enchantments" in text_l
    ):
        tags.add("BOARD_WIPE")

    if "copy target" in text_l or "change the target" in text_l or "new targets" in text_l:
        tags.add("STACK_INTERACTION")

    # Protection / resilience
    if "hexproof" in text_l or "indestructible" in text_l or "protection from" in text_l or "phase out" in text_l:
        tags.add("PROTECTION")

    if "return target" in text_l and "from your graveyard" in text_l:
        tags.add("RECURSION")

    if "exile all cards from target player's graveyard" in text_l or "exile target graveyard" in text_l:
        tags.add("GRAVEYARD_HATE")

    # Board development
    if "create" in text_l and "token" in text_l:
        tags.add("TOKEN_PRODUCTION")

    if "sacrifice a" in text_l or "sacrifice another" in text_l:
        tags.add("SAC_OUTLET")

    if (
        "whenever" in text_l
        and "dies" in text_l
        and ("each opponent loses" in text_l or "lose 1 life" in text_l)
    ) or ("blood artist" in text_l):
        tags.add("ARISTOCRAT_PAYOFF")

    if "when" in text_l and "enters the battlefield" in text_l:
        tags.add("ETB_VALUE")

    if "double" in text_l and ("tokens" in text_l or "counters" in text_l or "triggered ability" in text_l):
        tags.add("TRIGGER_DOUBLER")

    # Counters / stats
    if "+1/+1 counter" in text_l:
        tags.add("COUNTER_SYNERGY")

    if "proliferate" in text_l:
        tags.add("PROLIFERATE")
        tags.add("COUNTER_SYNERGY")

    if "creatures you control get +" in text_l:
        tags.add("PUMP_TEAM")

    # Stax / denial
    if "players can't" in text_l or "can't cast" in text_l or "can't draw" in text_l or "can't search" in text_l:
        tags.add("STAX_RULES")

    if "spells cost" in text_l and "more to cast" in text_l:
        tags.add("TAX_EFFECT")

    if "doesn't untap" in text_l or ("tap" in text_l and "doesn't untap" in text_l):
        tags.add("TAP_DOWN")

    if "destroy target land" in text_l or "sacrifice a land" in text_l or "each opponent discards" in text_l:
        tags.add("RESOURCE_DENIAL")

    # Life / damage
    if "you gain" in text_l and "life" in text_l:
        tags.add("LIFEGAIN")

    if "each opponent loses" in text_l and "life" in text_l:
        tags.add("DRAIN")

    if "deals" in text_l and "damage" in text_l:
        tags.add("DIRECT_DAMAGE")

    # Combat
    if any(
        k in keywords_l
        for k in [
            "flying",
            "menace",
            "trample",
            "first strike",
            "double strike",
            "deathtouch",
            "vigilance",
            "haste",
        ]
    ):
        tags.add("EVASION")

    if "until end of turn" in text_l and ("gets +" in text_l or "gains" in text_l):
        tags.add("COMBAT_TRICK")

    # CARD_ADVANTAGE is in vocabulary but not explicitly covered by v0 ruleset.
    return sorted(tags)


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

        legalities_json = json.dumps(card.get("legalities") or {})
        primitives_json = json.dumps(extract_primitives(card))

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
            legalities_json,
            primitives_json,
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
         colors, color_identity, produced_mana, keywords, legalities_json, primitives_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
