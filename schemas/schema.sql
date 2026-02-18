PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  source TEXT NOT NULL,
  scryfall_bulk_uri TEXT NOT NULL,
  scryfall_bulk_updated_at TEXT,
  manifest_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cards_raw (
  snapshot_id TEXT NOT NULL,
  scryfall_id TEXT NOT NULL,
  oracle_id TEXT,
  lang TEXT,
  name TEXT,
  json TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, scryfall_id)
);

CREATE INDEX IF NOT EXISTS idx_cards_raw_oracle
ON cards_raw(snapshot_id, oracle_id);

CREATE TABLE IF NOT EXISTS cards (
  snapshot_id TEXT NOT NULL,
  oracle_id TEXT NOT NULL,
  name TEXT NOT NULL,
  mana_cost TEXT,
  cmc REAL,
  type_line TEXT,
  oracle_text TEXT,
  colors TEXT,
  color_identity TEXT,
  produced_mana TEXT,
  keywords TEXT,
  legalities_json TEXT,
  primitives_json TEXT,
  PRIMARY KEY (snapshot_id, oracle_id)
);

CREATE INDEX IF NOT EXISTS idx_cards_name
ON cards(snapshot_id, name);
