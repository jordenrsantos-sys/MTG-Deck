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

CREATE TABLE IF NOT EXISTS run_history_v0 (
  run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  engine_version TEXT,
  db_snapshot_id TEXT,
  profile_id TEXT,
  bracket_id TEXT,
  endpoint TEXT NOT NULL,
  input_hash_v1 TEXT NOT NULL,
  output_build_hash_v1 TEXT NOT NULL,
  output_proof_attempts_hash_v2 TEXT,
  layer_hashes_json TEXT NOT NULL,
  request_json TEXT NOT NULL,
  response_json TEXT NOT NULL,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_history_created_at
ON run_history_v0(created_at);

CREATE INDEX IF NOT EXISTS idx_run_history_output_build_hash
ON run_history_v0(output_build_hash_v1);

CREATE INDEX IF NOT EXISTS idx_run_history_input_hash
ON run_history_v0(input_hash_v1);

CREATE TABLE IF NOT EXISTS primitive_defs_v0 (
  primitive_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  category TEXT NOT NULL,
  is_engine_primitive INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS primitive_rules_v0 (
  rule_id TEXT PRIMARY KEY,
  primitive_id TEXT NOT NULL,
  rule_type TEXT NOT NULL,
  pattern TEXT NOT NULL,
  weight REAL NOT NULL DEFAULT 1.0,
  priority INTEGER NOT NULL DEFAULT 100,
  notes TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  ruleset_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS card_primitive_tags_v0 (
  oracle_id TEXT NOT NULL,
  card_name TEXT NOT NULL,
  primitive_id TEXT NOT NULL,
  ruleset_version TEXT NOT NULL,
  confidence REAL NOT NULL,
  evidence_json TEXT NOT NULL,
  PRIMARY KEY (oracle_id, primitive_id, ruleset_version)
);

CREATE INDEX IF NOT EXISTS idx_cpt_v0_primitive
ON card_primitive_tags_v0(primitive_id, ruleset_version);

CREATE INDEX IF NOT EXISTS idx_cpt_v0_oracle
ON card_primitive_tags_v0(oracle_id, ruleset_version);

CREATE TABLE IF NOT EXISTS primitive_tag_runs_v0 (
  run_id TEXT PRIMARY KEY,
  db_snapshot_id TEXT NOT NULL,
  ruleset_version TEXT NOT NULL,
  cards_processed INTEGER NOT NULL,
  tags_emitted INTEGER NOT NULL,
  unknowns_emitted INTEGER NOT NULL,
  run_hash_v1 TEXT NOT NULL,
  created_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_primitive_tag_runs_v0_snapshot
ON primitive_tag_runs_v0(db_snapshot_id, created_at);

CREATE TABLE IF NOT EXISTS primitive_tag_unknowns_v0 (
  oracle_id TEXT NOT NULL,
  card_name TEXT NOT NULL,
  reason TEXT NOT NULL,
  details_json TEXT NOT NULL,
  ruleset_version TEXT NOT NULL,
  PRIMARY KEY (oracle_id, reason, ruleset_version)
);

CREATE INDEX IF NOT EXISTS idx_primitive_tag_unknowns_v0_ruleset
ON primitive_tag_unknowns_v0(ruleset_version, reason);
