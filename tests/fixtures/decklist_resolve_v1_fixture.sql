CREATE TABLE snapshots (
  snapshot_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  source TEXT NOT NULL,
  scryfall_bulk_uri TEXT NOT NULL,
  scryfall_bulk_updated_at TEXT,
  manifest_json TEXT NOT NULL
);

CREATE TABLE cards (
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

CREATE TABLE card_aliases (
  snapshot_id TEXT NOT NULL,
  alias_name TEXT NOT NULL,
  oracle_id TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, alias_name, oracle_id)
);

CREATE TABLE card_tags (
  oracle_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  taxonomy_version TEXT NOT NULL,
  ruleset_version TEXT NOT NULL,
  primitive_ids_json TEXT NOT NULL,
  equiv_class_ids_json TEXT NOT NULL,
  facets_json TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (oracle_id, snapshot_id, taxonomy_version)
);

INSERT INTO snapshots (
  snapshot_id,
  created_at,
  source,
  scryfall_bulk_uri,
  scryfall_bulk_updated_at,
  manifest_json
) VALUES (
  'DECKLIST_TEST_SNAPSHOT',
  '2026-02-20T00:00:00+00:00',
  'decklist_fixture',
  'local://decklist/fixture',
  '2026-02-20T00:00:00+00:00',
  '{"tags_compiled":1}'
);

INSERT INTO cards (
  snapshot_id,
  oracle_id,
  name,
  mana_cost,
  cmc,
  type_line,
  oracle_text,
  colors,
  color_identity,
  produced_mana,
  keywords,
  legalities_json,
  primitives_json
) VALUES
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_CMDR_001',
  'Krenko, Mob Boss',
  '{2}{R}{R}',
  4,
  'Legendary Creature — Goblin Warrior',
  NULL,
  '["R"]',
  '["R"]',
  '["R"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_CMDR_002',
  'Esior, Wardwing Familiar',
  '{1}{U}',
  2,
  'Legendary Creature — Bird',
  NULL,
  '["U"]',
  '["U"]',
  '["U"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_CMDR_003',
  'Ishai, Ojutai Dragonspeaker',
  '{2}{W}{U}',
  4,
  'Legendary Creature — Bird Monk',
  NULL,
  '["W","U"]',
  '["W","U"]',
  '["W","U"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_RING_001',
  'Sol Ring',
  '{1}',
  1,
  'Artifact',
  NULL,
  '[]',
  '[]',
  '["C"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_SIGNET_001',
  'Arcane Signet',
  '{2}',
  2,
  'Artifact',
  NULL,
  '[]',
  '[]',
  '["R"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_AMB_001',
  'Twin Name',
  '{1}',
  1,
  'Artifact',
  NULL,
  '[]',
  '[]',
  '[]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_AMB_002',
  'twin name',
  '{1}',
  1,
  'Artifact',
  NULL,
  '[]',
  '[]',
  '[]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_BASIC_001',
  'Plains',
  '',
  0,
  'Basic Land — Plains',
  NULL,
  '[]',
  '[]',
  '["W"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_SNOW_BASIC_001',
  'Snow-Covered Plains',
  '',
  0,
  'Basic Snow Land — Plains',
  NULL,
  '[]',
  '[]',
  '["W"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_DFC_001',
  'Bala Ged Recovery // Bala Ged Sanctuary',
  '{2}{G}',
  3,
  'Sorcery // Land',
  NULL,
  '["G"]',
  '["G"]',
  '["G"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_DFC_AMB_001',
  'Mirror Front // Mirror Back',
  '{1}{U}',
  2,
  'Instant // Land',
  NULL,
  '["U"]',
  '["U"]',
  '["U"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
),
(
  'DECKLIST_TEST_SNAPSHOT',
  'ORA_DFC_AMB_002',
  'Mirror Front // Mirror Lake',
  '{1}{U}',
  2,
  'Instant // Land',
  NULL,
  '["U"]',
  '["U"]',
  '["U"]',
  '[]',
  '{"commander":"legal"}',
  '[]'
);

INSERT INTO card_aliases (snapshot_id, alias_name, oracle_id)
VALUES ('DECKLIST_TEST_SNAPSHOT', 'Signet of Arcana', 'ORA_SIGNET_001');

INSERT INTO card_tags (
  oracle_id,
  snapshot_id,
  taxonomy_version,
  ruleset_version,
  primitive_ids_json,
  equiv_class_ids_json,
  facets_json,
  evidence_json,
  created_at
) VALUES (
  'ORA_CMDR_001',
  'DECKLIST_TEST_SNAPSHOT',
  'taxonomy_v_fixture',
  'ruleset_v_fixture',
  '[]',
  '[]',
  '{"commander_legal":true}',
  '{}',
  '2026-02-20T00:00:00+00:00'
);
