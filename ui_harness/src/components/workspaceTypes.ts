export type JsonRecord = Record<string, unknown>;

export type CardSuggestRow = {
  oracle_id: string;
  name: string;
  mana_cost: string | null;
  type_line: string | null;
  image_uri: string | null;
};

export type ParsedDecklistRow = {
  name: string;
  count: number;
  source_order: number;
  original_line: string;
};

export type BuildRequestPayload = {
  db_snapshot_id: string;
  profile_id: string;
  bracket_id: string;
  format: "commander";
  commander: string;
  cards: string[];
  engine_patches_v0: unknown[];
};

export type BuildResponsePayload = JsonRecord & {
  result?: JsonRecord;
  unknowns?: unknown[];
};

export type BuildHistoryEntry = {
  id: string;
  timestamp_iso: string;
  timestamp_label: string;
  deck_name: string;
  commander_input: string;
  db_snapshot_id: string;
  profile_id: string;
  bracket_id: string;
  status: string;
  request_payload: BuildRequestPayload;
  response_body: BuildResponsePayload;
};

export type HoverCard = {
  name: string;
  oracle_id: string;
  type_line: string | null;
  primitive_tags: string[];
  source: "suggest" | "primitive" | "unknown" | "deck";
};

export type PrimitiveExplorerCardRow = {
  slot_id: string;
  name: string;
  oracle_id: string;
  type_line: string | null;
  primitive_tags: string[];
};

export type PrimitiveExplorerGroup = {
  primitive_id: string;
  count: number;
  slot_ids: string[];
  cards: PrimitiveExplorerCardRow[];
};
