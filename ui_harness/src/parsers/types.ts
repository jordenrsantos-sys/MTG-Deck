/**
 * Common parser types — Phase 4.3 deck-import flow.
 *
 * Mirrors Engine-4A's `import_url_v1` envelope shape so URL imports +
 * client-side text parsers produce the same `ImportResult` for downstream
 * adapters (DeckInputPanel hand-off via `onImportComplete`).
 *
 * Engine-4A is authoritative for the canonical shape — see
 * `repo/api/import_url_v1.py:_envelope`.
 */

export type ImportStatus =
  | "OK"
  | "DEFERRED"
  | "INVALID_REQUEST"
  | "PARSE_ERROR"
  | "FETCH_ERROR";

export type ImportSource =
  | "archidekt"
  | "moxfield"
  | "edhrec"
  | "arena_text"
  | "mtgo_text"
  | "plain_text"
  | "file_upload"
  | "unknown";

export type ParsedCard = {
  name: string;
  count: number;
  oracle_id: string | null;
};

export type ParsedDeck = {
  commander: ParsedCard | null;
  cards: ParsedCard[];
  format: string;
};

export type ParsedUnknown = {
  name: string;
  reason: string;
};

export type ImportResult = {
  version: "import_url_v1";
  status: ImportStatus;
  source: ImportSource;
  deck: ParsedDeck | null;
  unknowns: ParsedUnknown[];
  reason_code: string | null;
  deferred_reason: string | null;
};

/**
 * Build an empty ImportResult envelope with caller-controlled status + source.
 * All parsers compose results through this helper to keep field shape stable.
 */
export function makeImportResult(args: {
  status: ImportStatus;
  source: ImportSource;
  deck?: ParsedDeck | null;
  unknowns?: ParsedUnknown[];
  reason_code?: string | null;
  deferred_reason?: string | null;
}): ImportResult {
  return {
    version: "import_url_v1",
    status: args.status,
    source: args.source,
    deck: args.deck ?? null,
    unknowns: args.unknowns ?? [],
    reason_code: args.reason_code ?? null,
    deferred_reason: args.deferred_reason ?? null,
  };
}
