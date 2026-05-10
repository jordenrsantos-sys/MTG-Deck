/**
 * MTGA (Arena) decklist parser — Phase 4.3.
 *
 * Arena export format:
 *
 *   Commander
 *   1 Atraxa, Praetors' Voice (CMR) 270
 *
 *   Deck
 *   1 Sol Ring (CMR) 251
 *   ...
 *
 *   Sideboard
 *   1 Some Card (XYZ) 99
 *
 * The set/collector tail is stripped during parsing; oracle_id resolution
 * is server-side per soft-safety #6 (autonomous_repair_log: missing
 * /cards/names endpoint → unresolved names surface as unknowns[] mirrors
 * Engine-4A pattern).
 */
import { _parsePlainText } from "./plainText";
import type { ImportResult } from "./types";

// Strip Arena set/collector tails like " (CMR) 270".
const ARENA_TAIL_RE = /\s*\([A-Za-z0-9]+\)\s+\d+\s*$/;

export function parseArenaText(input: string): ImportResult {
  if (typeof input !== "string") {
    return _parsePlainText("", "arena_text");
  }
  const stripped = input
    .split(/\r?\n/)
    .map((line) => line.replace(ARENA_TAIL_RE, ""))
    .join("\n");
  return _parsePlainText(stripped, "arena_text");
}
