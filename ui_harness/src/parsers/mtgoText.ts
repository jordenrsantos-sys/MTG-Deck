/**
 * MTGO (Magic Online) decklist parser — Phase 4.3.
 *
 * MTGO export format is a simpler `<count> <name>` per line, with a blank
 * line separating maindeck from sideboard. No set/collector tail. No
 * explicit "Commander" header — first line of maindeck is convention.
 *
 * Per autonomous_repair_log soft-safety #4 (Arena/MTGO edge cases): we
 * treat the FIRST non-empty line as commander when it appears alone on a
 * single-line block AND the input also has multi-line content (typical
 * MTGO Commander export pattern); otherwise we leave commander null and
 * let the caller pick. Rare formats are queued as 4.3.1 follow-up.
 */
import { _parsePlainText } from "./plainText";
import type { ImportResult } from "./types";

export function parseMtgoText(input: string): ImportResult {
  if (typeof input !== "string") {
    return _parsePlainText("", "mtgo_text");
  }
  // MTGO blank-line separator becomes a Sideboard section so the plainText
  // parser drops cards after the gap into unknowns with reason
  // "sideboard_dropped_for_commander".
  const lines = input.split(/\r?\n/);
  const result: string[] = [];
  let inMain = true;
  for (const line of lines) {
    if (inMain && line.trim() === "") {
      result.push("Sideboard");
      inMain = false;
      continue;
    }
    result.push(line);
  }
  return _parsePlainText(result.join("\n"), "mtgo_text");
}
