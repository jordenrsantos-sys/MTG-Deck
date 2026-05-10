/**
 * Plain-text decklist parser — Phase 4.3.
 *
 * Accepts loose decklist input where each non-empty, non-comment line is one
 * card entry of the shape `[<count>x?] <card name>` or just `<card name>`.
 * Comments: lines starting with `#`, `//`, or `SB:` (sideboard prefix).
 * Sections: lines starting with `Commander`, `Sideboard`, `Maybeboard` are
 * treated as section headers; cards under "Commander" become commander.
 *
 * No oracle_id resolution at the client tier — `oracle_id` is left null;
 * the server will resolve via existing card-name lookup OR surface in
 * `unknowns[]` per Engine-4A pattern (autonomous_repair_log soft-safety #6).
 */
import { makeImportResult, type ImportResult, type ImportSource, type ParsedCard, type ParsedUnknown } from "./types";

const COMMENT_PREFIXES = ["#", "//"];
const SIDEBOARD_PREFIXES = ["SB:", "SIDEBOARD"];
const COMMANDER_HEADER_RE = /^\s*(?:commander|cmdr)s?\s*[:\-]?\s*$/i;
const SIDEBOARD_HEADER_RE = /^\s*(?:sideboard|maybeboard)\s*[:\-]?\s*$/i;
const DECK_HEADER_RE = /^\s*(?:deck|main(?:board)?)\s*[:\-]?\s*$/i;

const LINE_RE = /^\s*(\d+)?\s*[xX]?\s*(.+?)\s*$/;

type ParseSection = "deck" | "commander" | "sideboard";

export function parsePlainText(input: string): ImportResult {
  return _parsePlainText(input, "plain_text");
}

// Internal callable separated from the public one so other parsers can
// re-tag the source without rewriting the body.
export function _parsePlainText(input: string, source: ImportSource): ImportResult {
  if (typeof input !== "string" || input.trim() === "") {
    return makeImportResult({
      status: "INVALID_REQUEST",
      source,
      reason_code: "empty_input",
    });
  }

  const lines = input.split(/\r?\n/);
  const cards: ParsedCard[] = [];
  let commander: ParsedCard | null = null;
  const unknowns: ParsedUnknown[] = [];

  let section: ParseSection = "deck";

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (line === "") continue;
    if (COMMENT_PREFIXES.some((p) => line.startsWith(p))) continue;
    if (SIDEBOARD_PREFIXES.some((p) => line.toUpperCase().startsWith(p))) {
      section = "sideboard";
      continue;
    }
    if (COMMANDER_HEADER_RE.test(line)) {
      section = "commander";
      continue;
    }
    if (SIDEBOARD_HEADER_RE.test(line)) {
      section = "sideboard";
      continue;
    }
    if (DECK_HEADER_RE.test(line)) {
      section = "deck";
      continue;
    }

    const m = LINE_RE.exec(line);
    if (!m) {
      unknowns.push({ name: line, reason: "line_does_not_match_count_name_pattern" });
      continue;
    }
    const countRaw = m[1];
    const namePart = (m[2] || "").trim();
    if (namePart === "") {
      unknowns.push({ name: line, reason: "blank_name" });
      continue;
    }
    const count = countRaw ? parseInt(countRaw, 10) : 1;
    if (!Number.isFinite(count) || count < 1) {
      unknowns.push({ name: namePart, reason: "invalid_count" });
      continue;
    }
    const card: ParsedCard = { name: namePart, count, oracle_id: null };

    if (section === "commander") {
      if (commander === null) commander = card;
      else cards.push(card);
    } else if (section === "sideboard") {
      // Sideboard cards are dropped from the runtime decklist (commander
      // format is singleton main + commander); flagged in unknowns for
      // visibility per Engine-4A unknowns-surfacing convention.
      unknowns.push({ name: namePart, reason: "sideboard_dropped_for_commander" });
    } else {
      cards.push(card);
    }
  }

  if (commander === null && cards.length === 0 && unknowns.length === 0) {
    return makeImportResult({
      status: "PARSE_ERROR",
      source,
      reason_code: "no_cards_parsed",
    });
  }

  return makeImportResult({
    status: "OK",
    source,
    deck: { commander, cards, format: "commander" },
    unknowns,
  });
}
