/**
 * deckDiff — v1.3 Stage 1.
 *
 * Pure decklist-text diff for UI-side fallback when the engine's
 * `added_cards_v1` array is empty (which happens on certain baseline
 * status paths). Computes additions only; cuts/removals are out of
 * scope (surfaced via UpgradeSuggestionsList's swap-out display).
 *
 * Parse rules — kept deliberately permissive + mirror Phase 4.3
 * `parseDecklistInput` conventions:
 *   - Strip comments (`//`, `#`, `;`) and blank lines.
 *   - Strip `Commander`/`Deck`/`Sideboard`/`Maybeboard` banner lines.
 *   - Strip set/collector annotations like `(M21) 132`, `*F*`, `*E*`,
 *     `[modern]`. Anything inside () or [] following the card name.
 *   - Lines parse as `[count] name` — count default 1.
 *   - Duplicate-name lines merge counts.
 *
 * Output entries match WorkspaceView's `DeckCompleteAddedCardV1` shape
 * locally — `{name, reasons_v1, primitives_added_v1}`. The diff source
 * uses the placeholder reason `"added_during_completion"`; the UI
 * tooltip explains the placeholder semantics.
 *
 * Deterministic alphabetical ordering for stable rendering (test
 * harness asserts this — see `__tests__/deckDiff.test.ts`).
 */

export type DeckDiffAddedEntry = {
  name: string;
  reasons_v1: ReadonlyArray<string>;
  primitives_added_v1: ReadonlyArray<string>;
};

// v1.5 Stage 1: extended section-header vocabulary. The engine's
// `_build_completed_decklist_text` emits "Commander" + commander name(s) +
// "Deck" + deck cards; user-imported decks may have additional banners
// from Archidekt/Moxfield exports (Mainboard / Sideboard / Maybeboard /
// Companion / Tokens). All matched case-insensitively, with trailing
// colon + whitespace tolerated ("Commander", "commander", "Commander:",
// "Commander :" all strip).
const _BANNER_LINES = new Set([
  "commander",
  "deck",
  "mainboard",
  "sideboard",
  "maybeboard",
  "companion",
  "tokens",
]);
const _DEFAULT_REASON = "added_during_completion";

/** v1.5 Stage 1: normalize a candidate banner line for the _BANNER_LINES
 *  lookup — lowercase, strip trailing colon, strip outer whitespace. */
function _normalizeBannerCandidate(raw: string): string {
  const lower = raw.trim().toLowerCase();
  // Strip trailing colon + any whitespace before it: "commander :" → "commander".
  return lower.replace(/\s*:\s*$/g, "").trim();
}

/** Strip set/collector annotations + foil markers from a card name segment. */
function _stripAnnotations(s: string): string {
  // Remove (...) and [...] groups + *F*/*E* foil markers.
  let out = s.replace(/\([^)]*\)/g, " ").replace(/\[[^\]]*\]/g, " ");
  out = out.replace(/\*[A-Za-z]+\*/g, " ");
  // Collapse whitespace + strip trailing collector numbers like "  132".
  out = out.replace(/\s+\d+\s*$/g, " ");
  return out.replace(/\s+/g, " ").trim();
}

/** Parse a single decklist line into [count, name] OR null when skip-able. */
function _parseLine(rawLine: string): [number, string] | null {
  const trimmed = rawLine.trim();
  if (trimmed === "") return null;
  // Comment markers.
  if (trimmed.startsWith("//") || trimmed.startsWith("#") || trimmed.startsWith(";")) {
    return null;
  }
  // Banner lines — v1.5 normalizes trailing colon + whitespace before lookup.
  if (_BANNER_LINES.has(_normalizeBannerCandidate(trimmed))) return null;
  // Reject lines that don't start with a letter or digit (malformed/negative).
  const first = trimmed.charCodeAt(0);
  const isLetter =
    (first >= 0x41 && first <= 0x5a) || (first >= 0x61 && first <= 0x7a);
  const isDigit = first >= 0x30 && first <= 0x39;
  if (!isLetter && !isDigit) return null;

  // Match optional leading "Nx" or "N " or just name.
  const m = trimmed.match(/^(\d+)\s*[xX]?\s+(.*)$/);
  let count = 1;
  let rest = trimmed;
  if (m) {
    count = parseInt(m[1], 10);
    rest = m[2];
  }
  const name = _stripAnnotations(rest);
  if (name === "" || _BANNER_LINES.has(_normalizeBannerCandidate(name))) return null;
  if (!Number.isFinite(count) || count <= 0) return null;
  return [count, name];
}

/** Build a {name → totalCount} map from a decklist text. */
export function _decklistToCountMap(text: string): Map<string, number> {
  const counts = new Map<string, number>();
  if (typeof text !== "string" || text === "") return counts;
  for (const line of text.split(/\r?\n/)) {
    const parsed = _parseLine(line);
    if (!parsed) continue;
    const [count, name] = parsed;
    counts.set(name, (counts.get(name) ?? 0) + count);
  }
  return counts;
}

/**
 * Pure diff: returns entries for cards present in `afterText` with
 * higher total count than in `beforeText`. Each entry's count is
 * NOT surfaced in the entry shape (AddedCardEntry has no count
 * field) — instead, duplicate-add entries are emitted N times
 * (one entry per added copy) so the panel's list-length matches
 * the deck growth, mirroring the engine's added_cards_v1 1:1
 * length-equals-growth invariant from v1.2 Stage 1.
 *
 * Cards REMOVED (afterCount < beforeCount) are NOT emitted — out
 * of scope for AddedCardsPanel (additions-only by design).
 *
 * Sort: alphabetical by name (English locale-insensitive
 * comparison), then within same name the entries are identical
 * so order is stable by definition.
 */
export function computeDeckAdditions(
  beforeText: string,
  afterText: string,
): ReadonlyArray<DeckDiffAddedEntry> {
  const before = _decklistToCountMap(beforeText);
  const after = _decklistToCountMap(afterText);

  const additions: DeckDiffAddedEntry[] = [];
  // Iterate `after` keys for additions; cards only in `before` are removals (skipped).
  for (const [name, afterCount] of after) {
    const beforeCount = before.get(name) ?? 0;
    const diff = afterCount - beforeCount;
    if (diff <= 0) continue;
    for (let i = 0; i < diff; i += 1) {
      additions.push({
        name,
        reasons_v1: [_DEFAULT_REASON],
        primitives_added_v1: [],
      });
    }
  }
  // Stable alphabetical ordering on name.
  additions.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  return additions;
}
