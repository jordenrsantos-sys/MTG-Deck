/**
 * applySwap — Phase 4 BUNDLE Stage 2 (sub-task 4.8).
 *
 * Pure client-side helpers that apply a swap_suggestions[] entry against
 * a deck list (string OR string[] form). Used by SwapSuggestionsList's
 * one-click "Apply Swap" affordance to update the workspace decklist
 * without re-running /build.
 *
 * Per Stage 0 capture: swap entries from `seed_to_deck_v1.swap_suggestions`
 * have flexible shape; the canonical fields are `out` (card to remove) +
 * `in` (card to add). Optional fields (rationale_code, swap_reason, etc.)
 * may surface but are presentation-only.
 *
 * Per HARD safety: NO HTTP, NO engine calls, NO mtg.sqlite reads.
 */

export type SwapSuggestion = {
  out?: string | null;
  in?: string | null;
  rationale_code?: string | null;
  why?: string | null;
  // Allow other free-form fields without losing them.
  [key: string]: unknown;
};

export type ApplySwapResult = {
  applied: boolean;
  reason: string;
  before: string;
  after: string;
  swap: SwapSuggestion;
};

/**
 * Apply a single swap to a multi-line decklist text. Lines are tolerantly
 * parsed: "1 Card Name" / "1x Card Name" / "Card Name" all match the card
 * name (case-insensitive). The first occurrence of `out` is replaced with
 * `in`, preserving the leading count + formatting.
 */
export function applySwapToDecklistText(
  decklistText: string,
  swap: SwapSuggestion,
): ApplySwapResult {
  const out = (swap.out ?? "").trim();
  const incoming = (swap.in ?? "").trim();
  if (out === "" || incoming === "") {
    return {
      applied: false,
      reason: "swap_missing_in_or_out",
      before: decklistText,
      after: decklistText,
      swap,
    };
  }
  if (typeof decklistText !== "string") {
    return {
      applied: false,
      reason: "decklist_not_string",
      before: "",
      after: "",
      swap,
    };
  }

  const lines = decklistText.split(/\r?\n/);
  const targetLower = out.toLowerCase();
  let matchedIndex = -1;
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const m = /^\s*(\d+)?\s*[xX]?\s*(.+?)\s*$/.exec(line);
    if (!m) continue;
    const namePart = (m[2] ?? "").trim().toLowerCase();
    if (namePart === targetLower) {
      matchedIndex = i;
      break;
    }
  }
  if (matchedIndex === -1) {
    return {
      applied: false,
      reason: "out_card_not_in_decklist",
      before: decklistText,
      after: decklistText,
      swap,
    };
  }

  const original = lines[matchedIndex];
  const m = /^(\s*)(\d+\s*[xX]?\s*)?(.+?)(\s*)$/.exec(original);
  const leading = m?.[1] ?? "";
  const countToken = m?.[2] ?? "";
  const trailing = m?.[4] ?? "";
  lines[matchedIndex] = `${leading}${countToken}${incoming}${trailing}`;

  return {
    applied: true,
    reason: "ok",
    before: decklistText,
    after: lines.join("\n"),
    swap,
  };
}

/** Apply multiple swaps in order; stops at the first non-applied swap. */
export function applySwapsInOrder(
  decklistText: string,
  swaps: ReadonlyArray<SwapSuggestion>,
): ApplySwapResult[] {
  const results: ApplySwapResult[] = [];
  let current = decklistText;
  for (const swap of swaps) {
    const r = applySwapToDecklistText(current, swap);
    results.push(r);
    if (!r.applied) break;
    current = r.after;
  }
  return results;
}
