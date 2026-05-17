/**
 * justificationLabels — v1.6.2 Stage 3.
 *
 * UI-side translation map from raw engine reason codes (emitted on the
 * `/deck/complete_v1.added_cards_v1[].reasons_v1` + `/deck/tune_v1.
 * recommended_swaps_v1[].reasons_v1` arrays) into human-readable prose
 * for Badge/Tooltip display.
 *
 * **Engine reason-code emission is contract** — per HARD safety #1 of
 * v1.6.2 (engine path BYTE-IDENTICAL), this translation lives UI-side
 * only. The engine continues to emit canonical code strings; this
 * helper normalizes the display.
 *
 * **Forward-compatibility:** `translateJustification(code)` returns the
 * raw `code` string when no mapping exists, so newly-emitted codes
 * surface verbatim (not crash, not blank) until a label is added.
 *
 * Source vocabulary surveyed from:
 *   - v1.2 + v1.5 engine reason codes (see deck_complete_engine_v1.py
 *     `reasons_v1` keys + `_backfill_added_cards_from_diff` placeholder).
 *   - v1.3 deckDiff fallback placeholder (`added_during_completion`).
 *   - Power Tune engine output observed by orchestrator (v1.6.1
 *     browser walk): `ADD_PRIMITIVE_COVERAGE`, `CUT_DEAD_SLOT`,
 *     `GC_COMPLIANCE_PRESERVED`.
 *   - Round-based engine additions: `ADD_REQUIRED_COVERAGE`,
 *     `ADD_REDUNDANCY_SUPPORT`, `ADD_INTERACTION_OR_PROTECTION`,
 *     `COMPLETE_TO_TARGET_SIZE`, `ADD_BASIC_LAND_FILL_AUTO`.
 *   - v1.5 backfill placeholder: `auto_completion_target_size`.
 *   - v1.2 vocabulary aliases: `basic_land_fill`, `land_target_completion`,
 *     `primitive_coverage_fill`.
 */

export const JUSTIFICATION_LABELS: Readonly<Record<string, string>> = Object.freeze({
  // ─── Power Tune (engine /deck/tune_v1.recommended_swaps_v1) ──────────
  ADD_PRIMITIVE_COVERAGE: "Fills missing primitive",
  CUT_DEAD_SLOT: "Replaces underperforming card",
  GC_COMPLIANCE_PRESERVED: "Stays within bracket limits",

  // ─── Complete (engine /deck/complete_v1.added_cards_v1) — rounds ─────
  ADD_REQUIRED_COVERAGE: "Required for sufficiency",
  ADD_REDUNDANCY_SUPPORT: "Adds redundancy support",
  ADD_INTERACTION_OR_PROTECTION: "Adds interaction or protection",
  COMPLETE_TO_TARGET_SIZE: "Brings deck up to target size",

  // ─── Complete (engine /deck/complete_v1.added_cards_v1) — lands ──────
  ADD_BASIC_LAND_FILL_AUTO: "Auto-filled basic land",

  // ─── v1.5 engine backfill placeholder (defense-in-depth) ─────────────
  auto_completion_target_size: "Added during completion",

  // ─── v1.3 UI-side diff fallback placeholder (lib/deckDiff.ts) ────────
  added_during_completion: "Added by the engine during completion",

  // ─── v1.2 spec body vocabulary aliases (may not be live-emitted) ─────
  basic_land_fill: "Basic land fill",
  land_target_completion: "Land target completion",
  primitive_coverage_fill: "Primitive coverage fill",

  // ─── v1.7 Stage 2 — combo-aware Complete-Deck reasoning ──────────────
  // Engine emits tagged-string reasons of form "COMBO_ENABLER:<json>".
  // The raw "COMBO_ENABLER" code is mapped here as a forward-compat
  // hover label; the structured payload is decoded by
  // `decodeComboEnablerPayload` and rendered as a distinct chip variant.
  COMBO_ENABLER: "Enables a 2-card combo",

  // ─── v1.7 Stage 4 — bracket-aware proactive GC recommendations ───────
  // Engine emits tagged-string reasons of form "BRACKET_AWARE_GC:<json>".
  // The raw "BRACKET_AWARE_GC" code is mapped here as a forward-compat
  // hover label; the structured payload is decoded by
  // `decodeBracketAwareGcPayload` and surfaces in Upgrade Swap Preview.
  BRACKET_AWARE_GC: "Bracket allows game-changer",

  // ─── v1.7.3 — proactive combo completion ─────────────────────────────
  // Plain-string reason added by `proactive_combo_completion_v1` to
  // added_cards_v1 rows representing partners the engine injected to
  // complete a known 2-card combo. Pairs naturally with the
  // COMBO_ENABLER chip that the existing combo_enabler annotator
  // appends on the same row.
  PROACTIVE_COMBO_TARGET: "Added to enable a known combo",
});

/**
 * Translate a single engine reason code into human-readable prose.
 *
 * Returns the raw `code` when no mapping exists — forward-compatibility
 * so new engine codes surface verbatim until labels are added.
 *
 * v1.7 Stage 4: also handles BRACKET_AWARE_GC tagged-string reasons by
 * decoding the JSON payload and returning a formatted chip label —
 * this lets the Power Tune Swap Preview's existing render seam
 * (UpgradeSuggestionsList → translateJustification → Badge) surface
 * the recommendation prose without component-level changes.
 *
 * Pure function: trivial to test (see __tests__/justificationLabels.test.ts).
 */
export function translateJustification(code: string): string {
  if (typeof code !== "string" || code === "") return code;
  if (code.startsWith(BRACKET_AWARE_GC_REASON_PREFIX)) {
    const decoded = decodeBracketAwareGcPayload(code);
    if (decoded !== null) return formatBracketAwareGcChipLabel(decoded);
  }
  const mapped = JUSTIFICATION_LABELS[code];
  return typeof mapped === "string" && mapped !== "" ? mapped : code;
}

/**
 * v1.7 Stage 2 — COMBO_ENABLER structured payload helpers.
 *
 * The combo_enabler_reasons_v1 engine layer (repo/api/engine/layers/
 * combo_enabler_reasons_v1.py) appends reasons of the tagged-string form
 * `"COMBO_ENABLER:<json>"` to `added_cards_v1[].reasons_v1`. The JSON
 * payload carries `{partner_card_oracle_id, partner_card_name,
 * combo_outcome_label}` sourced from the Stage 1.5 outcome pack
 * (commander_spellbook_combo_outcomes_v1.json).
 *
 * The tagged-string form was chosen so the reason survives api/main.py's
 * strict `reasons_v1: List[str]` filter (HARD safety BYTE-IDENTICAL).
 */
export const COMBO_ENABLER_REASON_PREFIX = "COMBO_ENABLER:";

export const COMBO_OUTCOME_LABEL_TRUNCATION_CHARS = 40;

export type ComboEnablerPayload = {
  partner_card_oracle_id: string;
  partner_card_name: string;
  combo_outcome_label: string;
};

/**
 * Decode a single `reasons_v1` entry into a COMBO_ENABLER payload.
 * Returns `null` if the input is not a COMBO_ENABLER reason or if the
 * payload is malformed.
 *
 * Pure function: trivial to test.
 */
export function decodeComboEnablerPayload(reason: unknown): ComboEnablerPayload | null {
  if (typeof reason !== "string") return null;
  if (!reason.startsWith(COMBO_ENABLER_REASON_PREFIX)) return null;
  const jsonText = reason.slice(COMBO_ENABLER_REASON_PREFIX.length);
  if (jsonText === "") return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonText);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== "object") return null;
  const obj = parsed as Record<string, unknown>;
  const oracleId = obj["partner_card_oracle_id"];
  const partnerName = obj["partner_card_name"];
  const outcomeLabel = obj["combo_outcome_label"];
  if (
    typeof oracleId !== "string" || oracleId === "" ||
    typeof partnerName !== "string" || partnerName === "" ||
    typeof outcomeLabel !== "string" || outcomeLabel === ""
  ) return null;
  return {
    partner_card_oracle_id: oracleId,
    partner_card_name: partnerName,
    combo_outcome_label: outcomeLabel,
  };
}

/**
 * Build the chip display text for a COMBO_ENABLER payload:
 * `"Enables {partner} → {outcome}"`, with `outcome` truncated to
 * `COMBO_OUTCOME_LABEL_TRUNCATION_CHARS` (with ellipsis) when longer.
 *
 * Hover-title rendering should use the full payload — only the chip
 * face is truncated.
 */
export function formatComboEnablerChipLabel(payload: ComboEnablerPayload): string {
  const label = payload.combo_outcome_label;
  const truncated =
    label.length > COMBO_OUTCOME_LABEL_TRUNCATION_CHARS
      ? label.slice(0, COMBO_OUTCOME_LABEL_TRUNCATION_CHARS - 1).trimEnd() + "…"
      : label;
  return `Enables ${payload.partner_card_name} → ${truncated}`;
}

/**
 * v1.7 Stage 4 — BRACKET_AWARE_GC structured payload helpers.
 *
 * The bracket_aware_recommendations_v1 engine layer
 * (repo/api/engine/layers/bracket_aware_recommendations_v1.py)
 * appends reasons of the tagged-string form
 *   `"BRACKET_AWARE_GC:<json>"`
 * to `recommended_swaps_v1[].reasons_v1`. The JSON payload carries
 *   `{recommended_gc_oracle_id, recommended_gc_name,
 *     current_deck_gc_count, bracket_max_gc}`
 * sourced from `api/engine/data/brackets/gc_limits_v1.json` +
 * `data/game_changers/gc_v0_userlist_2025-11-20.json` (both files
 * read-only HARD safeties).
 */
export const BRACKET_AWARE_GC_REASON_PREFIX = "BRACKET_AWARE_GC:";

export type BracketAwareGcPayload = {
  recommended_gc_oracle_id: string;
  recommended_gc_name: string;
  current_deck_gc_count: number;
  bracket_max_gc: number | null;
};

/**
 * Decode a single `reasons_v1` entry into a BRACKET_AWARE_GC payload.
 * Returns `null` if the input is not a BRACKET_AWARE_GC reason or if
 * the payload is malformed. Pure function.
 */
export function decodeBracketAwareGcPayload(
  reason: unknown,
): BracketAwareGcPayload | null {
  if (typeof reason !== "string") return null;
  if (!reason.startsWith(BRACKET_AWARE_GC_REASON_PREFIX)) return null;
  const jsonText = reason.slice(BRACKET_AWARE_GC_REASON_PREFIX.length);
  if (jsonText === "") return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonText);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== "object") return null;
  const obj = parsed as Record<string, unknown>;
  const oracleId = obj["recommended_gc_oracle_id"];
  const name = obj["recommended_gc_name"];
  const currentCount = obj["current_deck_gc_count"];
  const maxRaw = obj["bracket_max_gc"];
  if (
    typeof oracleId !== "string" || oracleId === "" ||
    typeof name !== "string" || name === "" ||
    typeof currentCount !== "number"
  ) return null;
  const bracketMax =
    typeof maxRaw === "number" ? maxRaw : maxRaw === null ? null : null;
  return {
    recommended_gc_oracle_id: oracleId,
    recommended_gc_name: name,
    current_deck_gc_count: currentCount,
    bracket_max_gc: bracketMax,
  };
}

/**
 * Build the chip display text for a BRACKET_AWARE_GC payload:
 *   `"Bracket allows game-changer — recommends {name}"`
 * — the bracket cap (numeric or "unlimited") is reserved for the
 * hover title to keep the chip concise.
 */
export function formatBracketAwareGcChipLabel(
  payload: BracketAwareGcPayload,
): string {
  return `Bracket allows game-changer — recommends ${payload.recommended_gc_name}`;
}
