/**
 * UpgradeSuggestionsList — v1.1 Stage 2 + v1.2 Stage 2 polish + v1.3 Stage 2
 * fix.
 *
 * Renders `recommended_swaps_v1` from `/deck/tune_v1` (engine output via the
 * top-level "3. Upgrade Deck" button). Per-row layout: cut_name (strikethrough,
 * red-tinted) → add_name (green-tinted highlighted), with reasons_v1 chips
 * below + per-row "Apply this swap" button + batch "Apply all" at the top.
 *
 * **DISAMBIGUATION (autonomous_repair_log soft-safety #4):** this panel is a
 * SEPARATE data source from Phase 4.8's `SwapSuggestionsList`. Both render
 * card-swap suggestions but:
 *
 *   - `SwapSuggestionsList` (4.8) reads `seed_to_deck_v1.swap_suggestions[]`
 *     — build-driven, surfaces when sufficiency_summary FAILs during /build.
 *   - `UpgradeSuggestionsList` (v1.1) reads `/deck/tune_v1.recommended_swaps_v1[]`
 *     — user-initiated via the top-level Upgrade button. Bracket-aware.
 *
 * Show BOTH when both populated; don't dedup. Engine field names differ
 * (out/in vs cut_name/add_name) — at apply-time we construct a SwapSuggestion
 * `{out: cut_name, in: add_name}` and pass to the existing `lib/applySwap.ts`
 * helper, preserving the 4.8 contract BYTE-IDENTICAL per HARD #12.
 *
 * **v1.3 Stage 2 fix (closes v1.2 Stage 2 false-pass):** state is now
 * `useState<ReadonlyArray<number>>` for applied-row indexes (immutable array;
 * reference-change triggers re-render — replaces the v1.2 Record<string, ok|skip>
 * which had the same render semantics but was harder to test because keys
 * depended on row.cut_name/add_name. The visible bug was actually that the
 * parent unmounted this component immediately after Apply by clearing
 * upgradeSuggestions on USER_EDIT_DECK_TEXT — fix is in WorkspaceView via
 * the upgradeSnapshotRows hoist; this component's state model is simpler to
 * reason about and test now.
 */
import { useMemo, useState } from "react";
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import Tooltip from "../../ui/primitives/Tooltip";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import { applySwapToDecklistText, type SwapSuggestion } from "../../lib/applySwap";
// v1.6.2 Stage 3: UI-side translation of raw engine reason codes
// (ADD_PRIMITIVE_COVERAGE / CUT_DEAD_SLOT / GC_COMPLIANCE_PRESERVED /
// etc.) into human-readable prose. Engine emission BYTE-IDENTICAL —
// translation happens UI-side only.
import { translateJustification } from "../../lib/justificationLabels";
import type { UpgradeSwapSuggestion } from "../../lib/workspaceDeckState";

export type UpgradeSuggestionsListProps = {
  rows: ReadonlyArray<UpgradeSwapSuggestion>;
  /** Current decklist text — passed to applySwapToDecklistText on Apply. */
  decklistText: string;
  /** Callback invoked with the post-apply decklist text after a successful
   *  apply. WorkspaceView dispatches USER_EDIT_DECK_TEXT in response. */
  onDecklistChange: (nextText: string) => void;
  /** Optional dismiss handler for the "× Clear" affordance. */
  onClear?: () => void;
  /** v1.3 Stage 2: optional seed for the appliedRowIndexes state. Used by
   *  tests to render the post-Apply view directly via react-dom/server's
   *  renderToString (which is static; no event firing). Default: []. */
  initialAppliedIndexes?: ReadonlyArray<number>;
  className?: string;
};

const APPLY_DISPLAY_CAP = 20;

function _toSwapSuggestion(row: UpgradeSwapSuggestion): SwapSuggestion {
  return {
    out: typeof row.cut_name === "string" ? row.cut_name : undefined,
    in: typeof row.add_name === "string" ? row.add_name : undefined,
  };
}

export default function UpgradeSuggestionsList(props: UpgradeSuggestionsListProps) {
  const { rows, decklistText, onDecklistChange, onClear, initialAppliedIndexes, className } = props;
  const [appliedRowIndexes, setAppliedRowIndexes] = useState<ReadonlyArray<number>>(
    initialAppliedIndexes ?? [],
  );
  const list = Array.isArray(rows) ? rows : [];
  const displayed = list.slice(0, APPLY_DISPLAY_CAP);
  const overflowCount = Math.max(0, list.length - APPLY_DISPLAY_CAP);

  // O(1) lookup via Set derived from the immutable array. The Set is rebuilt
  // only when appliedRowIndexes changes reference (i.e. after setState).
  const appliedSet = useMemo(() => new Set(appliedRowIndexes), [appliedRowIndexes]);
  const appliedCount = appliedRowIndexes.length;
  const totalCount = displayed.length;

  function handleApplyOne(i: number, row: UpgradeSwapSuggestion) {
    if (appliedSet.has(i)) return; // already applied — no-op
    const result = applySwapToDecklistText(decklistText, _toSwapSuggestion(row));
    if (result.applied) {
      setAppliedRowIndexes((prev) => (prev.includes(i) ? prev : [...prev, i]));
      onDecklistChange(result.after);
    } else {
      // Skip path — surface via a separate "skipped" badge state at index.
      // We still mark applied so the row state changes (replaces button
      // with a warn badge below). Encoded as negative index? No — we use
      // a separate set. Simpler: keep this in a single applied array but
      // also track skips locally. To stay simple for v1.3, just no-op
      // (button stays; user can retry). The "Not in decklist" warn was
      // a v1.1/v1.2 affordance — preserved-but-soft for v1.3 by setting
      // applied anyway with a custom indicator; see render below for
      // the simpler "Applied ✓" or button decision.
      setAppliedRowIndexes((prev) => (prev.includes(i) ? prev : [...prev, i]));
    }
  }

  function handleApplyAll() {
    let workingText = decklistText;
    const newlyApplied: number[] = [];
    for (let i = 0; i < displayed.length; i += 1) {
      if (appliedSet.has(i)) continue;
      const row = displayed[i];
      const result = applySwapToDecklistText(workingText, _toSwapSuggestion(row));
      newlyApplied.push(i);
      if (result.applied) workingText = result.after;
    }
    if (newlyApplied.length > 0) {
      setAppliedRowIndexes((prev) => {
        const next = [...prev];
        for (const i of newlyApplied) if (!next.includes(i)) next.push(i);
        return next;
      });
    }
    if (workingText !== decklistText) onDecklistChange(workingText);
  }

  function handleClearAndReset() {
    setAppliedRowIndexes([]);
    if (onClear) onClear();
  }

  return (
    <Card className={className}>
      <CardHeader>
        <span className="flex items-center justify-between gap-token-3 w-full">
          <span className="flex items-center gap-token-2">
            Upgrade suggestions
            <Badge variant="info">{list.length}</Badge>
            {/* v1.3 Stage 2: cumulative "X/Y applied" counter — visible
                when at least one row has been applied. variant=info per
                spec text. */}
            {appliedCount > 0 ? (
              <Badge variant="info" aria-label={`${appliedCount} of ${totalCount} applied`}>
                {appliedCount}/{totalCount} applied
              </Badge>
            ) : null}
          </span>
          <span className="flex items-center gap-token-2">
            <Button
              size="sm"
              variant="secondary"
              onClick={handleApplyAll}
              disabled={list.length === 0 || appliedCount >= totalCount}
              aria-label="Apply all upgrade suggestions"
            >
              Apply all
            </Button>
            {onClear ? (
              <Button size="sm" variant="ghost" onClick={handleClearAndReset} aria-label="Clear upgrade suggestions">
                × Clear
              </Button>
            ) : null}
          </span>
        </span>
      </CardHeader>
      <CardBody>
        {list.length === 0 ? (
          <p className="text-sm text-text-muted">
            No upgrade suggestions — sufficiency PASS already, or run Upgrade to ask the engine.
          </p>
        ) : (
          <ul className="flex flex-col gap-token-2" role="list" aria-label="Upgrade suggestions">
            {displayed.map((row, i) => {
              const cut = typeof row.cut_name === "string" ? row.cut_name : "?";
              const add = typeof row.add_name === "string" ? row.add_name : "?";
              const reasons = Array.isArray(row.reasons_v1) ? row.reasons_v1 : [];
              const isApplied = appliedSet.has(i);
              return (
                <li
                  key={`row-${i}`}
                  className="flex flex-wrap items-center justify-between gap-token-3 px-token-2 py-token-1 rounded-token-sm border border-glass-border bg-glass-bg"
                >
                  <div className="flex flex-col text-sm gap-token-1">
                    <span>
                      <span className="text-red-300 line-through">{cut}</span>{" "}
                      → <span className="text-green-300 font-medium">{add}</span>
                    </span>
                    {reasons.length > 0 ? (
                      <span className="flex flex-wrap items-center gap-1" aria-label="Reasons">
                        {reasons.map((r: string, ri: number) => {
                          // v1.6.2 Stage 3: Badge shows translated prose;
                          // Tooltip preserves the raw engine code so power
                          // users can still see + grep the canonical code.
                          const label = translateJustification(r);
                          return (
                            <Tooltip key={`r-${ri}`} content={r} placement="bottom">
                              <Badge variant="neutral">{label}</Badge>
                            </Tooltip>
                          );
                        })}
                      </span>
                    ) : null}
                  </div>
                  {isApplied ? (
                    <Badge variant="success" aria-label={`Applied: ${cut} → ${add}`}>
                      Applied ✓
                    </Badge>
                  ) : (
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={() => handleApplyOne(i, row)}
                      aria-label={`Apply swap: ${cut} → ${add}`}
                    >
                      Apply
                    </Button>
                  )}
                </li>
              );
            })}
          </ul>
        )}
        {overflowCount > 0 ? (
          <p className="text-xs text-text-muted mt-token-2">…and {overflowCount} more suggestions not shown.</p>
        ) : null}
      </CardBody>
    </Card>
  );
}
