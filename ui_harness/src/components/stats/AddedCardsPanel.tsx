/**
 * AddedCardsPanel — v1.1 Stage 1.
 *
 * Renders the `added_cards_v1` rows returned by `/deck/complete_v1`.
 * Surfaces above DeckEditorPanel after a successful Complete so users
 * immediately see what was added + why (was previously buried under
 * "Complete to 100" + "Apply Complete" toolbar — the Phase 4.14 unified
 * "1. Complete deck" button auto-commits the additions; this panel
 * tells the user what just happened).
 *
 * Per spec: panel doesn't render when empty. Caller (WorkspaceView)
 * conditions on `rows.length > 0`.
 *
 * Per HARD #11: prop API is local to v1.1 (NEW component); doesn't
 * widen any Phase 4.x contracts. AddedCardEntry shape mirrors
 * DeckCompleteAddedCardV1 (defined in WorkspaceView).
 */
import { useMemo, useState } from "react";
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import Tooltip from "../../ui/primitives/Tooltip";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import AddedCardRow, { type AddedCardEntry } from "./AddedCardRow";
// v1.6.3 Stage 2: pending-review chips reuse the v1.6.2 translation map
// BYTE-IDENTICAL — engine reason codes render as human-readable prose
// (raw code preserved on Badge title hover for power users).
import { translateJustification } from "../../lib/justificationLabels";
import type { ProposedAdd } from "../../lib/workspaceDeckState";
// v1.7 Stage 1: partial-completion mode uses the same multiset diff
// helper as v1.3's WorkspaceView-level fallback. When the engine sets
// `complete_status: "ERROR"` (e.g., baseline_unknowns non-empty) it returns
// `added_cards_v1: []` even though `completed_decklist_text_v1` carries
// the completed deck — the diff surfaces the additions client-side at the
// panel boundary so direct callers see partial-completion render.
import { computeDeckAdditions } from "../../lib/deckDiff";

/** v1.3 Stage 1: source signal so the "Why?" tooltip can explain
 *  whether the rows came from engine domain codes or the diff fallback's
 *  placeholder reason. Optional — defaults to "engine" (back-compat). */
export type AddedCardsSource = "engine" | "diff" | "none";

export type AddedCardsPanelProps = {
  rows: ReadonlyArray<AddedCardEntry>;
  /** Optional caller-supplied click handler for the "× Dismiss" affordance.
   *  When omitted, dismissal is purely local (component hides itself). */
  onDismiss?: () => void;
  /** Optional caller-supplied hover handler — forwards to AddedCardRow. */
  onHoverCardName?: (name: string) => void;
  /** v1.3 Stage 1: which source produced these rows. Drives the header
   *  "Why?" tooltip copy. Defaults to "engine" when omitted (v1.1 callers
   *  back-compat per HARD #11). */
  source?: AddedCardsSource;
  /** v1.6.3 Stage 2 — pending-review queue. When non-empty, the panel
   *  renders a per-card accept/reject UI INSTEAD of the v1.1 applied
   *  rows view. Empty (default) preserves v1.1/v1.3 behavior verbatim. */
  pendingAdds?: ReadonlyArray<ProposedAdd>;
  /** v1.6.3 Stage 2 — per-row checkbox toggle. Required when pendingAdds
   *  is non-empty. */
  onTogglePendingAdd?: (index: number) => void;
  /** v1.6.3 Stage 2 — bulk control: commit accepted subset to deckText
   *  (dispatches APPLY_ACCEPTED_ADDS in WorkspaceView). Required when
   *  pendingAdds is non-empty. */
  onApplyAccepted?: () => void;
  /** v1.6.3 Stage 2 — bulk control: flip all rows to accepted. */
  onAcceptAll?: () => void;
  /** v1.6.3 Stage 2 — bulk control: flip all rows to rejected. */
  onRejectAll?: () => void;
  /** v1.6.3 Stage 2 — bulk control: clear pending queue without
   *  deck mutation (dispatches DISMISS_PROPOSED_ADDS). */
  onDismissPending?: () => void;
  /** v1.7 Stage 1 — pre-completion deck text (the "before" side of the
   *  multiset diff). When provided alongside `completedDecklistText`, the
   *  panel computes the diff client-side and renders partial-completion
   *  mode if pendingAdds is empty + rows is empty + diff is non-zero. */
  deckText?: string;
  /** v1.7 Stage 1 — engine's `completed_decklist_text_v1` (the "after"
   *  side of the multiset diff). Drives partial-completion-mode resolution. */
  completedDecklistText?: string;
  /** v1.7 Stage 1 — commit-all callback for partial-completion mode.
   *  Caller wires this to APPLY_ACCEPTED_ADDS-equivalent dispatch. When
   *  omitted the Apply All button renders disabled. */
  onApplyAllPartial?: () => void;
  className?: string;
};

const _SOURCE_TOOLTIP_COPY: Record<AddedCardsSource, string> = {
  engine:
    "Engine source: each row's reasons_v1 codes come from the deck-completion engine — domain codes like ADD_BASIC_LAND_FILL_AUTO or COMPLETE_TO_TARGET_SIZE.",
  diff: "Diff source: the engine returned an empty added_cards_v1 array, so this list is a text-diff of the completed deck vs. what you sent in. The placeholder reason 'added_during_completion' means: added by the engine during deck-completion to reach the target deck size.",
  none: "No additions detected.",
};

const _PARTIAL_COMPLETION_FALLBACK_CHIP =
  "Engine completed deck — reason details unavailable";

export default function AddedCardsPanel(props: AddedCardsPanelProps) {
  const {
    rows,
    onDismiss,
    onHoverCardName,
    source = "engine",
    pendingAdds,
    onTogglePendingAdd,
    onApplyAccepted,
    onAcceptAll,
    onRejectAll,
    onDismissPending,
    deckText,
    completedDecklistText,
    onApplyAllPartial,
    className,
  } = props;
  const [dismissed, setDismissed] = useState(false);

  // v1.6.3 Stage 2 — pending-review mode takes precedence when the
  // pendingAdds queue is non-empty. Pending mode ignores the local
  // dismissed flag (the queue is parent-owned state).
  const hasPending = Array.isArray(pendingAdds) && pendingAdds.length > 0;
  const hasAppliedRows = Array.isArray(rows) && rows.length > 0;

  // v1.7 Stage 1 — partial-completion mode resolution. Only computed when
  // both prior modes are inactive; the multiset diff is pure + cheap but
  // memoized so the SSR path doesn't recompute per render.
  const partialAdditions = useMemo(() => {
    if (hasPending || hasAppliedRows) return [];
    if (typeof completedDecklistText !== "string" || completedDecklistText === "") return [];
    if (typeof deckText !== "string") return [];
    return computeDeckAdditions(deckText, completedDecklistText);
  }, [hasPending, hasAppliedRows, deckText, completedDecklistText]);
  const hasPartial = partialAdditions.length > 0;

  if (!hasPending && !hasAppliedRows && !hasPartial) return null;
  if (!hasPending && !hasPartial && dismissed) return null;

  function handleDismissClick() {
    setDismissed(true);
    if (onDismiss) onDismiss();
  }

  if (hasPending) {
    const queue = pendingAdds as ReadonlyArray<ProposedAdd>;
    const acceptedCount = queue.filter((row) => row.accepted === true).length;
    return (
      <div data-v163-stage="pending-review-panel">
      <Card className={className}>
        <CardHeader>
          <span className="flex items-center justify-between gap-token-3 w-full">
            <span className="flex items-center gap-token-2">
              Review proposed additions
              <Badge variant="info">{queue.length}</Badge>
              <Badge variant="success" aria-label={`${acceptedCount} of ${queue.length} accepted`}>
                {acceptedCount}/{queue.length} accepted
              </Badge>
            </span>
            <span className="flex items-center gap-token-2">
              <Button
                size="sm"
                variant="primary"
                onClick={() => onApplyAccepted?.()}
                disabled={acceptedCount === 0 || !onApplyAccepted}
                aria-label={`Apply Selected (${acceptedCount})`}
              >
                Apply Selected ({acceptedCount})
              </Button>
              <Button
                size="sm"
                variant="secondary"
                onClick={() => onAcceptAll?.()}
                disabled={!onAcceptAll || acceptedCount === queue.length}
                aria-label="Accept All"
              >
                Accept All
              </Button>
              <Button
                size="sm"
                variant="secondary"
                onClick={() => onRejectAll?.()}
                disabled={!onRejectAll || acceptedCount === 0}
                aria-label="Reject All"
              >
                Reject All
              </Button>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => onDismissPending?.()}
                disabled={!onDismissPending}
                aria-label="Dismiss proposed additions"
              >
                × Dismiss
              </Button>
            </span>
          </span>
        </CardHeader>
        <CardBody>
          <p className="text-xs text-text-muted mb-token-2">
            The engine proposed these cards to bring the deck up to 99 cards.
            Uncheck a row to reject it; click <strong>Apply Selected</strong> to
            commit only the accepted cards.
          </p>
          <ul
            className="flex flex-col gap-token-1"
            role="list"
            aria-label="Proposed additions"
          >
            {queue.map((row, i) => (
              <li
                key={`pending-${i}-${row.card_name}`}
                className="flex flex-wrap items-center gap-token-2 py-token-1 border-b border-glass-border"
              >
                <label
                  className="inline-flex items-center gap-token-2 cursor-pointer"
                  aria-label={`Toggle ${row.card_name} for application`}
                >
                  <input
                    type="checkbox"
                    checked={row.accepted}
                    onChange={() => onTogglePendingAdd?.(i)}
                    aria-label={`Accept ${row.card_name}`}
                  />
                  <span
                    className={
                      row.accepted
                        ? "font-semibold text-text-primary text-sm"
                        : "font-semibold text-text-muted text-sm line-through"
                    }
                  >
                    {row.card_name}
                  </span>
                </label>
                {Array.isArray(row.reasons) && row.reasons.length > 0 ? (
                  <span
                    className="flex flex-wrap items-center gap-1"
                    aria-label="Reasons"
                  >
                    {row.reasons.map((r, ri) => (
                      // Wrapping span carries the raw engine code via title
                      // attribute (Badge primitive doesn't forward title).
                      <span key={`r-${ri}-${r}`} title={r} className="inline-flex">
                        <Badge variant="neutral">{translateJustification(r)}</Badge>
                      </span>
                    ))}
                  </span>
                ) : null}
              </li>
            ))}
          </ul>
        </CardBody>
      </Card>
      </div>
    );
  }

  // v1.7 Stage 1 — partial-completion mode renders the client-side multiset
  // diff when the engine returned empty added_cards_v1 + empty pendingAdds
  // but a populated completed_decklist_text_v1. Each diff card carries the
  // fallback chip text "Engine completed deck — reason details unavailable"
  // since the engine didn't emit per-card reason codes for this path. The
  // single Apply All button dispatches the caller-supplied commit handler.
  if (hasPartial) {
    return (
      <div data-v17-stage="partial-completion-panel">
        <Card className={className}>
          <CardHeader>
            <span className="flex items-center justify-between gap-token-3 w-full">
              <span className="flex items-center gap-token-2">
                Partial completion
                <Badge variant="info">{partialAdditions.length}</Badge>
                <Tooltip
                  content="The engine completed the deck but didn't emit per-card reason codes for this path (e.g., baseline status reported errors or unknowns). The cards listed below were derived from a client-side diff of your input deck vs. the engine's completed_decklist_text_v1."
                  placement="bottom"
                >
                  <Badge variant="warn" aria-label="Source: partial-completion">
                    Why? (partial)
                  </Badge>
                </Tooltip>
              </span>
              <span className="flex items-center gap-token-2">
                <Button
                  size="sm"
                  variant="primary"
                  onClick={() => onApplyAllPartial?.()}
                  disabled={!onApplyAllPartial}
                  aria-label="Apply All partial-completion additions"
                >
                  Apply All ({partialAdditions.length})
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={handleDismissClick}
                  aria-label="Dismiss partial-completion panel"
                >
                  × Dismiss
                </Button>
              </span>
            </span>
          </CardHeader>
          <CardBody>
            <p className="text-xs text-text-muted mb-token-2">
              The engine completed the deck but didn't emit per-card reason
              codes. These additions were derived from a text-diff between
              your input deck and the engine's completed deck.
            </p>
            <ul
              className="flex flex-col gap-0"
              role="list"
              aria-label="Partial-completion additions"
            >
              {partialAdditions.map((entry, i) => (
                <li
                  key={`partial-${i}-${entry.name}`}
                  className="flex flex-wrap items-baseline gap-token-2 py-token-1 border-b border-glass-border"
                  onMouseEnter={() => {
                    if (onHoverCardName && entry.name !== "") onHoverCardName(entry.name);
                  }}
                >
                  <span className="font-semibold text-text-primary text-sm">
                    {entry.name}
                  </span>
                  <span className="flex flex-wrap items-center gap-1" aria-label="Reasons">
                    {/* Badge primitive doesn't forward `title`; wrap with a
                        span so the raw fallback reason code surfaces on hover. */}
                    <span
                      title={entry.reasons_v1[0] ?? "added_during_completion"}
                      className="inline-flex"
                    >
                      <Badge variant="warn">{_PARTIAL_COMPLETION_FALLBACK_CHIP}</Badge>
                    </span>
                  </span>
                </li>
              ))}
            </ul>
          </CardBody>
        </Card>
      </div>
    );
  }

  return (
    <Card className={className}>
      <CardHeader>
        <span className="flex items-center justify-between gap-token-3 w-full">
          <span className="flex items-center gap-token-2">
            Added cards
            <Badge variant="success">{rows.length}</Badge>
            {/* v1.3 Stage 1: source tooltip — explains diff fallback copy. */}
            <Tooltip content={_SOURCE_TOOLTIP_COPY[source]} placement="bottom">
              <Badge variant={source === "diff" ? "warn" : "neutral"} aria-label={`Source: ${source}`}>
                Why? ({source})
              </Badge>
            </Tooltip>
          </span>
          <Button
            size="sm"
            variant="ghost"
            onClick={handleDismissClick}
            aria-label="Dismiss added-cards panel"
          >
            × Dismiss
          </Button>
        </span>
      </CardHeader>
      <CardBody>
        <p className="text-xs text-text-muted mb-token-2">
          The engine added these cards to bring the deck to 99 cards. Each row shows the reasons + new primitives.
        </p>
        <ul className="flex flex-col gap-0" role="list" aria-label="Added cards list">
          {rows.map((entry, i) => (
            <AddedCardRow
              key={`${entry.name ?? "row"}-${i}`}
              entry={entry}
              onHover={onHoverCardName}
            />
          ))}
        </ul>
      </CardBody>
    </Card>
  );
}
