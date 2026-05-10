/**
 * SwapSuggestionsList — Phase 4 BUNDLE Stage 2 (sub-task 4.8).
 *
 * Reads `seed_to_deck_v1.swap_suggestions` (Stage 0 captured shape) and
 * renders one row per suggestion with "Apply Swap" Button. Apply triggers
 * the parent's `onApplySwap` callback — caller (WorkspaceView) is
 * responsible for wiring `applySwapToDecklistText` into the decklist text
 * setter (existing 4.6 adapter pattern).
 *
 * Per autonomous_repair_log soft-safety #2 (absent/empty swap_suggestions):
 * empty state surfaced cleanly; no crash.
 */
import Button from "../../ui/primitives/Button";
import Badge from "../../ui/primitives/Badge";
import Tooltip from "../../ui/primitives/Tooltip";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import type { SwapSuggestion } from "../../lib/applySwap";

export type SwapSuggestionsListProps = {
  swaps: ReadonlyArray<SwapSuggestion> | null | undefined;
  onApplySwap?: (swap: SwapSuggestion, index: number) => void;
  className?: string;
};

export default function SwapSuggestionsList(props: SwapSuggestionsListProps) {
  const { swaps, onApplySwap, className } = props;
  const list = Array.isArray(swaps) ? swaps : [];
  return (
    <Card className={className}>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Swap Suggestions
          <Badge variant="neutral">{list.length}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        {list.length === 0 ? (
          <p className="text-sm text-text-muted">
            No swap suggestions yet — sufficiency PASS or no tune iterations
            triggered.
          </p>
        ) : (
          <ul className="flex flex-col gap-token-2" role="list">
            {list.map((swap, idx) => (
              <li
                key={`${swap.out}->${swap.in}-${idx}`}
                className="flex items-center justify-between gap-token-3 px-token-2 py-token-1 rounded-token-sm border border-glass-border bg-glass-bg"
              >
                <div className="flex flex-col text-sm gap-token-1">
                  <span>
                    <span className="text-red-300 line-through">
                      {swap.out ?? "—"}
                    </span>{" "}
                    →{" "}
                    <span className="text-green-300">{swap.in ?? "—"}</span>
                  </span>
                  {swap.rationale_code || swap.why ? (
                    <Tooltip
                      content={swap.why ?? swap.rationale_code ?? ""}
                      placement="bottom"
                    >
                      <span className="text-xs text-text-muted underline-offset-2 hover:underline cursor-help">
                        Why?
                      </span>
                    </Tooltip>
                  ) : null}
                </div>
                <Button
                  size="sm"
                  variant="secondary"
                  onClick={() => onApplySwap?.(swap, idx)}
                  aria-label={`Apply swap: ${swap.out} → ${swap.in}`}
                >
                  Apply
                </Button>
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}
