/**
 * BracketViolationsBanner — red warning surface when the deck violates bracket rules.
 *
 * v1.7.5 — closes the user-visible gap on the bracket-combo enforcement bug.
 * Engine layer (api/engine/layers/complete_bracket_violations_v1.py) emits
 * `violations_v1` entries with codes TWO_CARD_COMBOS_DISALLOWED_B1 /
 * TWO_CARD_COMBOS_DISALLOWED_B2 when the user submits a deck containing a known
 * 2-card combo at bracket B1 or B2. The response.status also downgrades from "OK"
 * to "BRACKET_VIOLATION" so the UI can surface a clear "this deck isn't bracket-legal"
 * warning without polishing the deck panel.
 *
 * Props:
 *   - violations: list of {code, card_name, message} dicts from /deck/complete_v1's
 *     violations_v1 field
 *   - status: top-level response.status (used to determine the visual treatment —
 *     "BRACKET_VIOLATION" renders the red full warning; other statuses render null
 *     even if isolated bracket-prefixed violations slip through).
 *
 * Self-hides (returns null) when no bracket-combo violations are present in the
 * violations list, regardless of status.
 */
import Badge from "../../ui/primitives/Badge";
import { Card, CardBody } from "../../ui/primitives/Card";

export type BracketViolationRow = {
  code: string;
  card_name?: string;
  count?: number;
  message: string;
};

export type BracketViolationsBannerProps = {
  violations?: ReadonlyArray<BracketViolationRow>;
  status?: string;
  // v1.7.6: the user's currently-selected bracket. Violations carry the
  // bracket they were emitted FOR in their code suffix (e.g.
  // TWO_CARD_COMBOS_DISALLOWED_B2). When the user raises the bracket via
  // the new selector, those stale violations no longer apply — filter them
  // out client-side so the banner self-hides immediately instead of
  // waiting for the next /deck/complete_v1 round-trip.
  currentBracketId?: string;
};

const _BRACKET_COMBO_CODE_PREFIX = "TWO_CARD_COMBOS_DISALLOWED";

function _isBracketComboViolation(row: BracketViolationRow): boolean {
  if (!row || typeof row.code !== "string") {
    return false;
  }
  return row.code.startsWith(_BRACKET_COMBO_CODE_PREFIX);
}

export default function BracketViolationsBanner(
  props: BracketViolationsBannerProps,
) {
  const violations = Array.isArray(props.violations) ? props.violations : [];
  const currentBracketId =
    typeof props.currentBracketId === "string"
      ? props.currentBracketId.trim()
      : "";
  const bracketCombo = violations
    .filter(_isBracketComboViolation)
    .filter((row) => {
      if (currentBracketId === "") return true;
      return row.code.endsWith(`_${currentBracketId}`);
    });

  if (bracketCombo.length === 0) {
    return null;
  }

  // Detect the bracket id from the code suffix (e.g., "..._B2" → "B2").
  const bracketIds = Array.from(
    new Set(
      bracketCombo
        .map((row) => {
          const tail = row.code.split("_").pop() || "";
          return /^B[0-9]$/.test(tail) ? tail : "";
        })
        .filter((s) => s !== ""),
    ),
  );
  const bracketLabel = bracketIds.length === 1 ? bracketIds[0] : "this bracket";

  return (
    <Card
      data-v175-stage="bracket-violations-banner"
      data-status={props.status || ""}
      className="border-red-500/40 bg-red-500/10 text-text-primary"
    >
      <CardBody>
        <div className="flex items-start gap-token-3">
          <div className="flex-1">
            <div className="flex items-center gap-token-2 flex-wrap">
              <Badge variant="error" className="font-bold uppercase">
                Bracket Violation
              </Badge>
              <span className="text-sm text-text-muted">
                This deck is not legal at bracket {bracketLabel}.
              </span>
            </div>
            <p className="mt-token-2 text-sm text-text-secondary">
              Bracket {bracketLabel} disallows 2-card combos. Either remove one
              half of each combo from your deck, or raise the bracket to B3+ to
              allow combo strategies.
            </p>
            <ul className="mt-token-3 space-y-token-2">
              {bracketCombo.map((row, idx) => (
                <li
                  key={`${row.code}-${idx}`}
                  className="rounded-token-sm bg-bg-elev-2 px-token-3 py-token-2"
                >
                  <p className="text-sm text-text-primary">{row.message}</p>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </CardBody>
    </Card>
  );
}
