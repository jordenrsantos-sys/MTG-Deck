/**
 * GoldfishLibraryControls — Phase 4.9 Stage 2.
 * Draw / Shuffle / Mulligan / Reset buttons + library counter.
 */
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import type { PlayAction } from "../../lib/goldfishState";

export type GoldfishLibraryControlsProps = {
  libraryCount: number;
  mulligansTaken: number;
  dispatch: (action: PlayAction) => void;
};

export default function GoldfishLibraryControls({
  libraryCount,
  mulligansTaken,
  dispatch,
}: GoldfishLibraryControlsProps) {
  return (
    <Card>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Library <Badge variant="neutral">{libraryCount}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        <div className="flex flex-wrap gap-token-2" aria-label="Library controls">
          <Button
            variant="primary"
            onClick={() => dispatch({ type: "DRAW", count: 1 })}
            disabled={libraryCount === 0}
            aria-label="Draw 1 (D)"
          >
            Draw (D)
          </Button>
          <Button
            variant="ghost"
            onClick={() => dispatch({ type: "SHUFFLE_LIBRARY" })}
            disabled={libraryCount === 0}
            aria-label="Shuffle library (S)"
          >
            Shuffle (S)
          </Button>
          <Button variant="ghost" onClick={() => dispatch({ type: "MULLIGAN" })} aria-label="Mulligan (M)">
            Mulligan (M)
          </Button>
          <Button variant="ghost" onClick={() => dispatch({ type: "ADVANCE_TURN" })} aria-label="Advance turn (E)">
            Next Turn (E)
          </Button>
          <Button variant="ghost" onClick={() => dispatch({ type: "UNTAP_ALL" })} aria-label="Untap all (U)">
            Untap All (U)
          </Button>
          <Button variant="ghost" onClick={() => dispatch({ type: "RESET_GAME" })} aria-label="Reset game (R)">
            Reset (R)
          </Button>
        </div>
        {mulligansTaken > 0 ? (
          <div className="mt-token-2 text-xs text-text-muted">
            Mulligans taken: <Badge variant="warn">{mulligansTaken}</Badge> — London rules apply: keep N below 7 and place {mulligansTaken} on bottom.
          </div>
        ) : null}
      </CardBody>
    </Card>
  );
}
