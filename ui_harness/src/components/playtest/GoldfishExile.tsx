/**
 * GoldfishExile — Phase 4.9 Stage 2. Read-only by default; "Hand" button
 * returns the card.
 */
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import type { GoldfishCard, PlayAction } from "../../lib/goldfishState";

export type GoldfishExileProps = {
  exile: ReadonlyArray<GoldfishCard>;
  dispatch: (action: PlayAction) => void;
};

export default function GoldfishExile({ exile, dispatch }: GoldfishExileProps) {
  return (
    <Card>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Exile <Badge variant="neutral">{exile.length}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        {exile.length === 0 ? (
          <div className="text-sm text-text-muted">Exile is empty.</div>
        ) : (
          <ul className="flex flex-col gap-token-1" aria-label="Exile">
            {exile.map((card) => (
              <li
                key={card.instanceId}
                className="flex items-center justify-between gap-token-2 px-token-2 py-token-1 rounded-token-md bg-glass-bg border border-glass-border"
              >
                <span className="text-sm text-text-primary truncate">{card.name}</span>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => dispatch({ type: "RETURN_TO_HAND", from: "exile", instanceId: card.instanceId })}
                  aria-label={`Return ${card.name} to hand`}
                >
                  Hand
                </Button>
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}
