/**
 * GoldfishGraveyard — Phase 4.9 Stage 2.
 * Click "Hand" returns to hand; "Exi" moves to exile.
 */
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import type { GoldfishCard, PlayAction } from "../../lib/goldfishState";

export type GoldfishGraveyardProps = {
  graveyard: ReadonlyArray<GoldfishCard>;
  dispatch: (action: PlayAction) => void;
};

export default function GoldfishGraveyard({ graveyard, dispatch }: GoldfishGraveyardProps) {
  return (
    <Card>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Graveyard <Badge variant="neutral">{graveyard.length}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        {graveyard.length === 0 ? (
          <div className="text-sm text-text-muted">Graveyard is empty.</div>
        ) : (
          <ul className="flex flex-col gap-token-1" aria-label="Graveyard">
            {graveyard.map((card) => (
              <li
                key={card.instanceId}
                className="flex items-center justify-between gap-token-2 px-token-2 py-token-1 rounded-token-md bg-glass-bg border border-glass-border"
              >
                <span className="text-sm text-text-primary truncate">{card.name}</span>
                <span className="flex gap-token-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => dispatch({ type: "RETURN_TO_HAND", from: "graveyard", instanceId: card.instanceId })}
                    aria-label={`Return ${card.name} to hand`}
                  >
                    Hand
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => dispatch({ type: "MOVE_TO_EXILE", from: "graveyard", instanceId: card.instanceId })}
                    aria-label={`Exile ${card.name}`}
                  >
                    Exi
                  </Button>
                </span>
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}
