/**
 * GoldfishHand — Phase 4.9 Stage 2.
 *
 * Renders the hand zone. Click a card to play it (auto-categorized via
 * type_line first-type heuristic). Right-click → context menu deferred to
 * 4.9.1 — for now, supplementary keyboard buttons handle move-to-graveyard
 * + move-to-exile from the hand.
 */
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import type { GoldfishCard, PlayAction } from "../../lib/goldfishState";

export type GoldfishHandProps = {
  hand: ReadonlyArray<GoldfishCard>;
  dispatch: (action: PlayAction) => void;
};

export default function GoldfishHand({ hand, dispatch }: GoldfishHandProps) {
  return (
    <Card>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Hand <Badge variant="info">{hand.length}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        {hand.length === 0 ? (
          <div className="text-sm text-text-muted">Hand is empty.</div>
        ) : (
          <ul className="flex flex-col gap-token-1 overflow-x-auto" aria-label="Hand">
            {hand.map((card) => (
              <li
                key={card.instanceId}
                className="flex flex-wrap items-center justify-between gap-token-2 px-token-2 py-token-1 rounded-token-md bg-glass-bg border border-glass-border"
              >
                <span className="text-sm text-text-primary truncate">{card.name}</span>
                <span className="flex gap-token-1">
                  <Button
                    size="sm"
                    variant="primary"
                    onClick={() => dispatch({ type: "PLAY_CARD", from: "hand", instanceId: card.instanceId })}
                    aria-label={`Play ${card.name}`}
                  >
                    Play
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => dispatch({ type: "MOVE_TO_GRAVEYARD", from: "hand", instanceId: card.instanceId })}
                    aria-label={`Discard ${card.name}`}
                  >
                    Discard
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => dispatch({ type: "MOVE_TO_EXILE", from: "hand", instanceId: card.instanceId })}
                    aria-label={`Exile ${card.name}`}
                  >
                    Exile
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
