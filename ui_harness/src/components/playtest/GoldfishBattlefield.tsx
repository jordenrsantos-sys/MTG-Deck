/**
 * GoldfishBattlefield — Phase 4.9 Stage 2.
 *
 * Battlefield grouped by category (creature / land / artifact / enchantment /
 * planeswalker / battle / other). Click to toggle tap; small action buttons
 * for move-to-graveyard / exile / hand. Categorization is the first-type
 * heuristic per autonomous_repair_log soft-safety #5; split / DFC / saga
 * edge cases queued as 4.9.1.
 */
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import type { BattlefieldCategory, BattlefieldEntry, PlayAction } from "../../lib/goldfishState";

const CATEGORY_ORDER: ReadonlyArray<BattlefieldCategory> = [
  "creature",
  "planeswalker",
  "artifact",
  "enchantment",
  "battle",
  "land",
  "other",
];

const CATEGORY_LABEL: Record<BattlefieldCategory, string> = {
  creature: "Creatures",
  planeswalker: "Planeswalkers",
  artifact: "Artifacts",
  enchantment: "Enchantments",
  battle: "Battles",
  land: "Lands",
  other: "Other",
};

export type GoldfishBattlefieldProps = {
  battlefield: ReadonlyArray<BattlefieldEntry>;
  dispatch: (action: PlayAction) => void;
};

function _groupByCategory(battlefield: ReadonlyArray<BattlefieldEntry>): Map<BattlefieldCategory, BattlefieldEntry[]> {
  const out = new Map<BattlefieldCategory, BattlefieldEntry[]>();
  for (const entry of battlefield) {
    const arr = out.get(entry.category) ?? [];
    arr.push(entry);
    out.set(entry.category, arr);
  }
  return out;
}

export default function GoldfishBattlefield({ battlefield, dispatch }: GoldfishBattlefieldProps) {
  const grouped = _groupByCategory(battlefield);
  return (
    <Card>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Battlefield <Badge variant="info">{battlefield.length}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        {battlefield.length === 0 ? (
          <div className="text-sm text-text-muted">Battlefield is empty.</div>
        ) : (
          <div className="flex flex-col gap-token-3" aria-label="Battlefield">
            {CATEGORY_ORDER.filter((cat) => grouped.has(cat)).map((cat) => (
              <section key={cat}>
                <div className="text-xs uppercase tracking-wide text-text-muted mb-token-1">
                  {CATEGORY_LABEL[cat]} ({grouped.get(cat)!.length})
                </div>
                <ul className="flex flex-col gap-token-1">
                  {grouped.get(cat)!.map((entry) => (
                    <li
                      key={entry.card.instanceId}
                      className={`flex items-center justify-between gap-token-2 px-token-2 py-token-1 rounded-token-md border ${
                        entry.tapped
                          ? "bg-bg-elev-1 border-amber-400/40 opacity-70"
                          : "bg-glass-bg border-glass-border"
                      }`}
                    >
                      <span className="text-sm text-text-primary truncate flex items-center gap-token-1">
                        {entry.card.name}
                        {entry.tapped ? <Badge variant="warn">tapped</Badge> : null}
                      </span>
                      <span className="flex gap-token-1">
                        <Button
                          size="sm"
                          variant={entry.tapped ? "ghost" : "primary"}
                          onClick={() =>
                            dispatch({
                              type: entry.tapped ? "UNTAP" : "TAP",
                              instanceId: entry.card.instanceId,
                            })
                          }
                          aria-label={`${entry.tapped ? "Untap" : "Tap"} ${entry.card.name}`}
                        >
                          {entry.tapped ? "Untap" : "Tap"}
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() =>
                            dispatch({ type: "MOVE_TO_GRAVEYARD", from: "battlefield", instanceId: entry.card.instanceId })
                          }
                          aria-label={`Send ${entry.card.name} to graveyard`}
                        >
                          GY
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() =>
                            dispatch({ type: "MOVE_TO_EXILE", from: "battlefield", instanceId: entry.card.instanceId })
                          }
                          aria-label={`Exile ${entry.card.name}`}
                        >
                          Exi
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() =>
                            dispatch({ type: "RETURN_TO_HAND", from: "battlefield", instanceId: entry.card.instanceId })
                          }
                          aria-label={`Return ${entry.card.name} to hand`}
                        >
                          Hand
                        </Button>
                      </span>
                    </li>
                  ))}
                </ul>
              </section>
            ))}
          </div>
        )}
      </CardBody>
    </Card>
  );
}
