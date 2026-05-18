/**
 * AddedCardRow — v1.1 Stage 1.
 *
 * One row in AddedCardsPanel. Renders a single auto-added card from
 * `/deck/complete_v1`'s `added_cards_v1` array. Shape per WorkspaceView
 * `DeckCompleteAddedCardV1`: `{name, reasons_v1: string[], primitives_added_v1:
 * string[]}`. Reasons render as info Badges; primitives as smaller neutral
 * Badges. Empty-state strings (no reasons / no primitives) suppress the
 * respective Badge group cleanly.
 */
import Badge from "../../ui/primitives/Badge";
// v1.6.2 Stage 3: translate raw engine reason codes to human-readable
// prose at the Badge render seam. Engine emission BYTE-IDENTICAL.
// v1.7 Stage 2: additionally decode COMBO_ENABLER tagged-string reasons
// into a distinct chip variant carrying partner name + outcome label.
import {
  decodeComboEnablerPayload,
  formatComboEnablerChipLabel,
  translateJustification,
} from "../../lib/justificationLabels";

export type AddedCardEntry = {
  name?: string;
  reasons_v1?: ReadonlyArray<string>;
  primitives_added_v1?: ReadonlyArray<string>;
};

export type AddedCardRowProps = {
  entry: AddedCardEntry;
  /** Optional hover callback to wire CardHoverPreview at the parent scope. */
  onHover?: (name: string) => void;
  /** Optional Game-Changer name set (O(1) lookup). When the row's card name
   *  appears in this set, a "GC" Badge renders next to the name with a
   *  tooltip explaining the bracket-floor impact. Sourced from
   *  /deck/complete_v1 → game_changers_v1. Defaults to no badge when omitted
   *  (back-compat for callers that don't yet plumb the set). */
  gameChangers?: ReadonlySet<string>;
};

/** Tooltip copy for the GC badge — spec-fixed. */
export const GAME_CHANGER_TOOLTIP_TEXT =
  "Game Changer — counts toward bracket floor (1-3 GCs → B3, 4+ → B4)";

export default function AddedCardRow({ entry, onHover, gameChangers }: AddedCardRowProps) {
  const name = typeof entry.name === "string" ? entry.name.trim() : "";
  const reasons = Array.isArray(entry.reasons_v1) ? entry.reasons_v1 : [];
  const primitives = Array.isArray(entry.primitives_added_v1) ? entry.primitives_added_v1 : [];
  const safeName = name === "" ? "(unnamed)" : name;
  const isGameChanger = gameChangers !== undefined && name !== "" && gameChangers.has(name);
  return (
    <li
      className="flex flex-wrap items-baseline gap-token-2 py-token-1 border-b border-glass-border"
      onMouseEnter={() => {
        if (onHover && name !== "") onHover(name);
      }}
    >
      <span className="font-semibold text-text-primary text-sm">{safeName}</span>
      {isGameChanger ? (
        // GC badge — uses the warn variant (amber) since the GC list is a
        // bracket-floor signal. Badge primitive doesn't forward title, so
        // wrap with a span that carries the spec tooltip text.
        <span
          data-game-changer="true"
          title={GAME_CHANGER_TOOLTIP_TEXT}
          aria-label={GAME_CHANGER_TOOLTIP_TEXT}
          className="inline-flex"
        >
          <Badge variant="warn">GC</Badge>
        </span>
      ) : null}
      {reasons.length > 0 ? (
        <span className="flex flex-wrap items-center gap-1" aria-label="Reasons">
          {reasons.map((r, i) => {
            // v1.7 Stage 2: COMBO_ENABLER reasons carry a JSON payload —
            // render as a distinct "success"-variant chip with partner
            // name + (truncated) outcome label. Hover-title preserves the
            // full outcome prose.
            const combo = decodeComboEnablerPayload(r);
            if (combo !== null) {
              const chipText = formatComboEnablerChipLabel(combo);
              const titleText = `Enables ${combo.partner_card_name} → ${combo.combo_outcome_label}`;
              return (
                <span
                  key={`r-combo-${i}-${combo.partner_card_oracle_id}`}
                  data-combo-enabler="true"
                  title={titleText}
                  className="inline-flex"
                >
                  <Badge variant="success">{chipText}</Badge>
                </span>
              );
            }
            return (
              <Badge key={`r-${i}-${r}`} variant="info" title={r}>
                {translateJustification(r)}
              </Badge>
            );
          })}
        </span>
      ) : null}
      {primitives.length > 0 ? (
        <span className="flex flex-wrap items-center gap-1 text-xs" aria-label="Primitives added">
          {primitives.map((p, i) => (
            <Badge key={`p-${i}-${p}`} variant="neutral">
              {p}
            </Badge>
          ))}
        </span>
      ) : null}
    </li>
  );
}
