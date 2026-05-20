/**
 * DeckCombosPanel — v1.7.2 Stage 2.
 *
 * Renders the engine's deck-combo insight surfaces from
 * `/deck/complete_v1`:
 *
 *   - `detected_combos_v1`  — pairs where both halves are in the deck.
 *     Shows "X + Y → outcome" with a Spellbook link.
 *   - `missing_partners_v1` — pairs where one half is present and the
 *     partner is not being added. Shows "Add Y to enable a combo with
 *     X → outcome". Capped to top-10 with a "Show all" toggle.
 *
 * Pillar A.6 Stage 4: tabbed layout. The two sections previously stacked
 * vertically inside the drawer body now render as tabs at the top of the
 * panel body. The "Active" tab (detected) is the default when detected
 * entries exist; otherwise the "Suggested" tab is the initial active tab.
 * Both triggers stay clickable regardless of which is empty so the user
 * can freely flip between them.
 *
 * Per-spec design:
 *   - Tab triggers use the existing Button primitive (no new shadcn or
 *     external deps per spec).
 *   - Active tab persisted only via local component state.
 *   - Null-render contract preserved — the panel hides entirely when both
 *     surfaces are empty (combo-free decks keep the workspace clean).
 *   - Reuses Card / Badge primitives — same visual weight as before.
 *   - Spellbook link href: `https://commanderspellbook.com/combo/{variant_id}`.
 */
import { useEffect, useMemo, useState } from "react";
import Badge from "../../ui/primitives/Badge";
import Button from "../../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";


export type DetectedComboEntry = {
  variant_id: string;
  card_a_name: string;
  card_a_oracle_id: string;
  card_b_name: string;
  card_b_oracle_id: string;
  combo_outcome_label: string;
};


export type MissingPartnerEntry = {
  variant_id: string;
  present_card_name: string;
  present_card_oracle_id: string;
  partner_card_name: string;
  partner_card_oracle_id: string;
  combo_outcome_label: string;
};


export type DeckCombosPanelProps = {
  detected_combos_v1?: ReadonlyArray<DetectedComboEntry>;
  missing_partners_v1?: ReadonlyArray<MissingPartnerEntry>;
  className?: string;
};


export const MISSING_PARTNERS_INITIAL_VISIBLE = 10;


type ComboTabKey = "active" | "suggested";


function spellbookHref(variantId: string): string {
  // The Commander Spellbook combo permalink mirrors the variant id;
  // safe for use as an external anchor target.
  return `https://commanderspellbook.com/combo/${encodeURIComponent(variantId)}`;
}


export default function DeckCombosPanel(props: DeckCombosPanelProps) {
  const detected = Array.isArray(props.detected_combos_v1) ? props.detected_combos_v1 : [];
  const missing = Array.isArray(props.missing_partners_v1) ? props.missing_partners_v1 : [];
  const [missingShowAll, setMissingShowAll] = useState(false);
  // Default tab: "active" if detected has entries; otherwise "suggested".
  // Both tabs remain clickable regardless of emptiness — the default only
  // controls the initial render.
  const initialTab: ComboTabKey = detected.length > 0 ? "active" : "suggested";
  const [activeTab, setActiveTab] = useState<ComboTabKey>(initialTab);

  // When the upstream arrays change shape (re-Complete flips a section
  // from empty → non-empty), pick a sensible default again so the user
  // doesn't land on an empty tab when the other one suddenly has rows.
  useEffect(() => {
    if (activeTab === "active" && detected.length === 0 && missing.length > 0) {
      setActiveTab("suggested");
      return;
    }
    if (activeTab === "suggested" && missing.length === 0 && detected.length > 0) {
      setActiveTab("active");
    }
  }, [activeTab, detected.length, missing.length]);

  const visibleMissing = useMemo(() => {
    if (missingShowAll) return missing;
    return missing.slice(0, MISSING_PARTNERS_INITIAL_VISIBLE);
  }, [missing, missingShowAll]);

  // Null-render contract — the panel hides entirely when both fields
  // are empty so the workspace stays clean for combo-free decks. The
  // wiring in WorkspaceView still mounts the component unconditionally;
  // the null return keeps DOM clean without prop-level gating.
  if (detected.length === 0 && missing.length === 0) {
    return null;
  }

  return (
    <Card className={props.className} data-v172-stage="deck-combos-panel">
      <CardHeader>
        <span className="flex items-center justify-between gap-token-3 w-full">
          <span className="flex items-center gap-token-2">
            Deck combos
            {detected.length > 0 ? (
              <Badge variant="success">{detected.length} detected</Badge>
            ) : null}
            {missing.length > 0 ? (
              <Badge variant="info">{missing.length} suggested</Badge>
            ) : null}
          </span>
        </span>
      </CardHeader>
      <CardBody>
        <div
          role="tablist"
          aria-label="Deck combo tabs"
          className="flex items-center gap-token-2 mb-token-3 border-b border-glass-border pb-token-1"
          data-v172-tablist="combos"
        >
          <Button
            size="sm"
            variant={activeTab === "active" ? "primary" : "ghost"}
            onClick={() => setActiveTab("active")}
            role="tab"
            aria-selected={activeTab === "active"}
            aria-controls="deck-combos-tab-active"
            data-combo-tab="active"
            data-active={activeTab === "active" ? "true" : "false"}
          >
            Active ({detected.length})
          </Button>
          <Button
            size="sm"
            variant={activeTab === "suggested" ? "primary" : "ghost"}
            onClick={() => setActiveTab("suggested")}
            role="tab"
            aria-selected={activeTab === "suggested"}
            aria-controls="deck-combos-tab-suggested"
            data-combo-tab="suggested"
            data-active={activeTab === "suggested" ? "true" : "false"}
          >
            Suggested ({missing.length})
          </Button>
        </div>

        {activeTab === "active" ? (
          <section
            id="deck-combos-tab-active"
            role="tabpanel"
            aria-label="Active combos in deck"
          >
            {detected.length === 0 ? (
              <p className="text-sm text-text-muted" data-v172-empty="active">
                No detected combos for this deck yet.
              </p>
            ) : (
              <ul
                className="flex flex-col gap-token-1"
                role="list"
                aria-label="Detected combos list"
                data-v172-section="detected"
              >
                {detected.map((entry) => (
                  <li
                    key={`det-${entry.variant_id}`}
                    className="flex flex-wrap items-baseline gap-token-2 py-token-1 border-b border-glass-border"
                  >
                    <span className="text-sm text-text-primary">
                      <span className="font-semibold">{entry.card_a_name}</span>
                      {" + "}
                      <span className="font-semibold">{entry.card_b_name}</span>
                      {" → "}
                      <span className="text-text-secondary">{entry.combo_outcome_label}</span>
                    </span>
                    <a
                      href={spellbookHref(entry.variant_id)}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-xs text-accent underline"
                      aria-label={`Open Spellbook combo ${entry.variant_id} in a new tab`}
                    >
                      Spellbook
                    </a>
                  </li>
                ))}
              </ul>
            )}
          </section>
        ) : null}

        {activeTab === "suggested" ? (
          <section
            id="deck-combos-tab-suggested"
            role="tabpanel"
            aria-label="Suggested combo partners"
          >
            {missing.length === 0 ? (
              <p className="text-sm text-text-muted" data-v172-empty="suggested">
                No partner suggestions for this deck.
              </p>
            ) : (
              <>
                <ul
                  className="flex flex-col gap-token-1"
                  role="list"
                  aria-label="Missing partner suggestions list"
                  data-v172-section="missing"
                >
                  {visibleMissing.map((entry) => (
                    <li
                      key={`miss-${entry.variant_id}-${entry.partner_card_oracle_id}`}
                      className="flex flex-wrap items-baseline gap-token-2 py-token-1 border-b border-glass-border"
                    >
                      <span className="text-sm text-text-primary">
                        Add <span className="font-semibold">{entry.partner_card_name}</span>
                        {" to enable a combo with "}
                        <span className="font-semibold">{entry.present_card_name}</span>
                        {" → "}
                        <span className="text-text-secondary">{entry.combo_outcome_label}</span>
                      </span>
                      <a
                        href={spellbookHref(entry.variant_id)}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs text-accent underline"
                        aria-label={`Open Spellbook combo ${entry.variant_id} in a new tab`}
                      >
                        Spellbook
                      </a>
                    </li>
                  ))}
                </ul>
                {missing.length > MISSING_PARTNERS_INITIAL_VISIBLE ? (
                  <div className="mt-token-2">
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={() => setMissingShowAll(!missingShowAll)}
                      aria-label={
                        missingShowAll
                          ? `Show fewer (${MISSING_PARTNERS_INITIAL_VISIBLE})`
                          : `Show all ${missing.length} suggested partners`
                      }
                    >
                      {missingShowAll
                        ? `Show fewer (${MISSING_PARTNERS_INITIAL_VISIBLE})`
                        : `Show all (${missing.length})`}
                    </Button>
                  </div>
                ) : null}
              </>
            )}
          </section>
        ) : null}
      </CardBody>
    </Card>
  );
}
