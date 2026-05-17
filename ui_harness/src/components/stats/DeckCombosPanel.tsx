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
 * Per-spec design:
 *   - Two collapsible sections, only renders when at least one field
 *     has entries (null-render keeps the workspace clean on no-combo
 *     decks).
 *   - Reuses existing Card / Badge / Button primitives — no new shadcn
 *     or external imports.
 *   - Spellbook link href: `https://commanderspellbook.com/combo/{variant_id}`.
 *   - Optional props (defaults to empty arrays) so legacy responses
 *     without the new engine fields render the panel as null.
 */
import { useMemo, useState } from "react";
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


function spellbookHref(variantId: string): string {
  // The Commander Spellbook combo permalink mirrors the variant id;
  // safe for use as an external anchor target.
  return `https://commanderspellbook.com/combo/${encodeURIComponent(variantId)}`;
}


export default function DeckCombosPanel(props: DeckCombosPanelProps) {
  const detected = Array.isArray(props.detected_combos_v1) ? props.detected_combos_v1 : [];
  const missing = Array.isArray(props.missing_partners_v1) ? props.missing_partners_v1 : [];
  const [detectedOpen, setDetectedOpen] = useState(true);
  const [missingOpen, setMissingOpen] = useState(true);
  const [missingShowAll, setMissingShowAll] = useState(false);

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
        {detected.length > 0 ? (
          <section className="mb-token-3" aria-label="Detected combos">
            <header className="flex items-center justify-between gap-token-2 mb-token-2">
              <h3 className="text-sm font-semibold text-text-primary">
                Detected combos
              </h3>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setDetectedOpen(!detectedOpen)}
                aria-label={detectedOpen ? "Collapse detected combos" : "Expand detected combos"}
                aria-expanded={detectedOpen}
              >
                {detectedOpen ? "Collapse" : "Expand"}
              </Button>
            </header>
            {detectedOpen ? (
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
            ) : null}
          </section>
        ) : null}

        {missing.length > 0 ? (
          <section aria-label="Suggested combo partners">
            <header className="flex items-center justify-between gap-token-2 mb-token-2">
              <h3 className="text-sm font-semibold text-text-primary">
                Suggested combo partners
              </h3>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setMissingOpen(!missingOpen)}
                aria-label={missingOpen ? "Collapse suggested partners" : "Expand suggested partners"}
                aria-expanded={missingOpen}
              >
                {missingOpen ? "Collapse" : "Expand"}
              </Button>
            </header>
            {missingOpen ? (
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
            ) : null}
          </section>
        ) : null}
      </CardBody>
    </Card>
  );
}
