/**
 * DeckThemesPanel — Phase 2.1d.
 *
 * Renders the engine's deck-theme classification surface from
 * `/deck/complete_v1` and `/deck/analyze_v1`:
 *
 *   - `deck_themes_v1` — list of ClassifiedTheme entries the classifier
 *     identified for this deck (typal + main themes that passed
 *     required_signals AND classify_threshold AND not anti_signals).
 *
 * Per-spec design (DESIGN_DECISIONS.md rule 1.1 creativity envelope):
 *   - Shows themes the deck is built around, not a "rank" or "score" that
 *     the user should optimize toward. Score + confidence_band shown for
 *     transparency, but no top-N narrowing — all active themes render.
 *   - Null-render when no themes classify (calibration-honest — we don't
 *     fabricate "this deck has no theme" — empty just means below threshold
 *     for any catalogued theme).
 *   - Reuses Card / Badge primitives — same style as DeckCombosPanel.
 */
import Badge from "../../ui/primitives/Badge";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";


export type ClassifiedTheme = {
  theme_id: string;
  theme_type: string;
  subtype: string | null;
  score: number;
  confidence_band: "HIGH" | "MED" | "LOW" | "BELOW_THRESHOLD";
  classify_threshold: string;
  passed_classify_threshold: boolean;
  anti_signal_hit: boolean;
  contributing_primitives: string[];
};


export type DeckThemesPanelProps = {
  deck_themes_v1?: ReadonlyArray<ClassifiedTheme>;
  className?: string;
};


function bandVariant(band: string): "success" | "info" | "warning" | "neutral" {
  if (band === "HIGH") return "success";
  if (band === "MED") return "info";
  if (band === "LOW") return "warning";
  return "neutral";
}


function prettyThemeName(theme_id: string, subtype: string | null): string {
  // "TYPAL_GOBLINS:Goblin" → "Goblin Tribal"
  // "THEME_CONTROL" → "Control"
  // "THEME_TOKENS" → "Tokens"
  if (theme_id.startsWith("TYPAL_") && subtype) {
    return `${subtype} Tribal`;
  }
  const prefix = theme_id.split(":")[0];
  const cleaned = prefix
    .replace(/^(THEME_|TYPAL_)/, "")
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
  return cleaned;
}


export default function DeckThemesPanel(props: DeckThemesPanelProps) {
  const themes = Array.isArray(props.deck_themes_v1) ? props.deck_themes_v1 : [];

  if (themes.length === 0) {
    return null;
  }

  return (
    <Card className={props.className} data-phase="2.1d-deck-themes-panel">
      <CardHeader>
        <span className="flex items-center justify-between gap-token-3 w-full">
          <span className="flex items-center gap-token-2">
            Deck themes
            <Badge variant="info">{themes.length} active</Badge>
          </span>
        </span>
      </CardHeader>
      <CardBody>
        <ul
          className="flex flex-col gap-token-2"
          role="list"
          aria-label="Classified deck themes"
        >
          {themes.map((theme) => {
            const niceName = prettyThemeName(theme.theme_id, theme.subtype);
            const variant = bandVariant(theme.confidence_band);
            return (
              <li
                key={theme.theme_id}
                className="flex flex-col gap-token-1 py-token-2 border-b border-glass-border last:border-b-0"
              >
                <div className="flex flex-wrap items-baseline gap-token-2">
                  <span className="text-sm font-semibold text-text-primary">
                    {niceName}
                  </span>
                  <Badge variant={variant}>{theme.confidence_band}</Badge>
                  <span className="text-xs text-text-secondary">
                    score {Math.round(theme.score)} ({theme.theme_type})
                  </span>
                </div>
                {theme.contributing_primitives.length > 0 ? (
                  <div className="flex flex-wrap gap-token-1 mt-token-1">
                    {theme.contributing_primitives.slice(0, 8).map((p) => (
                      <span
                        key={`${theme.theme_id}-${p}`}
                        className="text-[10px] font-mono px-token-1 py-[1px] rounded bg-glass-muted text-text-secondary"
                      >
                        {p}
                      </span>
                    ))}
                  </div>
                ) : null}
                <div className="text-[11px] text-text-muted font-mono">
                  {theme.theme_id}
                </div>
              </li>
            );
          })}
        </ul>
      </CardBody>
    </Card>
  );
}
