/**
 * SeedBuilderPanel — Phase 4 BUNDLE Stage 4 (sub-task 4.4).
 *
 * Per UI_OVERHAUL_DESIGN D6 (seed builder as workspace MODE) + D9 (bracket
 * awareness first-class throughout): live seed-builder pane wiring
 * usePartialBuild hook → /build/partial?layers=seed_synergy_detection_v1,
 * commander_recommendation_v1 with 200ms debounce.
 *
 * Surfaces:
 *  - Seed cards textarea (newline-separated names) + bracket Select
 *  - Live seed_synergy_detection_v1 readout (matched_themes count,
 *    bracket_compatible_flag warning)
 *  - Live commander_recommendation_v1 top_n=10 panel via injected child
 *    (parent typically passes <CommanderRecommendationPanel /> as `recommendationSlot`)
 *  - Loading / error Toast surfaces (Phase 4.2 primitive)
 */
import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import Badge from "../../ui/primitives/Badge";
import Select from "../../ui/primitives/Select";
import Toast from "../../ui/primitives/Toast";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import {
  usePartialBuild,
  type PartialBuildRequest,
} from "../../lib/usePartialBuild";

const PARTIAL_LAYERS = [
  "seed_synergy_detection_v1",
  "commander_recommendation_v1",
] as const;

const BRACKETS = ["B1", "B2", "B3", "B4", "B5"] as const;

export type SeedBuilderPanelProps = {
  apiBase?: string;
  snapshotId: string;
  profileId: string;
  initialBracketId?: string;
  initialSeedText?: string;
  initialCommander?: string;
  recommendationSlot?: ReactNode;
  onSeedChange?: (cards: string[]) => void;
  onBracketChange?: (bracket: string) => void;
  className?: string;
};

function _splitDecklist(text: string): string[] {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line !== "" && !line.startsWith("#") && !line.startsWith("//"))
    .map((line) => {
      const m = /^\s*(\d+)?\s*[xX]?\s*(.+?)\s*$/.exec(line);
      return (m?.[2] ?? line).trim();
    });
}

export default function SeedBuilderPanel(props: SeedBuilderPanelProps) {
  const {
    apiBase = "",
    snapshotId,
    profileId,
    initialBracketId = "B2",
    initialSeedText = "",
    initialCommander = "",
    recommendationSlot,
    onSeedChange,
    onBracketChange,
    className,
  } = props;

  const [seedText, setSeedText] = useState(initialSeedText);
  const [bracketId, setBracketId] = useState(initialBracketId);

  const cards = useMemo(() => _splitDecklist(seedText), [seedText]);

  const { state, trigger } = usePartialBuild({
    apiBase,
    layers: [...PARTIAL_LAYERS],
    enabled: snapshotId.trim() !== "",
  });

  // Trigger partial build on changes; the hook debounces internally (200ms).
  useEffect(() => {
    if (snapshotId.trim() === "") return;
    const req: PartialBuildRequest = {
      db_snapshot_id: snapshotId,
      profile_id: profileId,
      bracket_id: bracketId,
      commander: initialCommander,
      cards,
    };
    trigger(req);
    onSeedChange?.(cards);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedText, bracketId, snapshotId, profileId]);

  const synergyResult = state.result?.seed_synergy_detection_v1 as
    | { seed_synergy_detection_v1?: { matched_themes?: unknown[]; bracket_compatible_flag?: boolean } }
    | undefined;
  const inner = synergyResult?.seed_synergy_detection_v1 ?? {};
  const matchedThemes = Array.isArray(inner.matched_themes) ? inner.matched_themes : [];
  const bracketCompatible = inner.bracket_compatible_flag !== false;

  function handleBracketChange(value: string) {
    setBracketId(value);
    onBracketChange?.(value);
  }

  return (
    <Card className={className}>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Seed Builder
          {state.loading ? <Badge variant="info">loading…</Badge> : null}
          {!bracketCompatible ? <Badge variant="error">bracket conflict</Badge> : null}
        </span>
      </CardHeader>
      <CardBody>
        <div className="flex flex-col gap-token-3">
          <label className="flex flex-col gap-token-1">
            <span className="text-xs text-text-muted">Active bracket</span>
            <Select
              value={bracketId}
              onChange={(e) => handleBracketChange(e.target.value)}
              aria-label="Active bracket"
            >
              {BRACKETS.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </Select>
          </label>

          <label className="flex flex-col gap-token-1">
            <span className="text-xs text-text-muted">
              Seed cards ({cards.length})
            </span>
            <textarea
              rows={8}
              value={seedText}
              onChange={(e) => setSeedText(e.target.value)}
              placeholder="One card per line; e.g.&#10;1 Sol Ring&#10;Goblin Matron"
              className="w-full bg-glass-bg border border-glass-border rounded-token-md px-token-3 py-token-2 text-text-primary"
              aria-label="Seed cards"
            />
          </label>

          <div className="flex items-center gap-token-2 text-xs text-text-secondary">
            <span>themes matched:</span>
            <Badge variant={matchedThemes.length > 0 ? "info" : "neutral"}>
              {matchedThemes.length}
            </Badge>
            {!bracketCompatible ? (
              <span className="text-amber-300">
                bracket {bracketId} conflict — check seed composition
              </span>
            ) : null}
          </div>

          {state.error ? (
            <Toast variant="error">/build/partial: {state.error}</Toast>
          ) : null}

          {recommendationSlot ? (
            <div className="mt-token-2">{recommendationSlot}</div>
          ) : null}
        </div>
      </CardBody>
    </Card>
  );
}
