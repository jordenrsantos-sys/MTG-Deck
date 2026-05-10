/**
 * GoldfishView — Phase 4.9 Stage 4.
 *
 * Top-level single-player goldfish playtester per UI_OVERHAUL_DESIGN
 * Decision 7 + OQ-5=(b). Composes the playtest components + state
 * machine via `usePlaytest` hook. NO opponents (HARD safety #2).
 *
 * Reads optional staged playtest payload from localStorage slot
 * `mtgdb:playtest:staged_v1` (set by WorkspaceView's "Playtest"
 * button). Payload shape: { deck: GoldfishCard[], commander, seedToken,
 * probability_checkpoint_layer_v1? }. Falls back to a small synthetic
 * deck when no payload is staged so the view is never empty.
 */
import { useEffect, useMemo, useState } from "react";
import Button from "../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../ui/primitives/Card";
import GoldfishHand from "../components/playtest/GoldfishHand";
import GoldfishBattlefield from "../components/playtest/GoldfishBattlefield";
import GoldfishGraveyard from "../components/playtest/GoldfishGraveyard";
import GoldfishExile from "../components/playtest/GoldfishExile";
import GoldfishLibraryControls from "../components/playtest/GoldfishLibraryControls";
import GoldfishLifeAndStats from "../components/playtest/GoldfishLifeAndStats";
import SufficiencyOverlay, {
  type ProbabilityCheckpointLayer,
} from "../components/playtest/SufficiencyOverlay";
import Badge from "../ui/primitives/Badge";
import { usePlaytest } from "../lib/usePlaytest";
import type { GoldfishCard } from "../lib/goldfishState";
// Phase 4.13.1 Stage 4: source-banner adapter (logic-level testable).
import { buildGoldfishBannerText } from "./workspaceIntegrationAdapters";

const STAGED_PLAYTEST_KEY = "mtgdb:playtest:staged_v1";
// Phase 4 BUNDLE Integration (4.13) Stage 3: workspace-deck bridge slot.
// Workspace persists the active deck here on each commander+deckText change;
// Goldfish reads it as a SECONDARY source (after the explicit playtest stage
// slot, before the synthetic fallback). HARD safety #13: synthetic fallback
// path PRESERVED — fires when neither slot is populated.
const ACTIVE_WORKSPACE_DECK_KEY = "mtgdb:workspace:active_deck_v1";

type WorkspaceActiveDeck = {
  commander?: string;
  decklist?: string;
  source?: string;
  staged_at_iso?: string;
};

function _readActiveWorkspaceDeck(): WorkspaceActiveDeck | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(ACTIVE_WORKSPACE_DECK_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object") return parsed as WorkspaceActiveDeck;
    return null;
  } catch {
    return null;
  }
}

/** Convert a `<count> <name>` decklist text into GoldfishCard[] with
 *  expanded copies. Comments + blank lines are skipped. Pure helper. */
export function decklistTextToGoldfishCards(text: string): GoldfishCard[] {
  if (typeof text !== "string" || text.trim() === "") return [];
  const out: GoldfishCard[] = [];
  let instanceCounter = 0;
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#") || line.startsWith("//")) continue;
    const m = /^\s*(\d+)?\s*[xX]?\s*(.+?)\s*$/.exec(line);
    const count = m && m[1] ? Math.max(1, Number.parseInt(m[1], 10) || 1) : 1;
    const name = (m && m[2] ? m[2] : line).trim();
    if (name === "") continue;
    for (let i = 0; i < count; i += 1) {
      out.push({
        instanceId: `active-${instanceCounter}`,
        name,
        oracleId: null,
        typeLine: null,
      });
      instanceCounter += 1;
    }
  }
  return out;
}

export type GoldfishStagedPayload = {
  deck?: GoldfishCard[];
  commander?: GoldfishCard | null;
  seedToken?: string;
  probability_checkpoint_layer_v1?: ProbabilityCheckpointLayer | null;
};

function _readStagedPayload(): GoldfishStagedPayload | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(STAGED_PLAYTEST_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object") return parsed as GoldfishStagedPayload;
    return null;
  } catch {
    return null;
  }
}

function _makeSyntheticDeck(): GoldfishCard[] {
  const out: GoldfishCard[] = [];
  for (let i = 0; i < 24; i += 1) {
    out.push({ instanceId: `forest-${i}`, name: "Forest", oracleId: null, typeLine: "Basic Land — Forest" });
  }
  for (let i = 0; i < 30; i += 1) {
    out.push({ instanceId: `goblin-${i}`, name: "Goblin Token", oracleId: null, typeLine: "Creature — Goblin" });
  }
  for (let i = 0; i < 20; i += 1) {
    out.push({ instanceId: `bolt-${i}`, name: "Lightning Bolt", oracleId: null, typeLine: "Instant" });
  }
  for (let i = 0; i < 25; i += 1) {
    out.push({ instanceId: `relic-${i}`, name: "Iron Relic", oracleId: null, typeLine: "Artifact" });
  }
  return out;
}

export type GoldfishViewProps = {
  onBack?: () => void;
};

export default function GoldfishView({ onBack }: GoldfishViewProps) {
  const { state, dispatch } = usePlaytest({ hotkeysEnabled: true });

  const stagedPayload = useMemo(() => _readStagedPayload(), []);
  // Phase 4 BUNDLE Integration (4.13) Stage 3: secondary source — workspace
  // active deck. Read on mount; synthetic fallback fires only when BOTH
  // slots are empty (HARD safety #13 preserved).
  const workspaceActiveDeck = useMemo(() => _readActiveWorkspaceDeck(), []);
  const deckSource: "playtest_staged" | "workspace_active" | "synthetic" = useMemo(() => {
    if (stagedPayload?.deck && stagedPayload.deck.length > 0) return "playtest_staged";
    if (workspaceActiveDeck?.decklist && workspaceActiveDeck.decklist.trim() !== "") return "workspace_active";
    return "synthetic";
  }, [stagedPayload, workspaceActiveDeck]);
  const startingDeck = useMemo(() => {
    if (deckSource === "playtest_staged") return stagedPayload!.deck!;
    if (deckSource === "workspace_active") {
      return decklistTextToGoldfishCards(workspaceActiveDeck!.decklist ?? "");
    }
    return _makeSyntheticDeck();
  }, [deckSource, stagedPayload, workspaceActiveDeck]);
  const commander =
    stagedPayload?.commander ??
    (workspaceActiveDeck?.commander
      ? { instanceId: "active-cmdr", name: workspaceActiveDeck.commander, oracleId: null, typeLine: null }
      : null);
  const seedToken = stagedPayload?.seedToken;
  const probabilityLayer = stagedPayload?.probability_checkpoint_layer_v1 ?? null;
  const [hasAutoStarted, setHasAutoStarted] = useState(false);

  // Auto-start once on mount when a deck is available; users can RESET + START_GAME again from controls.
  useEffect(() => {
    if (hasAutoStarted) return;
    if (state.phase !== "pre_game") return;
    if (startingDeck.length === 0) return;
    dispatch({ type: "START_GAME", deck: startingDeck, commander, seedToken });
    setHasAutoStarted(true);
  }, [hasAutoStarted, state.phase, startingDeck, commander, seedToken, dispatch]);

  return (
    <main className="min-h-screen bg-bg-base text-text-primary p-token-3" aria-label="Goldfish playtester">
      <header className="flex items-center justify-between mb-token-3">
        <h1 className="text-xl font-semibold">Goldfish playtester</h1>
        <div className="flex items-center gap-token-2">
          <Button
            variant="ghost"
            onClick={() => dispatch({ type: "START_GAME", deck: startingDeck, commander, seedToken })}
            aria-label="Restart game"
          >
            Restart
          </Button>
          {onBack ? (
            <Button variant="ghost" onClick={onBack} aria-label="Back to workspace">
              ← Back
            </Button>
          ) : null}
        </div>
      </header>

      {state.phase === "pre_game" ? (
        <Card>
          <CardHeader>Ready when you are</CardHeader>
          <CardBody>
            <p className="text-sm text-text-muted mb-token-2">
              {deckSource === "playtest_staged"
                ? "Using staged playtest deck; press Start to begin."
                : deckSource === "workspace_active"
                  ? `Using imported deck${
                      workspaceActiveDeck?.commander ? ` (${workspaceActiveDeck.commander})` : ""
                    }; press Start to begin.`
                  : "Using sample deck (no active deck staged) — synthetic 99-card deck for demo purposes."}
            </p>
            <Button
              variant="primary"
              onClick={() => dispatch({ type: "START_GAME", deck: startingDeck, commander, seedToken })}
            >
              Start Game
            </Button>
          </CardBody>
        </Card>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-token-3">
          <section className="flex flex-col gap-token-3 lg:col-span-2">
            {/* Phase 4.13.1 Stage 4: source banner reflecting which deck slot
                fired (3-source precedence preserved per HARD #13). */}
            {(() => {
              const banner = buildGoldfishBannerText({
                deckSource,
                workspaceCommander: workspaceActiveDeck?.commander ?? null,
                workspaceSource: workspaceActiveDeck?.source ?? null,
              });
              return (
                <div role="status" aria-live="polite" aria-label="Deck source">
                  <Badge variant={banner.variant}>{banner.text}</Badge>
                </div>
              );
            })()}
            <GoldfishBattlefield battlefield={state.battlefield} dispatch={dispatch} />
            <GoldfishHand hand={state.hand} dispatch={dispatch} />
            <div className="grid grid-cols-1 md:grid-cols-2 gap-token-3">
              <GoldfishGraveyard graveyard={state.graveyard} dispatch={dispatch} />
              <GoldfishExile exile={state.exile} dispatch={dispatch} />
            </div>
          </section>
          <aside className="flex flex-col gap-token-3">
            <GoldfishLifeAndStats state={state} dispatch={dispatch} />
            <GoldfishLibraryControls
              libraryCount={state.library.length}
              mulligansTaken={state.mulligansTaken}
              dispatch={dispatch}
            />
            <SufficiencyOverlay layer={probabilityLayer} currentTurn={state.turn} />
            {state.commandZone.length > 0 ? (
              <Card>
                <CardHeader>Command zone</CardHeader>
                <CardBody>
                  <ul className="flex flex-col gap-token-1">
                    {state.commandZone.map((c) => (
                      <li key={c.instanceId} className="text-sm text-text-primary">
                        {c.name}
                      </li>
                    ))}
                  </ul>
                </CardBody>
              </Card>
            ) : null}
          </aside>
        </div>
      )}

      <footer className="mt-token-3 text-xs text-text-muted">
        Hotkeys: D=draw · M=mulligan · U=untap all · E=advance turn · S=shuffle · R=reset · Esc=back
      </footer>
    </main>
  );
}
