/**
 * AIBuildView — Pillar D Phase E.
 *
 * Self-contained UI for `POST /agent/build_deck_v1`. Lets the user supply
 * a commander, bracket, theme hints, and must-include cards; renders the
 * agent's 99-card deck with per-card reasoning + creativity-envelope
 * metrics + bracket / theme / strength callouts.
 *
 * Intentionally NOT wired into WorkspaceView state. The Apply-to-Workspace
 * action writes the deck into localStorage under the same key WorkspaceView
 * uses for hand-off (`mtg-engine.active-deck`), so navigating to
 * `#workspace-decks` picks it up — no reducer coupling needed.
 */
import { useEffect, useState } from "react";

import { Card, CardHeader, CardBody } from "../ui/primitives/Card";
import Button from "../ui/primitives/Button";
import Badge from "../ui/primitives/Badge";
import Chip from "../ui/primitives/Chip";
import Input from "../ui/primitives/Input";
import Select from "../ui/primitives/Select";
import { useBuildStreaming } from "../hooks/useBuildStreaming";

const API_BASE_URL =
  ((import.meta as ImportMeta).env?.VITE_API_BASE_URL as string | undefined) ??
  "http://localhost:8000";

const BRACKETS = ["B1", "B2", "B3", "B4", "B5"] as const;
const BRACKET_LABELS: Record<(typeof BRACKETS)[number], string> = {
  B1: "B1 — Exhibition",
  B2: "B2 — Core",
  B3: "B3 — Upgraded",
  B4: "B4 — Optimized",
  B5: "B5 — cEDH",
};

const ACTIVE_DECK_STORAGE_KEY = "mtg-engine.active-deck";

// Mega-task v5 Phase 4: client-side stopwatch + 240s timeout.
// Iter 5 5-case sweep observed wallclock 110-130s; we use this for the
// "typical" expectation anchor + the 240s safety ceiling.
// Mega-task v6 Phase 1: kept at 240. The earlier 240→480 bump was based on
// a misread (zero events arriving looked like slow builds); the real cause
// was a React 18 StrictMode mountedRef regression in useBuildStreaming
// (now fixed). Iter-7 budget per criterion #5 is ≤130s — 240 stays plenty
// of headroom even with Pillar E v0.5/v0.6 critique + semantic injection
// added later in this mega-task.
const BUILD_TIMEOUT_SECONDS = 240;
const BUILD_TYPICAL_LOW_S = 110;
const BUILD_TYPICAL_HIGH_S = 130;

type DeckCard = { card_name: string; reason: string; source: string };

type EnvelopeMetrics = {
  user_picks_present: number;
  user_picks_total: number;
  must_includes_resolved?: string[];
  must_includes_dropped?: string[];
  staples_avoided_count: number;
  theme_coherence_score: number;
};

type StrengthSummary = {
  bracket_signal?: string;
  mean_similarity?: number;
  nearest_neighbors_count?: number;
};

type LlmCall = {
  phase: string;
  ok: boolean;
  input_tokens: number;
  output_tokens: number;
  cost_usd: number;
  latency_ms: number;
  error_code?: string | null;
  retries?: number;
};

type LlmMetrics = {
  available: boolean;
  model: string | null;
  calls: LlmCall[];
  total_cost_usd: number;
  total_input_tokens: number;
  total_output_tokens: number;
  total_latency_ms: number;
};

type NovelComboFlag = {
  cards: string[];
  outcome: string;
  in_spellbook: boolean;
  source: string;
  applied_swap?: boolean;
  removed_card?: string;
};

type ConsiderAdding = { card: string; why: string };

type IntentAnalysis = {
  must_include_analysis?: Array<{
    card?: string;
    type?: string;
    key_abilities?: string[];
    signals_archetype?: string;
  }>;
  implicit_themes?: string[];
  suggested_extensions?: Array<{ card?: string; why?: string }>;
  conflict_warnings?: string[];
  likely_win_condition?: string;
};

type Summary = {
  themes_classified?: Array<{ theme_id?: string; name?: string; confidence?: number }>;
  bracket_placement?: string;
  bracket_estimate?: { bracket?: string } | null;
  color_identity?: string[];
  strength_check?: StrengthSummary | null;
  creativity_envelope_metrics?: EnvelopeMetrics & { creativity_delta_count?: number };
  endpoint_call_count?: number;
  phase_timings_ms?: { pool?: number; select?: number; validate?: number };
  validation_issues?: Array<{ code: string; message: string }>;
  // Iteration 2 additions.
  llm_metrics?: LlmMetrics;
  summary_narrative?: string | null;
  consider_adding?: ConsiderAdding[];
  novel_combo_flags?: NovelComboFlag[];
  intent_analysis?: IntentAnalysis | null;
};

type BuildResponse = {
  version: string;
  status: "OK" | "FAILED";
  deck: DeckCard[];
  summary: Summary;
  warnings: Array<{ code: string; message: string }>;
  elapsed_ms: number;
};

const SLOT_ORDER: Array<[string, (c: DeckCard) => boolean]> = [
  ["Commander", (c) => c.source === "user_intent" && c.reason.startsWith("Commander")],
  ["Lands", (c) =>
    c.reason.includes("[slot=land]") ||
    c.source === "mana_base" ||
    ["Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"].includes(c.card_name)],
  ["Ramp", (c) => c.reason.includes("[slot=ramp]")],
  ["Card Draw", (c) => c.reason.includes("[slot=card_draw]")],
  ["Removal", (c) => c.reason.includes("[slot=removal]")],
  ["Win Conditions", (c) => c.reason.includes("[slot=win_condition]")],
  ["Creatures", (c) => c.reason.includes("[slot=creature]")],
  ["Flex / Other", () => true], // catch-all
];

function _groupBySlot(deck: DeckCard[]): Array<[string, DeckCard[]]> {
  const used = new Set<number>();
  const groups: Array<[string, DeckCard[]]> = [];
  for (const [label, predicate] of SLOT_ORDER) {
    const bucket: DeckCard[] = [];
    deck.forEach((card, i) => {
      if (used.has(i)) return;
      if (predicate(card)) {
        bucket.push(card);
        used.add(i);
      }
    });
    if (bucket.length > 0) {
      groups.push([label, bucket]);
    }
  }
  return groups;
}

function _formatPct(x: number | undefined): string {
  if (typeof x !== "number" || !Number.isFinite(x)) return "—";
  return `${(x * 100).toFixed(0)}%`;
}

// Mega-task v5 Phase 3: humanize the SSE phase identifiers for the progress
// panel. Falls back to the raw identifier if the phase is unrecognized so a
// future server-side phase addition doesn't break the UI.
const _PHASE_LABELS: Record<string, string> = {
  intent_interpreter: "Inferring intent (LLM #1)",
  candidate_pool: "Building candidate pool",
  select_deck: "Selecting slots",
  c21_c22_parallel: "Refining picks (LLM #2 + #3 in parallel)",
  validate_swap: "Validating + swap-iterating",
  final_critic: "Rewriting rationales (LLM #4)",
  mana_base: "Reconciling mana base",
  card_advantage: "Reconciling card advantage",
  structural_safety_net: "Enforcing structural invariants",
  complete: "Complete",
};

function _formatPhaseLabel(phase: string | null): string {
  if (!phase) return "Starting build…";
  return _PHASE_LABELS[phase] ?? phase;
}

type AIBuildViewProps = {
  onBack?: () => void;
};

export default function AIBuildView(props: AIBuildViewProps) {
  const { onBack } = props;

  const [commander, setCommander] = useState("");
  const [bracket, setBracket] = useState<(typeof BRACKETS)[number]>("B3");
  const [themeInput, setThemeInput] = useState("");
  const [themeHints, setThemeHints] = useState<string[]>([]);
  const [mustIncludeInput, setMustIncludeInput] = useState("");
  const [mustIncludes, setMustIncludes] = useState<string[]>([]);
  const [snapshotId, setSnapshotId] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [snapshotAutoLoaded, setSnapshotAutoLoaded] = useState(false);

  const [response, setResponse] = useState<BuildResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // Mega-task v5 Phase 3: SSE build progress streaming. The hook handles the
  // stream lifecycle (open, parse, abort on unmount) and exposes phase /
  // elapsed_s / cost_usd / finalResponse. Builds always go through the
  // streaming endpoint; the non-streaming endpoint is still available for
  // Python/programmatic callers.
  const stream = useBuildStreaming({ apiBaseUrl: API_BASE_URL });
  const building = stream.isStreaming;

  // Mega-task v5 Phase 4: client-side stopwatch + 240s timeout. The
  // server-emitted elapsed_s updates only at phase boundaries (so it can sit
  // at the same value for 30+ seconds during a slow LLM call); the wallclock
  // stopwatch ticks every second so the user sees continuous progress.
  // Driven entirely off mount/unmount of the build (resets on each new build).
  const [wallSeconds, setWallSeconds] = useState(0);
  useEffect(() => {
    if (!building) {
      setWallSeconds(0);
      return;
    }
    const startedAt = Date.now();
    const intervalId = window.setInterval(() => {
      const elapsed = (Date.now() - startedAt) / 1000;
      setWallSeconds(elapsed);
      if (elapsed > BUILD_TIMEOUT_SECONDS) {
        // Force cancel + surface explicit error per kickoff spec.
        stream.cancel();
        setErrorMessage(
          "Build exceeded expected duration. Check engine logs in launch_dev.cmd terminal."
        );
      }
    }, 250);
    return () => {
      window.clearInterval(intervalId);
    };
    // We deliberately depend only on `building` so the timer resets on each
    // new build but not on every event tick.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [building]);

  // Push the final response (when stream completes) into the local response
  // state so the existing deck-render JSX picks it up unchanged.
  useEffect(() => {
    if (stream.finalResponse != null) {
      setResponse(stream.finalResponse as BuildResponse);
    }
  }, [stream.finalResponse]);

  // Surface stream errors into the existing errorMessage banner.
  useEffect(() => {
    if (stream.errorMessage) {
      setErrorMessage(stream.errorMessage);
    }
  }, [stream.errorMessage]);

  // Mega-task v5 Phase 2: auto-default the snapshot id from /snapshots/active
  // so the user never has to know this internal db identifier exists. If the
  // fetch fails, the user can still expand Advanced and type one in.
  useEffect(() => {
    let cancelled = false;
    async function loadActiveSnapshot() {
      try {
        const res = await fetch(`${API_BASE_URL}/snapshots/active`);
        if (!res.ok) return;
        const data = (await res.json()) as { snapshot_id?: string };
        if (cancelled) return;
        if (typeof data.snapshot_id === "string" && data.snapshot_id) {
          setSnapshotId(data.snapshot_id);
          setSnapshotAutoLoaded(true);
        }
      } catch {
        // Silent fallback: user can still set snapshot id manually via Advanced.
      }
    }
    void loadActiveSnapshot();
    return () => {
      cancelled = true;
    };
  }, []);

  function _addChip(input: string, list: string[], setter: (xs: string[]) => void, clearInput: () => void) {
    const value = input.trim();
    if (!value) return;
    if (list.includes(value)) {
      clearInput();
      return;
    }
    setter([...list, value]);
    clearInput();
  }

  function _removeChip(idx: number, list: string[], setter: (xs: string[]) => void) {
    setter(list.filter((_, i) => i !== idx));
  }

  async function _build() {
    const cmdr = commander.trim();
    if (!cmdr) {
      setErrorMessage("Commander is required.");
      return;
    }
    const snap = snapshotId.trim();
    if (!snap) {
      setErrorMessage("Snapshot ID is required. (Tip: see the workspace toolbar for the active snapshot.)");
      return;
    }
    setErrorMessage(null);
    setResponse(null);
    await stream.start({
      db_snapshot_id: snap,
      commander: cmdr,
      bracket,
      theme_hints: themeHints,
      must_include_cards: mustIncludes,
    });
  }

  // Mega-task v5 Phase 4: user-initiated abort. The hook's cancel() aborts
  // the underlying fetch; we also clear any partial response so the UI
  // returns to the pre-build state.
  function _cancelBuild() {
    stream.cancel();
    stream.reset();
    setResponse(null);
    setErrorMessage("Build cancelled.");
  }

  function _applyToWorkspace() {
    if (!response || response.status !== "OK") return;
    const lines = ["Commander", `1 ${commander}`, "Deck"];
    for (const c of response.deck.slice(1)) {
      lines.push(`1 ${c.card_name}`);
    }
    const decklistText = lines.join("\n");
    try {
      window.localStorage.setItem(
        ACTIVE_DECK_STORAGE_KEY,
        JSON.stringify({
          commander,
          decklist_text: decklistText,
          source: "ai_build",
          created_at: new Date().toISOString(),
        })
      );
      window.location.hash = "#workspace-decks";
    } catch (exc) {
      setErrorMessage(`Apply failed: ${(exc as Error).message ?? String(exc)}`);
    }
  }

  const grouped = response ? _groupBySlot(response.deck) : [];

  return (
    <main
      className="min-h-screen bg-bg-base text-text-primary p-token-3 md:p-panel-pad"
      aria-label="AI Build"
    >
      <header className="mb-token-3 flex items-baseline justify-between gap-token-3">
        <div>
          <h1 className="text-3xl font-semibold">AI Build</h1>
          <p className="text-sm text-text-muted mt-token-1">
            Build a 99-card deck from intent. The agent honors your picks, your themes, and your bracket —
            no forced staples that don't match what you asked for.
          </p>
        </div>
        {onBack ? (
          <Button variant="secondary" onClick={onBack} aria-label="Back to landing">
            Back
          </Button>
        ) : null}
      </header>

      {/* Iteration 2 — LLM layer unavailability banner. Appears between
          the header and the inputs whenever the response carries a
          LLM_LAYER_UNAVAILABLE warning. Lets the user know they're in
          iteration-1 fallback mode without burying it in the warnings
          drawer. */}
      {response?.warnings?.some((w) => w.code === "LLM_LAYER_UNAVAILABLE") ? (
        <div
          className="mb-token-3 rounded border border-amber-500/40 bg-amber-500/10 p-token-3 text-sm text-amber-200"
          role="alert"
          data-testid="llm-unavailable-banner"
        >
          <strong>LLM reasoning unavailable</strong> — pure pattern-match mode.
          Set <code>ANTHROPIC_API_KEY</code> in your server environment and restart
          to enable iteration-2 semantic reasoning (creativity boosts, wild-combo
          discovery, narrative summaries, per-card rationale rewrites).
        </div>
      ) : null}

      <section className="grid grid-cols-1 lg:grid-cols-2 gap-token-3 mb-token-3">
        <Card>
          <CardHeader>Inputs</CardHeader>
          <CardBody>
            <div className="space-y-token-3">
              <label className="block text-sm">
                <span className="block text-text-muted mb-token-1">Commander</span>
                <Input
                  value={commander}
                  onChange={(e) => setCommander(e.target.value)}
                  placeholder="e.g., Edgar Markov"
                  aria-label="Commander name"
                />
              </label>

              <label className="block text-sm">
                <span className="block text-text-muted mb-token-1">Bracket</span>
                <Select
                  value={bracket}
                  onChange={(e) => setBracket(e.target.value as (typeof BRACKETS)[number])}
                  aria-label="Bracket"
                >
                  {BRACKETS.map((b) => (
                    <option key={b} value={b}>
                      {BRACKET_LABELS[b]}
                    </option>
                  ))}
                </Select>
              </label>

              <fieldset>
                <legend className="block text-sm text-text-muted mb-token-1">
                  Theme hints (press Enter to add)
                </legend>
                <div className="flex gap-token-2 mb-token-2">
                  <Input
                    value={themeInput}
                    onChange={(e) => setThemeInput(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        _addChip(themeInput, themeHints, setThemeHints, () => setThemeInput(""));
                      }
                    }}
                    placeholder="e.g., aristocrats, graveyard recursion (optional — agent infers from cards)"
                    aria-label="Add theme hint"
                  />
                  <Button
                    variant="secondary"
                    onClick={() =>
                      _addChip(themeInput, themeHints, setThemeHints, () => setThemeInput(""))
                    }
                    aria-label="Add theme hint"
                  >
                    Add
                  </Button>
                </div>
                <div className="flex flex-wrap gap-token-1">
                  {themeHints.map((h, i) => (
                    <Chip key={h} className="ui-chip-removable">
                      {h}
                      <button
                        type="button"
                        className="ml-token-1 text-text-muted hover:text-text-primary"
                        aria-label={`Remove theme ${h}`}
                        onClick={() => _removeChip(i, themeHints, setThemeHints)}
                      >
                        ×
                      </button>
                    </Chip>
                  ))}
                </div>
              </fieldset>

              <fieldset>
                <legend className="block text-sm text-text-muted mb-token-1">
                  Must-include cards (press Enter to add)
                </legend>
                <div className="flex gap-token-2 mb-token-2">
                  <Input
                    value={mustIncludeInput}
                    onChange={(e) => setMustIncludeInput(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        _addChip(mustIncludeInput, mustIncludes, setMustIncludes, () =>
                          setMustIncludeInput("")
                        );
                      }
                    }}
                    placeholder="e.g., Vito, Thorn of the Dusk Rose"
                    aria-label="Add must-include card"
                  />
                  <Button
                    variant="secondary"
                    onClick={() =>
                      _addChip(mustIncludeInput, mustIncludes, setMustIncludes, () =>
                        setMustIncludeInput("")
                      )
                    }
                    aria-label="Add must-include card"
                  >
                    Add
                  </Button>
                </div>
                <div className="flex flex-wrap gap-token-1">
                  {mustIncludes.map((m, i) => (
                    <Chip key={m} className="ui-chip-removable">
                      {m}
                      <button
                        type="button"
                        className="ml-token-1 text-text-muted hover:text-text-primary"
                        aria-label={`Remove ${m}`}
                        onClick={() => _removeChip(i, mustIncludes, setMustIncludes)}
                      >
                        ×
                      </button>
                    </Chip>
                  ))}
                </div>
              </fieldset>

              {/* Mega-task v5 Phase 2: Snapshot ID is an internal db
                  identifier the agent needs but users have no reason to know.
                  It's auto-populated from /snapshots/active on mount and
                  hidden behind this Advanced toggle so the default flow is
                  Commander -> Bracket -> Build. */}
              <details
                open={advancedOpen}
                onToggle={(e) => setAdvancedOpen((e.target as HTMLDetailsElement).open)}
                data-testid="advanced-options"
              >
                <summary className="cursor-pointer text-text-muted text-sm select-none">
                  Advanced options
                </summary>
                <div className="mt-token-2">
                  <label className="block text-sm">
                    <span className="block text-text-muted mb-token-1">
                      Snapshot ID
                      {snapshotAutoLoaded ? (
                        <span className="ml-token-1 text-xs text-text-muted">
                          (auto-populated from active snapshot)
                        </span>
                      ) : null}
                    </span>
                    <Input
                      value={snapshotId}
                      onChange={(e) => setSnapshotId(e.target.value)}
                      placeholder="e.g., 20260217_190902_tagpass_20260222"
                      aria-label="Snapshot ID"
                      data-testid="snapshot-id-input"
                    />
                  </label>
                </div>
              </details>

              <div className="flex items-center gap-token-2 pt-token-2 flex-wrap">
                {building ? (
                  <Button
                    variant="secondary"
                    onClick={_cancelBuild}
                    aria-label="Cancel build"
                    data-testid="cancel-build-button"
                  >
                    Cancel
                  </Button>
                ) : (
                  <Button variant="primary" onClick={_build} aria-label="Build deck">
                    Build deck
                  </Button>
                )}
                {building ? (
                  <span
                    className="text-sm text-text-muted"
                    data-testid="build-stopwatch"
                    aria-live="polite"
                  >
                    Building… {wallSeconds.toFixed(0)}s (typical {BUILD_TYPICAL_LOW_S}-{BUILD_TYPICAL_HIGH_S}s)
                  </span>
                ) : null}
                {response?.status === "OK" && !building ? (
                  <Button variant="secondary" onClick={_applyToWorkspace} aria-label="Apply to workspace">
                    Apply to Workspace
                  </Button>
                ) : null}
              </div>

              {/* Mega-task v5 Phase 3: build progress display. Shows the
                  current phase + cumulative elapsed + cumulative LLM cost
                  while the SSE stream is running. Once the complete event
                  arrives, isStreaming flips false and the deck section
                  below renders the result. */}
              {building || (stream.events.length > 0 && stream.currentPhase !== "complete") ? (
                <div
                  className="mt-token-2 rounded border border-glass-border-subtle bg-glass-bg-subtle p-token-3 text-sm"
                  data-testid="build-progress-panel"
                  role="status"
                  aria-live="polite"
                >
                  <div className="flex items-baseline justify-between gap-token-2 mb-token-1">
                    <span className="font-medium">
                      {_formatPhaseLabel(stream.currentPhase)}
                    </span>
                    <span className="text-xs text-text-muted" data-testid="build-progress-elapsed">
                      {stream.elapsedSeconds.toFixed(1)}s
                    </span>
                  </div>
                  <div className="text-text-muted text-xs flex gap-token-2 flex-wrap">
                    <span>
                      LLM cost so far:{" "}
                      <strong className="text-text-primary">
                        ${stream.cumulativeCostUsd.toFixed(4)}
                      </strong>
                    </span>
                    <span>typical 110-130s</span>
                  </div>
                  {stream.events.length > 1 ? (
                    <details className="mt-token-1">
                      <summary className="cursor-pointer text-xs text-text-muted">
                        Phase log ({stream.events.length})
                      </summary>
                      <ul className="mt-token-1 text-xs text-text-muted space-y-token-1">
                        {stream.events.map((ev, i) => (
                          <li key={i}>
                            <code>
                              [{ev.elapsed_s.toFixed(1)}s] {ev.phase} {ev.status}
                            </code>
                          </li>
                        ))}
                      </ul>
                    </details>
                  ) : null}
                </div>
              ) : null}

              {errorMessage ? (
                <p className="text-sm text-red-400 mt-token-2" role="alert">
                  {errorMessage}
                </p>
              ) : null}
            </div>
          </CardBody>
        </Card>

        <Card>
          <CardHeader>Summary</CardHeader>
          <CardBody>
            {response?.status === "OK" ? (
              <div className="space-y-token-3 text-sm">
                <div className="flex flex-wrap gap-token-2">
                  <Badge>{response.deck.length} cards</Badge>
                  <Badge>Bracket: {response.summary.bracket_placement ?? "?"}</Badge>
                  {response.summary.bracket_estimate?.bracket ? (
                    <Badge>Estimated: {response.summary.bracket_estimate.bracket}</Badge>
                  ) : null}
                  <Badge>Calls: {response.summary.endpoint_call_count ?? 0}</Badge>
                  <Badge>Elapsed: {response.elapsed_ms}ms</Badge>
                </div>

                <div>
                  <div className="text-text-muted mb-token-1">Color identity</div>
                  <div className="flex gap-token-1">
                    {(response.summary.color_identity ?? []).map((c) => (
                      <Chip key={c}>{c}</Chip>
                    ))}
                  </div>
                </div>

                <div>
                  <div className="text-text-muted mb-token-1">Creativity envelope</div>
                  <ul className="space-y-token-1">
                    <li>
                      User picks present:{" "}
                      <strong>
                        {response.summary.creativity_envelope_metrics?.user_picks_present ?? 0} /{" "}
                        {response.summary.creativity_envelope_metrics?.user_picks_total ?? 0}
                      </strong>
                    </li>
                    <li>
                      Staples avoided:{" "}
                      <strong>{response.summary.creativity_envelope_metrics?.staples_avoided_count ?? 0}</strong>
                    </li>
                    <li>
                      Theme coherence:{" "}
                      <strong>
                        {_formatPct(response.summary.creativity_envelope_metrics?.theme_coherence_score)}
                      </strong>
                    </li>
                  </ul>
                  {response.summary.creativity_envelope_metrics?.must_includes_dropped &&
                    response.summary.creativity_envelope_metrics.must_includes_dropped.length > 0 ? (
                    <div className="mt-token-1 text-amber-400">
                      Dropped:{" "}
                      {response.summary.creativity_envelope_metrics.must_includes_dropped.join(", ")}
                    </div>
                  ) : null}
                </div>

                <div>
                  <div className="text-text-muted mb-token-1">Themes classified</div>
                  <div className="flex flex-wrap gap-token-1">
                    {(response.summary.themes_classified ?? []).map((t, i) => (
                      <Chip key={`${t.theme_id ?? t.name ?? i}`}>{t.theme_id ?? t.name ?? "?"}</Chip>
                    ))}
                  </div>
                </div>

                {response.summary.strength_check ? (
                  <div>
                    <div className="text-text-muted mb-token-1">Strength check</div>
                    <ul className="space-y-token-1">
                      <li>
                        Bracket signal:{" "}
                        <strong>{response.summary.strength_check.bracket_signal ?? "?"}</strong>
                      </li>
                      <li>
                        Mean similarity:{" "}
                        <strong>{response.summary.strength_check.mean_similarity?.toFixed(2) ?? "?"}</strong>
                      </li>
                      <li>
                        Nearest neighbors:{" "}
                        <strong>{response.summary.strength_check.nearest_neighbors_count ?? 0}</strong>
                      </li>
                    </ul>
                  </div>
                ) : null}

                {/* Mega-task v5 Phase 12: graduated playtest report card.
                    Shows per-tier pod winrates with advancement marks
                    so the user can see how the deck handled precons,
                    mid-tier, and high-tier opposition. */}
                {response.summary.graduated_playtest_report?.active &&
                response.summary.graduated_playtest_report?.report ? (
                  <div data-testid="graduated-playtest-block">
                    <div className="text-text-muted mb-token-1">
                      Graduated playtest
                    </div>
                    <div className="flex flex-wrap gap-token-2 mb-token-1">
                      {response.summary.graduated_playtest_report.report.tier_results.map(
                        (t: { tier: number; label: string; pod_winrate: number; advanced: boolean }) => (
                          <Badge key={t.tier}>
                            {t.advanced ? "✓" : "✗"} {t.label}: {(t.pod_winrate * 100).toFixed(0)}%
                          </Badge>
                        )
                      )}
                      <Badge>
                        Status: {response.summary.graduated_playtest_report.report.overall_status}
                      </Badge>
                    </div>
                    {response.summary.graduated_playtest_report.report.suggested_tweaks?.length > 0 ? (
                      <details>
                        <summary className="cursor-pointer text-text-muted text-xs">
                          Suggested tweaks ({response.summary.graduated_playtest_report.report.suggested_tweaks.length})
                        </summary>
                        <ul className="mt-token-1 space-y-token-1 text-xs text-text-muted">
                          {response.summary.graduated_playtest_report.report.suggested_tweaks.map(
                            (s: string, i: number) => (
                              <li key={i}>{s}</li>
                            )
                          )}
                        </ul>
                      </details>
                    ) : null}
                  </div>
                ) : null}

                {response.summary.phase_timings_ms ? (
                  <div>
                    <div className="text-text-muted mb-token-1">Phase timings (ms)</div>
                    <div className="flex gap-token-2">
                      <Badge>pool: {response.summary.phase_timings_ms.pool ?? 0}</Badge>
                      <Badge>select: {response.summary.phase_timings_ms.select ?? 0}</Badge>
                      <Badge>validate: {response.summary.phase_timings_ms.validate ?? 0}</Badge>
                    </div>
                  </div>
                ) : null}

                {/* Iteration 2 — LLM cost / latency summary. Only shown when
                    at least one call fired (i.e., LLM was available AND not
                    all skipped). Format mirrors the kickoff brief:
                    "Built in 23.4s · 4 LLM calls · $0.31" */}
                {response.summary.llm_metrics &&
                response.summary.llm_metrics.calls.length > 0 ? (
                  <div data-testid="llm-metrics-block">
                    <div className="text-text-muted mb-token-1">LLM reasoning layer</div>
                    <div className="flex flex-wrap gap-token-2 mb-token-1">
                      <Badge>
                        Built in {(response.elapsed_ms / 1000).toFixed(1)}s
                      </Badge>
                      <Badge>
                        {response.summary.llm_metrics.calls.length} LLM call
                        {response.summary.llm_metrics.calls.length === 1 ? "" : "s"}
                      </Badge>
                      <Badge>
                        ${response.summary.llm_metrics.total_cost_usd.toFixed(4)}
                      </Badge>
                      <Badge>
                        {response.summary.llm_metrics.total_input_tokens.toLocaleString()} in /{" "}
                        {response.summary.llm_metrics.total_output_tokens.toLocaleString()} out
                      </Badge>
                      <Badge>
                        Model: {response.summary.llm_metrics.model ?? "?"}
                      </Badge>
                    </div>
                    <details>
                      <summary className="cursor-pointer text-text-muted text-xs">
                        Per-call breakdown
                      </summary>
                      <ul className="mt-token-1 space-y-token-1 text-xs text-text-muted">
                        {response.summary.llm_metrics.calls.map((c, i) => (
                          <li key={i}>
                            <code>{c.phase}</code>: {c.ok ? "ok" : `FAILED (${c.error_code ?? "?"})`} ·{" "}
                            {c.input_tokens.toLocaleString()}→{c.output_tokens.toLocaleString()} tok ·{" "}
                            ${c.cost_usd.toFixed(4)} · {(c.latency_ms / 1000).toFixed(1)}s
                            {c.retries ? ` · retries=${c.retries}` : ""}
                          </li>
                        ))}
                      </ul>
                    </details>
                  </div>
                ) : null}

                {response.warnings.length > 0 ? (
                  <details>
                    <summary className="cursor-pointer text-text-muted">
                      Warnings ({response.warnings.length})
                    </summary>
                    <ul className="mt-token-1 space-y-token-1 text-text-muted">
                      {response.warnings.map((w, i) => (
                        <li key={i}>
                          <code className="text-xs">{w.code}</code>: {w.message}
                        </li>
                      ))}
                    </ul>
                  </details>
                ) : null}
              </div>
            ) : (
              <p className="text-sm text-text-muted">
                Fill in the form and click <em>Build deck</em> to see the agent's output here.
              </p>
            )}
          </CardBody>
        </Card>
      </section>

      {/* Iteration 2 — summary narrative + combo flags + consider-adding.
          Sits between the Summary card and the Deck card so users get the
          LLM's editorial take before scrolling through the cardlist. */}
      {response?.status === "OK" &&
      (response.summary.summary_narrative ||
        (response.summary.novel_combo_flags && response.summary.novel_combo_flags.length > 0) ||
        (response.summary.consider_adding && response.summary.consider_adding.length > 0) ||
        response.summary.intent_analysis) ? (
        <section className="mb-token-3 space-y-token-3" data-testid="iter2-narrative-section">
          {response.summary.summary_narrative ? (
            <Card>
              <CardHeader>Deck narrative</CardHeader>
              <CardBody>
                <p className="text-sm leading-relaxed" data-testid="summary-narrative">
                  {response.summary.summary_narrative}
                </p>
              </CardBody>
            </Card>
          ) : null}

          {response.summary.novel_combo_flags && response.summary.novel_combo_flags.length > 0 ? (
            <Card>
              <CardHeader>
                Combos surfaced ({response.summary.novel_combo_flags.length})
              </CardHeader>
              <CardBody>
                <ul className="space-y-token-2 text-sm" data-testid="novel-combo-flags-list">
                  {response.summary.novel_combo_flags.map((f, i) => (
                    <li key={i} className="border-b border-glass-border-subtle pb-token-2">
                      <div className="flex items-baseline justify-between gap-token-2">
                        <span className="font-medium">
                          {f.cards.join(" + ")}
                        </span>
                        <span className="flex gap-token-1">
                          {f.in_spellbook ? (
                            <Badge>Spellbook combo</Badge>
                          ) : (
                            <Badge>Novel</Badge>
                          )}
                          {f.applied_swap ? <Badge>Applied as swap</Badge> : null}
                        </span>
                      </div>
                      <div className="text-text-muted text-xs mt-token-1">
                        {f.outcome}
                      </div>
                      {f.removed_card ? (
                        <div className="text-text-muted text-xs mt-token-1">
                          Replaced: {f.removed_card}
                        </div>
                      ) : null}
                    </li>
                  ))}
                </ul>
              </CardBody>
            </Card>
          ) : null}

          {response.summary.consider_adding && response.summary.consider_adding.length > 0 ? (
            <Card>
              <CardHeader>Suggested additions to evaluate</CardHeader>
              <CardBody>
                <ul className="space-y-token-2 text-sm" data-testid="consider-adding-list">
                  {response.summary.consider_adding.map((c, i) => (
                    <li key={i} className="border-b border-glass-border-subtle pb-token-2">
                      <div className="font-medium">{c.card}</div>
                      <div className="text-text-muted text-xs mt-token-1">{c.why}</div>
                    </li>
                  ))}
                </ul>
              </CardBody>
            </Card>
          ) : null}

          {response.summary.intent_analysis ? (
            <Card>
              <CardHeader>Intent analysis</CardHeader>
              <CardBody>
                <div className="space-y-token-2 text-sm">
                  {response.summary.intent_analysis.likely_win_condition ? (
                    <div>
                      <div className="text-text-muted mb-token-1">Likely win condition</div>
                      <div>{response.summary.intent_analysis.likely_win_condition}</div>
                    </div>
                  ) : null}
                  {response.summary.intent_analysis.implicit_themes &&
                  response.summary.intent_analysis.implicit_themes.length > 0 ? (
                    <div>
                      <div className="text-text-muted mb-token-1">Implicit themes inferred</div>
                      <div className="flex flex-wrap gap-token-1">
                        {response.summary.intent_analysis.implicit_themes.map((t) => (
                          <Chip key={t}>{t}</Chip>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {response.summary.intent_analysis.suggested_extensions &&
                  response.summary.intent_analysis.suggested_extensions.length > 0 ? (
                    <div>
                      <div className="text-text-muted mb-token-1">Suggested extensions (boosted in pool)</div>
                      <ul className="text-text-muted text-xs space-y-token-1">
                        {response.summary.intent_analysis.suggested_extensions.map((e, i) => (
                          <li key={i}>
                            <strong className="text-text-primary">{e.card}</strong>
                            {e.why ? ` — ${e.why}` : ""}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                </div>
              </CardBody>
            </Card>
          ) : null}
        </section>
      ) : null}

      {response?.status === "OK" ? (
        <Card>
          <CardHeader>Deck</CardHeader>
          <CardBody>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-token-3">
              {grouped.map(([label, cards]) => (
                <section key={label}>
                  <h3 className="text-sm uppercase tracking-wider text-text-muted mb-token-2">
                    {label} ({cards.length})
                  </h3>
                  <ul className="space-y-token-1">
                    {cards.map((c, i) => (
                      <li
                        key={`${c.card_name}-${i}`}
                        className="text-sm border-b border-glass-border-subtle pb-token-1"
                      >
                        <div className="flex items-baseline justify-between">
                          <span className="font-medium">{c.card_name}</span>
                          <code className="text-xs text-text-muted">{c.source}</code>
                        </div>
                        <div className="text-text-muted text-xs mt-token-1" title={c.reason}>
                          {c.reason}
                        </div>
                      </li>
                    ))}
                  </ul>
                </section>
              ))}
            </div>
          </CardBody>
        </Card>
      ) : null}
    </main>
  );
}
