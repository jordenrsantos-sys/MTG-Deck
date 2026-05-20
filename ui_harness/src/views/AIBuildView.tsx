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
import { useState } from "react";

import { Card, CardHeader, CardBody } from "../ui/primitives/Card";
import Button from "../ui/primitives/Button";
import Badge from "../ui/primitives/Badge";
import Chip from "../ui/primitives/Chip";
import Input from "../ui/primitives/Input";
import Select from "../ui/primitives/Select";

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

type Summary = {
  themes_classified?: Array<{ theme_id?: string; name?: string; confidence?: number }>;
  bracket_placement?: string;
  bracket_estimate?: { bracket?: string } | null;
  color_identity?: string[];
  strength_check?: StrengthSummary | null;
  creativity_envelope_metrics?: EnvelopeMetrics;
  endpoint_call_count?: number;
  phase_timings_ms?: { pool?: number; select?: number; validate?: number };
  validation_issues?: Array<{ code: string; message: string }>;
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

  const [building, setBuilding] = useState(false);
  const [response, setResponse] = useState<BuildResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

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
    setBuilding(true);
    setErrorMessage(null);
    setResponse(null);
    try {
      const res = await fetch(`${API_BASE_URL}/agent/build_deck_v1`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          db_snapshot_id: snap,
          commander: cmdr,
          bracket,
          theme_hints: themeHints,
          must_include_cards: mustIncludes,
        }),
      });
      if (!res.ok) {
        setErrorMessage(`Build failed: HTTP ${res.status} ${res.statusText}`);
        return;
      }
      const data = (await res.json()) as BuildResponse;
      setResponse(data);
    } catch (exc) {
      setErrorMessage(`Request error: ${(exc as Error).message ?? String(exc)}`);
    } finally {
      setBuilding(false);
    }
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
                  placeholder="e.g. Edgar Markov"
                  aria-label="Commander name"
                />
              </label>

              <label className="block text-sm">
                <span className="block text-text-muted mb-token-1">Snapshot ID</span>
                <Input
                  value={snapshotId}
                  onChange={(e) => setSnapshotId(e.target.value)}
                  placeholder="e.g. SCRYFALL_2026_03_15"
                  aria-label="Snapshot ID"
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
                    placeholder="e.g. TYPAL_VAMPIRES"
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
                    placeholder="e.g. Vito, Thorn of the Dusk Rose"
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

              <div className="flex items-center gap-token-2 pt-token-2">
                <Button variant="primary" onClick={_build} disabled={building} aria-label="Build deck">
                  {building ? "Building…" : "Build deck"}
                </Button>
                {response?.status === "OK" ? (
                  <Button variant="secondary" onClick={_applyToWorkspace} aria-label="Apply to workspace">
                    Apply to Workspace
                  </Button>
                ) : null}
              </div>

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
