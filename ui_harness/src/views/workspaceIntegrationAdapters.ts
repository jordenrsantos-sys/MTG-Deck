/**
 * workspaceIntegrationAdapters — Phase 4 BUNDLE Integration (4.13).
 *
 * Pure helpers extracted from WorkspaceView so the integration wiring
 * (which BuildResponse fields feed which Phase 4 BUNDLE component) is
 * testable at the logic level WITHOUT mounting React.
 *
 * Per AUTOMATION_RULES halt-and-ask + autonomous_repair_log soft-safety
 * #7: rendering full WorkspaceView in vitest would require installing
 * @testing-library/react + jsdom (HARD halt territory). Instead, the
 * extracted adapters cover the regression contract: given a BuildResponse,
 * which panels should render and what data flows through them.
 *
 * HARD safety #11: component prop APIs preserved BYTE-IDENTICAL —
 * adapters read EXISTING BuildResponse fields and project to EXISTING
 * component prop shapes; no widening on either side.
 */
import type { BuildResponsePayload } from "../components/workspaceTypes";
import type { SufficiencySummary } from "../components/stats/sufficiencyTypes";
import type { SwapSuggestion } from "../lib/applySwap";
import type { CommanderRecommendation } from "../components/recommendation/commanderTypes";

const ACTIVE_DECK_KEY = "mtgdb:workspace:active_deck_v1";

export const ACTIVE_DECK_STORAGE_KEY = ACTIVE_DECK_KEY;

// Phase 4.13.1 Stage 3 widening: `source` accepts ActiveDeckSourceLabel
// (declared below) — purely additive, the prior values
// ("import" | "build" | "saved_deck" | "manual") REMAIN VALID for backward
// compatibility per HARD safety #14. Runtime payload BYTE-IDENTICAL.
export type ActiveDeckPayload = {
  commander: string;
  decklist: string;
  source: "import" | "build" | "saved_deck" | "manual" | "archidekt" | "arena_text" | "mtgo_text" | "plain_text" | "file_upload";
  staged_at_iso: string;
};

function _resultRecord(buildResponse: BuildResponsePayload | null | undefined): Record<string, unknown> | null {
  if (!buildResponse || typeof buildResponse !== "object") return null;
  const result = (buildResponse as { result?: unknown }).result;
  if (!result || typeof result !== "object") return null;
  return result as Record<string, unknown>;
}

/** Project sufficiency_summary_v1 out of BuildResponse for SufficiencyDashboard. */
export function extractSufficiencySummary(
  buildResponse: BuildResponsePayload | null | undefined,
): SufficiencySummary | null {
  const result = _resultRecord(buildResponse);
  if (!result) return null;
  const raw = result.sufficiency_summary_v1;
  if (!raw || typeof raw !== "object") return null;
  return raw as SufficiencySummary;
}

/** Project seed_to_deck_v1.swap_suggestions[] out of BuildResponse for SwapSuggestionsList. */
export function extractSwapSuggestions(
  buildResponse: BuildResponsePayload | null | undefined,
): SwapSuggestion[] {
  const result = _resultRecord(buildResponse);
  if (!result) return [];
  const seedToDeck = result.seed_to_deck_v1;
  if (!seedToDeck || typeof seedToDeck !== "object") return [];
  const list = (seedToDeck as { swap_suggestions?: unknown }).swap_suggestions;
  if (!Array.isArray(list)) return [];
  return list as SwapSuggestion[];
}

/** Project commander_recommendation_v1 out of BuildResponse. */
export function extractCommanderRecommendation(
  buildResponse: BuildResponsePayload | null | undefined,
): CommanderRecommendation | null {
  const result = _resultRecord(buildResponse);
  if (!result) return null;
  const raw = result.commander_recommendation_v1;
  if (!raw || typeof raw !== "object") return null;
  return raw as CommanderRecommendation;
}

/**
 * Decide whether SufficiencyDashboard should render. True iff a sufficiency
 * summary is present. Component itself shows an empty state if absent —
 * but at the wiring level, we only mount the dashboard when there's data
 * worth showing.
 */
export function shouldShowSufficiencyDashboard(
  buildResponse: BuildResponsePayload | null | undefined,
): boolean {
  return extractSufficiencySummary(buildResponse) !== null;
}

/**
 * Decide whether SwapSuggestionsList should render inside the dashboard.
 * Per spec: render when ANY domain shows FAIL. (If sufficiency is PASS but
 * swaps were emitted, they're informational — render anyway when non-empty.)
 */
export function shouldShowSwapSuggestions(
  buildResponse: BuildResponsePayload | null | undefined,
): boolean {
  const summary = extractSufficiencySummary(buildResponse);
  if (summary) {
    const verdicts = summary.domain_verdicts ?? {};
    for (const v of Object.values(verdicts)) {
      if (v?.status === "FAIL") return true;
    }
  }
  return extractSwapSuggestions(buildResponse).length > 0;
}

/**
 * Decide whether CommanderRecommendationPanel should render. Per spec:
 * when commander === "" or null. Recommendation data may or may not be
 * present — the panel renders an empty state in either case, so we mount
 * it whenever the user hasn't picked a commander yet.
 */
export function shouldShowCommanderRecommendation(
  commander: string | null | undefined,
): boolean {
  return !commander || commander.trim() === "";
}

/** Build the active-deck payload for the workspace→goldfish bridge slot. */
export function buildActiveDeckPayload(input: {
  commander: string;
  decklist: string;
  source: ActiveDeckPayload["source"];
  nowIso?: string;
}): ActiveDeckPayload {
  return {
    commander: input.commander,
    decklist: input.decklist,
    source: input.source,
    staged_at_iso: input.nowIso ?? new Date().toISOString(),
  };
}

// ============================================================================
// Phase 4.13.1 hotfix — additive helpers per HARD safety #14 (existing
// helpers above stay BYTE-IDENTICAL).
// ============================================================================

/**
 * Phase 4.13.1 Stage 3: actual source labels propagated from the
 * staged-import / saved-deck context. Widening of the existing
 * `ActiveDeckPayload.source` union — the prior values
 * ("import" | "build" | "saved_deck" | "manual") REMAIN VALID for backward
 * compatibility; the new variants name the parser registry sources.
 */
export type ActiveDeckSourceLabel =
  | ActiveDeckPayload["source"]
  | "archidekt"
  | "arena_text"
  | "mtgo_text"
  | "plain_text"
  | "file_upload";

/**
 * Map a raw source string (from `ImportResult.source` or a saved-deck flag
 * or a build event) to a canonical ActiveDeckSourceLabel. Defaults to
 * "manual" when the input is null / empty / unrecognized per
 * autonomous_repair_log soft-safety #5.
 */
export function normalizeActiveDeckSource(rawSource: string | null | undefined): ActiveDeckSourceLabel {
  if (typeof rawSource !== "string") return "manual";
  const norm = rawSource.trim().toLowerCase();
  if (norm === "") return "manual";
  // Direct enum matches.
  if (
    norm === "import" ||
    norm === "build" ||
    norm === "saved_deck" ||
    norm === "manual" ||
    norm === "archidekt" ||
    norm === "arena_text" ||
    norm === "mtgo_text" ||
    norm === "plain_text" ||
    norm === "file_upload"
  ) {
    return norm as ActiveDeckSourceLabel;
  }
  // ImportRoute encodes legacy `saved_deck:<id>` with a colon suffix —
  // strip the suffix and recognize the prefix.
  if (norm.startsWith("saved_deck")) return "saved_deck";
  // Per Phase 4.3 `ImportRoute.tsx` the `source` field carries the parser
  // name (arena_text / mtgo_text / plain_text / file_upload / archidekt)
  // OR the source registry id ("archidekt") — both already covered above.
  // Anything else falls through to "manual".
  return "manual";
}

/**
 * Phase 4.13.1 Stage 1: workspace remount state restore. Reads the
 * `mtgdb:workspace:active_deck_v1` slot WITHOUT clearing it (Goldfish
 * still consumes the slot). Returns the restored {commander, decklist}
 * pair OR null when the slot is empty / corrupt / SSR.
 *
 * Per autonomous_repair_log soft-safety #1: corrupt JSON or missing
 * fields fall through to null so the workspace lands on the hardcoded
 * default rather than a broken half-state.
 */
export type ActiveDeckRestore = {
  commander: string;
  decklist: string;
  source: ActiveDeckSourceLabel;
};

export function restoreFromActiveDeckSlot(): ActiveDeckRestore | null {
  if (typeof window === "undefined") return null;
  let raw: string | null;
  try {
    raw = window.localStorage.getItem(ACTIVE_DECK_KEY);
  } catch {
    return null;
  }
  if (!raw) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  const obj = parsed as { commander?: unknown; decklist?: unknown; source?: unknown };
  const commander = typeof obj.commander === "string" ? obj.commander : "";
  const decklist = typeof obj.decklist === "string" ? obj.decklist : "";
  if (commander.trim() === "" || decklist.trim() === "") return null;
  const source = normalizeActiveDeckSource(typeof obj.source === "string" ? obj.source : null);
  return { commander, decklist, source };
}

/**
 * Phase 4.13.1 Stage 4: GoldfishView source banner text.
 * deckSource matches GoldfishView's 3-source precedence chain
 * ("playtest_staged" | "workspace_active" | "synthetic"). Returns banner
 * copy + a Badge variant key per autonomous_repair_log soft-safety #6
 * (under 80 chars; commander name truncated when needed).
 */
export type GoldfishBannerVariant = "info" | "neutral" | "success";
export type GoldfishBannerText = {
  text: string;
  variant: GoldfishBannerVariant;
};

const BANNER_NAME_MAX_LEN = 32;
function _truncate(name: string | null | undefined, max: number): string {
  if (typeof name !== "string") return "";
  const trimmed = name.trim();
  if (trimmed.length <= max) return trimmed;
  return `${trimmed.slice(0, max - 1)}…`;
}

/**
 * Phase 4.13.1 Stage 2: pure /build request payload constructor.
 * Reads current workspace state + returns the BuildRequest body shape
 * Engine-4A's `POST /build` consumes (see `repo/api/main.py`).
 */
export type BuildRequestBody = {
  db_snapshot_id: string;
  profile_id: string;
  bracket_id: string;
  format: "commander";
  commander: string;
  cards: ReadonlyArray<string>;
  engine_patches_v0: ReadonlyArray<unknown>;
};

export function buildBuildRequestBody(input: {
  snapshotId: string;
  profileId: string;
  bracketId: string;
  commander: string;
  cards: ReadonlyArray<string>;
}): BuildRequestBody {
  return {
    db_snapshot_id: input.snapshotId.trim(),
    profile_id: input.profileId.trim() || "focused",
    bracket_id: input.bracketId.trim() || "B2",
    format: "commander",
    commander: input.commander.trim(),
    cards: input.cards,
    engine_patches_v0: [],
  };
}

/**
 * Phase 4.13.1 Stage 2: testable /build invocation. Pure async function —
 * caller injects a fetcher (defaults to global fetch). Returns either
 * `{ok: true, response}` or `{ok: false, error}`. Never throws — wraps
 * network + JSON-parse errors per autonomous_repair_log soft-safety #3.
 */
export type CallBuildResult =
  | { ok: true; response: unknown }
  | { ok: false; error: string };

export async function callBuildEndpoint(
  apiBase: string,
  body: BuildRequestBody,
  fetcher: typeof fetch = fetch,
): Promise<CallBuildResult> {
  let resp: Response;
  try {
    resp = await fetcher(`${apiBase}/build`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : "network error";
    return { ok: false, error: `/build network error: ${msg}` };
  }
  let text = "";
  try {
    text = await resp.text();
  } catch {
    return { ok: false, error: `/build response read error (HTTP ${resp.status})` };
  }
  let parsed: unknown = null;
  if (text.length > 0) {
    try {
      parsed = JSON.parse(text);
    } catch {
      return { ok: false, error: `/build returned non-JSON (HTTP ${resp.status})` };
    }
  }
  if (!resp.ok) {
    return { ok: false, error: `/build HTTP ${resp.status}: ${text.slice(0, 200)}` };
  }
  return { ok: true, response: parsed };
}

/**
 * Phase 4.14 Stage 2: status-pill text computation. Pure derivation from
 * reducer state — given the workspace's deck state + parsed-card count
 * + sufficiency-summary status string, returns a pill text + Badge
 * variant. Caller (WorkspaceView) renders `<Badge variant>{text}</Badge>`.
 *
 * Variants per spec:
 *  - source==="fallback" → "No deck loaded" (neutral)
 *  - !isCompleted → "{commander} · {N cards} · imported from {source}" (info)
 *  - isCompleted && !buildResponse → "{commander} · {N cards} · ready to build" (info)
 *  - buildResponse → "{commander} · {N cards} · built · sufficiency {summary}" (success)
 */
export type WorkspacePillVariant = "neutral" | "info" | "success";
export type WorkspacePillText = {
  text: string;
  variant: WorkspacePillVariant;
};

const PILL_COMMANDER_MAX_LEN = 32;
function _truncatePillName(name: string, max: number): string {
  if (typeof name !== "string") return "";
  const t = name.trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max - 1)}…`;
}

export function buildWorkspacePillText(input: {
  source: string;
  isHydrated?: boolean;
  isCompleted?: boolean;
  hasBuildResponse?: boolean;
  commander: string;
  cardCount: number;
  sufficiencyStatus?: string | null;
  /** v1.1 — count of recommended_swaps_v1 from the latest UPGRADE_SUCCESS.
   *  When > 0, suffixed to the pill text ("· N upgrade suggestions ready"). */
  upgradeSuggestionsCount?: number;
}): WorkspacePillText {
  if (input.source === "fallback") {
    return { text: "No deck loaded", variant: "neutral" };
  }
  const cmdr = _truncatePillName(input.commander, PILL_COMMANDER_MAX_LEN) || "(unnamed)";
  const cards = `${input.cardCount} card${input.cardCount === 1 ? "" : "s"}`;
  const upgradeSuffix =
    typeof input.upgradeSuggestionsCount === "number" && input.upgradeSuggestionsCount > 0
      ? ` · ${input.upgradeSuggestionsCount} upgrade suggestion${input.upgradeSuggestionsCount === 1 ? "" : "s"} ready`
      : "";
  if (input.hasBuildResponse) {
    const sufficiency = input.sufficiencyStatus && input.sufficiencyStatus.trim() !== ""
      ? input.sufficiencyStatus.trim()
      : "—";
    return {
      text: `${cmdr} · ${cards} · built · sufficiency ${sufficiency}${upgradeSuffix}`,
      variant: "success",
    };
  }
  if (input.isCompleted) {
    return { text: `${cmdr} · ${cards} · ready to build${upgradeSuffix}`, variant: "info" };
  }
  return { text: `${cmdr} · ${cards} · imported from ${input.source}${upgradeSuffix}`, variant: "info" };
}

export function buildGoldfishBannerText(input: {
  deckSource: "playtest_staged" | "workspace_active" | "synthetic";
  workspaceCommander?: string | null;
  workspaceSource?: string | null;
}): GoldfishBannerText {
  if (input.deckSource === "playtest_staged") {
    return { text: "Using staged playtest deck", variant: "success" };
  }
  if (input.deckSource === "synthetic") {
    return {
      text: "Using sample deck — import a deck from #import to playtest your own",
      variant: "neutral",
    };
  }
  // workspace_active
  const name = _truncate(input.workspaceCommander ?? "", BANNER_NAME_MAX_LEN) || "active deck";
  const source = normalizeActiveDeckSource(input.workspaceSource);
  return {
    text: `Using imported deck: ${name} · source: ${source}`,
    variant: "info",
  };
}
