import { useEffect, useId, useMemo, useReducer, useRef, useState } from "react";

import fixtureBuildResult from "../../fixtures/build_result.json";
import BuildHistoryPanel from "../components/BuildHistoryPanel";
// Phase 4.6: ImportRoute hand-off bridge + latency probe.
import { readStagedImport as _readStagedImport, clearStagedImport as _clearStagedImport } from "./ImportRoute";
// Phase 4 BUNDLE Integration (4.13): wire orphaned panels.
import SufficiencyDashboard from "../components/stats/SufficiencyDashboard";
import SwapSuggestionsList from "../components/stats/SwapSuggestionsList";
import AddedCardsPanel from "../components/stats/AddedCardsPanel";
// v1.7.2 Stage 3 — engine's deck-combo insight payload
// (detected_combos_v1 + missing_partners_v1) from /deck/complete_v1.
// DeckCombosPanel is now wrapped by CombosDrawer (right-anchored drawer
//
// Pillar A.6 fix: import the typed entry shapes so the workspace can
// materialize a single memoized arrays-of-typed-entries and feed both
// the toolbar Combos badge AND the drawer from the same reference.
// Previously the badge derived a count from completionResult and the
// drawer read completionResult inline — same source, but two distinct
// computations whose dependency wiring could drift (Bug 2/3).
// toggled from the workspace toolbar) instead of rendered inline.
import CombosDrawer from "../components/stats/CombosDrawer";
import type {
  DetectedComboEntry,
  MissingPartnerEntry,
} from "../components/stats/DeckCombosPanel";
import DeckThemesPanel from "../components/stats/DeckThemesPanel";
// v1.7.5 — bracket-combo violation warning. Renders red banner when the
// engine response carries TWO_CARD_COMBOS_DISALLOWED_B* violations or
// status === "BRACKET_VIOLATION". Self-hides when no bracket-combo
// violations exist (legacy responses + B3+ unaffected).
import BracketViolationsBanner from "../components/stats/BracketViolationsBanner";
import UpgradeSuggestionsList from "../components/stats/UpgradeSuggestionsList";
import CommanderRecommendationPanel from "../components/recommendation/CommanderRecommendationPanel";
import SeedBuilderPanel from "../components/seed/SeedBuilderPanel";
// Phase 4.14 Stage 3: GroupedDeckList + GroupableCard imports removed
// (the render call is gone; the source files stay BYTE-IDENTICAL per HARD #14).
import type { SwapSuggestion } from "../lib/applySwap";
// v1.3 Stage 1: diff-based AddedCardsPanel fallback.
import { computeDeckAdditions } from "../lib/deckDiff";
// v1.6.4 Stage 2: translation helper for Power Tune Swap Preview reason
// chips (line ~4990). v1.6.2 wired this at UpgradeSuggestionsList; the
// Tools-panel Power Tune sub-panel has a SEPARATE render seam that
// previously emitted raw engine codes (ADD_PRIMITIVE_COVERAGE etc.).
// justificationLabels.ts BYTE-IDENTICAL from v1.6.2.
import { translateJustification } from "../lib/justificationLabels";
import Badge from "../ui/primitives/Badge";
// v1.6 Stage 2: Button primitive for Action buttons (visually distinct from Mode tabs).
import Button from "../ui/primitives/Button";
import { saveDeck as _persistDeckViaAdapter } from "../lib/decks/savedDecks";
import {
  ACTIVE_DECK_STORAGE_KEY,
  buildActiveDeckPayload,
  buildBuildRequestBody,
  buildWorkspacePillText,
  callBuildEndpoint,
  extractCommanderRecommendation,
  extractSufficiencySummary,
  extractSwapSuggestions,
  normalizeActiveDeckSource,
  restoreFromActiveDeckSlot as _restoreFromActiveDeckSlot,
  shouldShowCommanderRecommendation,
  shouldShowSufficiencyDashboard,
  shouldShowSwapSuggestions,
} from "./workspaceIntegrationAdapters";
// Phase 4.13.2: pure reducer + types for workspace deck state.
import {
  deckReducer,
  INITIAL_STATE as INITIAL_DECK_STATE,
  type DeckSource,
  type UpgradeSwapSuggestion,
} from "../lib/workspaceDeckState";
import CanonicalSlotsPanel from "../components/CanonicalSlotsPanel";
import CardModal from "../components/CardModal";
import HoverCardPreview from "../components/cards/HoverCardPreview";
import DeckEditorPanel, { type DeckEditorCardHint } from "../components/deck/DeckEditorPanel";
import type { DeckPanelCard, DeckPanelCommander } from "../components/deck/DeckPanel";
import HeaderChips from "../components/HeaderChips";
// v1.6.1 hotfix Stage 2: LeftRail import + render hoisted to AppRouter.tsx
// so the hamburger persists across all 7 ViewId routes (was previously
// confined to the WorkspaceView mount).
import PrimitiveExplorerPanel from "../components/PrimitiveExplorerPanel";
import StatusBar from "../components/StatusBar";
import UnknownsPatchesPanel from "../components/UnknownsPatchesPanel";
import GlassPanel from "../ui/primitives/GlassPanel";
import type {
  BuildHistoryDeckDiff,
  BuildHistoryEntry,
  BuildRequestPayload,
  BuildResponsePayload,
  HoverCard,
} from "../components/workspaceTypes";
import {
  DEFAULT_API_BASE,
  asArray,
  asRecord,
  buildPrefetchCardImagesCommand,
  copyTextToClipboard,
  extractResolveNamesMissingNames,
  expandDecklistRowsInInputOrder,
  fetchLatestSnapshotIdFromApi,
  firstNumber,
  firstNonEmptyString,
  normalizeApiBase,
  parseDecklistInput,
  safeParseJson,
} from "../components/workspaceUtils";

const fixtureRoot = asRecord(fixtureBuildResult);
const defaultSnapshotId = firstNonEmptyString(fixtureRoot?.db_snapshot_id) || "";

// Phase 4.6: one-shot latency probe (Phase 4.4-vs-Phase-6A decision input).
// Captures the first /build fetch wall-clock; logs ONCE to console + sets a
// window-scoped flag for visual probes. Per autonomous_repair_log soft-safety
// #8: instrumentation impact is a single one-shot log only.
let _PHASE_4_6_BUILD_LATENCY_LOGGED = false;
function _phase46MarkBuildLatency(ms: number, source: "fresh" | "reused"): void {
  if (_PHASE_4_6_BUILD_LATENCY_LOGGED) return;
  _PHASE_4_6_BUILD_LATENCY_LOGGED = true;
  try {
    // eslint-disable-next-line no-console
    console.info(
      `[phase4.6 latency] /build first call: ${ms.toFixed(1)}ms (source=${source})`,
    );
    if (typeof window !== "undefined") {
      (window as unknown as { __phase46BuildLatencyMs?: number }).__phase46BuildLatencyMs = ms;
    }
  } catch {
    /* noop */
  }
}
const defaultProfileId = firstNonEmptyString(fixtureRoot?.profile_id) || "focused";
const defaultBracketId = firstNonEmptyString(fixtureRoot?.bracket_id) || "B2";
// Phase 4.13.2: defaultCommander removed — INITIAL_DECK_STATE in
// lib/workspaceDeckState owns the fallback commander/decklist constants.
const HOVER_PREFETCH_LRU_LIMIT = 200;
const DEFAULT_MAX_SWAPS = 5;
const DEFAULT_COMPLETE_TARGET_DECK_SIZE = 100;
const DEFAULT_COMPLETE_MAX_ADDS = 200;
const DEFAULT_COMPLETE_ALLOW_BASIC_LANDS = true;
const MAX_SWAPS_LIMIT = 50;
const MAX_COMPLETE_ADDS = 500;
const RESOLVE_NAMES_MAX_NAMES_PER_REQUEST = 200;
const DEV_SMOKE_TEST_TOAST_DURATION_MS = 5000;
const BASIC_LAND_NAME_KEYS = new Set<string>([
  "plains",
  "island",
  "swamp",
  "mountain",
  "forest",
  "wastes",
  "snow-covered plains",
  "snow-covered island",
  "snow-covered swamp",
  "snow-covered mountain",
  "snow-covered forest",
]);
const LOCAL_PATH_REDACTED = "<LOCAL_PATH_REDACTED>";
const WINDOWS_ABSOLUTE_PATH_RE = /^[a-zA-Z]:[\\/]/;
const UNC_ABSOLUTE_PATH_RE = /^\\\\[^\\]/;
const POSIX_ABSOLUTE_PATH_RE = /^\/(?!\/)/;
const HTTP_URL_RE = /^https?:\/\//i;
const DISALLOWED_NETWORK_WRAPPER_KEYS = ["endpoint_payload", "smoke_request_debug"] as const;

const MULLIGAN_MODEL_OPTIONS = ["NORMAL"] as const;
type MulliganModelId = (typeof MULLIGAN_MODEL_OPTIONS)[number];

const COMPLETE_LAND_MODE_OPTIONS = ["AUTO", "NONE"] as const;
type DeckCompleteLandMode = (typeof COMPLETE_LAND_MODE_OPTIONS)[number];

// Phase 4.14 Stage 1: ANALYZE mode removed (it was redundant with TOOLS;
// fired the same /deck/complete_v1 endpoint and confused users). EDIT +
// TOOLS remain.
type WorkspaceMode = "EDIT" | "TOOLS";
const WORKSPACE_MODE_STORAGE_KEY = "mtg_workspace_mode_v1";
const SAVED_DECKS_STORAGE_KEY = "mtg_saved_decks_v1";
const SELECTED_SAVED_DECK_STORAGE_KEY = "mtg_selected_saved_deck_v1";
const SAVED_DECKS_MAX = 50;
const WORKSPACE_MODE_OPTIONS: WorkspaceMode[] = ["EDIT", "TOOLS"];

type WorkspaceToolId = "DECK_TRIM" | "POWER_TUNE";

type SavedDeckEntry = {
  name: string;
  commander: string;
  deckText: string;
  updatedAtMs: number;
};

type SavedDeckDialogMode = "SAVE" | "RENAME" | "DELETE";

function normalizeWorkspaceMode(value: string | null | undefined): WorkspaceMode {
  const token = typeof value === "string" ? value.trim().toUpperCase() : "";
  if (token === "TOOLS") {
    return "TOOLS";
  }
  // Legacy "ANALYZE" stored in localStorage from prior versions falls through
  // to "EDIT" — the mode no longer exists per Phase 4.14 Stage 1.
  return "EDIT";
}

type NameOverrideV1 = {
  name_raw: string;
  resolved_oracle_id?: string;
  resolved_name?: string;
};

type ValidateUnknownCandidate = {
  oracle_id?: string;
  name?: string;
};

type ValidateUnknownRow = {
  name_raw?: string;
  reason_code?: string;
  candidates?: ValidateUnknownCandidate[];
};

type ValidateViolationRow = {
  code?: string;
  card_name?: string;
  count?: number;
  line_nos?: number[];
  message?: string;
};

type DeckValidateResponsePayload = {
  status?: string;
  canonical_deck_input?: Record<string, unknown>;
  unknowns?: ValidateUnknownRow[];
  violations_v1?: ValidateViolationRow[];
};

type DeckTuneSwapV1 = {
  cut_name?: string;
  add_name?: string;
  reasons_v1?: string[];
};

type DeckTuneResponseV1 = {
  status?: string;
  recommended_swaps_v1?: DeckTuneSwapV1[];
  unknowns?: ValidateUnknownRow[];
  violations_v1?: ValidateViolationRow[];
  baseline_summary_v1?: Record<string, unknown>;
};

type DeckCompleteAddedCardV1 = {
  name?: string;
  reasons_v1?: string[];
  primitives_added_v1?: string[];
};

type DeckCompleteResponseV1 = {
  status?: string;
  cards_added_count?: number;
  lands_added_count?: number;
  added_cards_v1?: DeckCompleteAddedCardV1[];
  completed_decklist_text_v1?: string;
  unknowns?: ValidateUnknownRow[];
  violations_v1?: ValidateViolationRow[];
  // Game-changer detection — flat list of card names from this deck that
  // appear on the engine's Game Changers userlist. Drives the GC badge
  // overlay on AddedCardRow / deck overview rows.
  game_changers_v1?: string[];
  detected_combos_v1?: unknown[];
  missing_partners_v1?: unknown[];
  deck_themes_v1?: unknown[];
};

type SnapshotPreflightErrorRow = {
  code?: string;
  message?: string;
};

type DeckTrimResultRow = {
  cardName: string;
  reasons: string[];
  primitives: string[];
};

type DeckTrimToolResult = {
  status: string;
  cards_to_cut: number;
  cut_order: string[];
  can_apply: boolean;
  message?: string;
  source?: "REUSED_BUILD" | "NEW_BUILD";
};

type DeckValidateRequestPayload = {
  db_snapshot_id: string;
  raw_decklist_text: string;
  format: "commander";
  profile_id: string;
  bracket_id: string;
  commander?: string;
  name_overrides_v1?: NameOverrideV1[];
};

type DeckTuneRequestPayload = {
  db_snapshot_id: string;
  raw_decklist_text: string;
  format: "commander";
  profile_id: string;
  bracket_id: string;
  mulligan_model_id: MulliganModelId;
  max_swaps: number;
  commander?: string;
  name_overrides_v1?: NameOverrideV1[];
};

type DeckCompleteRequestPayload = {
  db_snapshot_id: string;
  raw_decklist_text: string;
  format: "commander";
  profile_id: string;
  bracket_id: string;
  mulligan_model_id: MulliganModelId;
  target_deck_size: number;
  max_adds: number;
  allow_basic_lands: boolean;
  land_target_mode: DeckCompleteLandMode;
  commander?: string;
  name_overrides_v1?: NameOverrideV1[];
};

type PreflightSmartToolResult = {
  snapshotId: string;
  validateResponse: DeckValidateResponsePayload;
  canonicalCommander: string;
  canonicalCards: string[];
};

type ApiErrorDetails = {
  statusCode: number | null;
  endpoint: string;
  method: string;
  requestId: string | null;
  timestampIso: string;
  requestPayloadText: string;
  requestBodyText: string;
  requestDebugText: string;
  responseJsonText: string;
  stackTrace: string;
};

type ApiPingSummary = {
  status: "PENDING" | "OK" | "ERROR";
  endpoint: "/health" | "/version" | "-";
  engineVersion: string;
  dbSnapshotId: string;
  rulesetVersion: string;
  bracketDefinitionVersion: string;
};

const DEFAULT_API_PING_SUMMARY: ApiPingSummary = {
  status: "PENDING",
  endpoint: "-",
  engineVersion: "-",
  dbSnapshotId: "-",
  rulesetVersion: "-",
  bracketDefinitionVersion: "-",
};

type SmartToolHistoryOptions = {
  toolType?: string;
  inputDeckText?: string;
  outputDeckText?: string;
  inputCards?: string[];
  outputCards?: string[];
  summaryCounts?: Record<string, number>;
};

// Phase 4.13.2: getBuildResultCommander helper retired alongside the
// defaultCommander module-level binding it served (INITIAL_DECK_STATE
// in lib/workspaceDeckState now owns the fallback).

function buildCardsInputFromPayloadCards(cards: string[]): string {
  return cards.map((name: string) => `1 ${name}`).join("\n");
}

function buildTimestampLabel(now: Date): string {
  return `${now.toLocaleDateString()} ${now.toLocaleTimeString()}`;
}

function buildHoverPreviewImageUrl(apiBase: string, oracleIdRaw: string): string {
  const oracleId = oracleIdRaw.trim();
  if (oracleId === "") {
    return "";
  }
  return `${normalizeApiBase(apiBase)}/cards/image/${encodeURIComponent(oracleId)}?size=normal`;
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function normalizeSavedDeckName(value: string): string {
  return value
    .trim()
    .replace(/\s+/g, " ");
}

function normalizeSavedDeckEntry(value: unknown): SavedDeckEntry | null {
  const row = asRecord(value);
  if (!row) {
    return null;
  }

  const name = normalizeSavedDeckName(asString(row.name));
  const commander = asString(row.commander).trim();
  const deckText = asString(row.deckText);
  const updatedAtMsCandidate = firstNumber(row.updatedAtMs, row.updated_at_ms, row.updated_at);
  const updatedAtMs = updatedAtMsCandidate !== null ? Math.max(0, Math.trunc(updatedAtMsCandidate)) : 0;

  if (name === "" || commander === "" || deckText.trim() === "") {
    return null;
  }

  return {
    name,
    commander,
    deckText,
    updatedAtMs,
  };
}

function normalizeSavedDeckEntries(value: unknown): SavedDeckEntry[] {
  if (!Array.isArray(value)) {
    return [];
  }

  const dedupedByKey = new Map<string, SavedDeckEntry>();
  for (const entry of value) {
    const normalized = normalizeSavedDeckEntry(entry);
    if (!normalized) {
      continue;
    }

    const key = normalized.name.toLowerCase();
    const existing = dedupedByKey.get(key);
    if (!existing) {
      dedupedByKey.set(key, normalized);
      continue;
    }

    if (normalized.updatedAtMs > existing.updatedAtMs) {
      dedupedByKey.set(key, normalized);
      continue;
    }

    if (normalized.updatedAtMs === existing.updatedAtMs && normalized.name.localeCompare(existing.name) < 0) {
      dedupedByKey.set(key, normalized);
    }
  }

  return [...dedupedByKey.values()]
    .sort((left: SavedDeckEntry, right: SavedDeckEntry) => {
      if (left.updatedAtMs !== right.updatedAtMs) {
        return right.updatedAtMs - left.updatedAtMs;
      }
      return left.name.localeCompare(right.name);
    })
    .slice(0, SAVED_DECKS_MAX);
}

function countNonEmptyTextLines(text: string): number {
  return text
    .split(/\r?\n/)
    .map((line: string) => line.trim())
    .filter((line: string) => line !== "").length;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown): entry is string => typeof entry === "string");
}

function asValidateUnknownRows(value: unknown): ValidateUnknownRow[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown) => Boolean(asRecord(entry))) as ValidateUnknownRow[];
}

function asValidateViolationRows(value: unknown): ValidateViolationRow[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown) => Boolean(asRecord(entry))) as ValidateViolationRow[];
}

function asDeckTuneSwapRows(value: unknown): DeckTuneSwapV1[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown) => Boolean(asRecord(entry))) as DeckTuneSwapV1[];
}

function asDeckCompleteAddedRows(value: unknown): DeckCompleteAddedCardV1[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown) => Boolean(asRecord(entry))) as DeckCompleteAddedCardV1[];
}

function asSnapshotPreflightErrorRows(value: unknown): SnapshotPreflightErrorRow[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown) => Boolean(asRecord(entry))) as SnapshotPreflightErrorRow[];
}

function collectSnapshotPreflightFixInstructions(errors: SnapshotPreflightErrorRow[]): string[] {
  const codeSet = new Set<string>();
  for (const row of errors) {
    const code = asString(row.code).trim().toUpperCase();
    if (code !== "") {
      codeSet.add(code);
    }
  }

  const instructions: string[] = [];
  if (codeSet.has("SNAPSHOT_TAGS_NOT_COMPILED")) {
    instructions.push("If SNAPSHOT_TAGS_NOT_COMPILED: run snapshot_build.tag_snapshot then snapshot_build.index_build.");
  }
  if (codeSet.has("CARD_IMAGES_SCHEMA_INVALID")) {
    instructions.push("If CARD_IMAGES_SCHEMA_INVALID: run migration script snapshot_build.migrate_card_images_table.");
  }

  return instructions;
}

function isLocalAbsolutePath(value: string): boolean {
  const token = value.trim();
  if (token === "" || HTTP_URL_RE.test(token)) {
    return false;
  }
  return WINDOWS_ABSOLUTE_PATH_RE.test(token) || UNC_ABSOLUTE_PATH_RE.test(token) || POSIX_ABSOLUTE_PATH_RE.test(token);
}

function redactLocalPathsForError(value: unknown, seen = new WeakSet<object>()): unknown {
  if (typeof value === "string") {
    return isLocalAbsolutePath(value) ? LOCAL_PATH_REDACTED : value;
  }
  if (Array.isArray(value)) {
    return value.map((entry: unknown) => redactLocalPathsForError(entry, seen));
  }

  const row = asRecord(value);
  if (!row) {
    return value;
  }
  if (seen.has(row)) {
    return "[Circular]";
  }
  seen.add(row);

  const out: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(row)) {
    out[key] = redactLocalPathsForError(entry, seen);
  }
  return out;
}

function toErrorPayloadText(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  const redacted = redactLocalPathsForError(value);
  try {
    return JSON.stringify(redacted, null, 2);
  } catch {
    return String(redacted);
  }
}

function toErrorRequestBodyText(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  return toErrorPayloadText(value);
}

function findDisallowedNetworkWrapperKey(value: unknown): (typeof DISALLOWED_NETWORK_WRAPPER_KEYS)[number] | null {
  const row = asRecord(value);
  if (!row) {
    return null;
  }

  for (const key of DISALLOWED_NETWORK_WRAPPER_KEYS) {
    if (Object.prototype.hasOwnProperty.call(row, key)) {
      return key;
    }
  }

  return null;
}

function serializeRequestBodyForNetwork(endpoint: string, payload: unknown): string {
  const disallowedKey = findDisallowedNetworkWrapperKey(payload);
  if (disallowedKey) {
    throw new Error(
      `Refusing to send wrapped payload to ${endpoint}: unexpected key "${disallowedKey}". Send the flat endpoint request model.`,
    );
  }
  return JSON.stringify(payload);
}

function toErrorStackTrace(error: unknown): string {
  if (error instanceof Error) {
    return error.stack || error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  if (error === null || error === undefined) {
    return "";
  }
  try {
    return JSON.stringify(error, null, 2);
  } catch {
    return String(error);
  }
}

function readRequestIdFromPayload(payload: unknown): string | null {
  const root = asRecord(payload);
  if (!root) {
    return null;
  }

  const detail = asRecord(root.detail);
  return (
    firstNonEmptyString(
      root.request_id,
      root.requestId,
      root.trace_id,
      root.traceId,
      detail?.request_id,
      detail?.requestId,
      detail?.trace_id,
      detail?.traceId,
    ) || null
  );
}

function buildApiErrorDetails(
  endpoint: string,
  response: Response | null,
  responseText: string,
  responsePayload: unknown,
  options?: {
    method?: string;
    requestPayload?: unknown;
    requestDebug?: unknown;
    error?: unknown;
  },
): ApiErrorDetails {
  const headerRequestId = response
    ? firstNonEmptyString(
        response.headers.get("x-request-id"),
        response.headers.get("request-id"),
        response.headers.get("x-correlation-id"),
        response.headers.get("x-trace-id"),
      )
    : null;
  const payloadRequestId = readRequestIdFromPayload(responsePayload);
  const requestId = firstNonEmptyString(headerRequestId, payloadRequestId) || null;

  let responseJsonText = responseText;
  if (responseJsonText.trim() === "") {
    responseJsonText = toErrorPayloadText(responsePayload);
  }
  const requestBodyText = toErrorRequestBodyText(options?.requestPayload);
  const requestDebugText = toErrorPayloadText(options?.requestDebug);

  return {
    statusCode: response ? response.status : null,
    endpoint,
    method: (options?.method || "POST").trim().toUpperCase() || "POST",
    requestId,
    timestampIso: new Date().toISOString(),
    requestPayloadText: requestBodyText,
    requestBodyText,
    requestDebugText,
    responseJsonText,
    stackTrace: toErrorStackTrace(options?.error),
  };
}

function buildSyntheticApiErrorDetails(
  endpoint: string,
  statusCode: number,
  responsePayload: unknown,
  options?: {
    method?: string;
    requestPayload?: unknown;
    requestDebug?: unknown;
    error?: unknown;
  },
): ApiErrorDetails {
  const responseJsonText = toErrorPayloadText(responsePayload);
  const requestBodyText = toErrorRequestBodyText(options?.requestPayload);
  const requestDebugText = toErrorPayloadText(options?.requestDebug);

  return {
    statusCode,
    endpoint,
    method: (options?.method || "POST").trim().toUpperCase() || "POST",
    requestId: readRequestIdFromPayload(responsePayload),
    timestampIso: new Date().toISOString(),
    requestPayloadText: requestBodyText,
    requestBodyText,
    requestDebugText,
    responseJsonText,
    stackTrace: toErrorStackTrace(options?.error),
  };
}

function formatApiErrorMessage(details: ApiErrorDetails): string {
  const statusToken = details.statusCode === null ? "network" : String(details.statusCode);
  const payloadJson = details.responseJsonText.trim() === "" ? "(empty)" : details.responseJsonText;
  return `${details.endpoint} failed (status: ${statusToken})\n${payloadJson}`;
}

function clampInteger(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) {
    return min;
  }
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return Math.trunc(value);
}

function normalizeDeckTextForHash(value: string): string {
  return value
    .split(/\r?\n/)
    .map((line: string) => line.trim())
    .filter((line: string) => line !== "")
    .join("\n");
}

function hashFnv1aHex(value: string): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

function buildDeckHashV1(commanderName: string, decklistText: string): string {
  const commanderToken = commanderName.trim().toLowerCase();
  const deckToken = normalizeDeckTextForHash(decklistText).toLowerCase();
  return `deck_hash_v1_${hashFnv1aHex(`${commanderToken}::${deckToken}`)}`;
}

function buildDeckHashFromCards(commanderName: string, cards: string[]): string {
  return buildDeckHashV1(commanderName, collapseCardNamesInInputOrder(cards));
}

function buildCardCountMap(cards: string[]): Map<string, { name: string; count: number }> {
  const counters = new Map<string, { name: string; count: number }>();
  for (const rawName of cards) {
    const name = rawName.trim();
    if (name === "") {
      continue;
    }
    const key = normalizeNameToken(name);
    const existing = counters.get(key);
    if (!existing) {
      counters.set(key, {
        name,
        count: 1,
      });
      continue;
    }
    existing.count += 1;
  }
  return counters;
}

function flattenCardUnits(entries: Array<{ name: string; count: number }>): string[] {
  const out: string[] = [];
  for (const entry of entries) {
    const boundedCount = Math.max(0, Math.trunc(entry.count));
    for (let idx = 0; idx < boundedCount; idx += 1) {
      out.push(entry.name);
    }
  }
  return out;
}

function summarizeCardUnits(units: string[]): string[] {
  const counters = new Map<string, { name: string; count: number }>();
  for (const rawName of units) {
    const name = rawName.trim();
    if (name === "") {
      continue;
    }
    const key = normalizeNameToken(name);
    const existing = counters.get(key);
    if (!existing) {
      counters.set(key, {
        name,
        count: 1,
      });
      continue;
    }
    existing.count += 1;
  }

  return Array.from(counters.values())
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((row) => `${row.count} ${row.name}`);
}

function buildDeckDiff(inputCards: string[], outputCards: string[]): BuildHistoryDeckDiff {
  const inputCounts = buildCardCountMap(inputCards);
  const outputCounts = buildCardCountMap(outputCards);
  const allKeys = Array.from(new Set<string>([...inputCounts.keys(), ...outputCounts.keys()])).sort((a, b) =>
    a.localeCompare(b),
  );

  const addedRows: Array<{ name: string; count: number }> = [];
  const removedRows: Array<{ name: string; count: number }> = [];
  for (const key of allKeys) {
    const input = inputCounts.get(key);
    const output = outputCounts.get(key);
    const delta = (output?.count || 0) - (input?.count || 0);
    if (delta > 0) {
      addedRows.push({
        name: output?.name || input?.name || key,
        count: delta,
      });
    }
    if (delta < 0) {
      removedRows.push({
        name: input?.name || output?.name || key,
        count: -delta,
      });
    }
  }

  const addedUnits = flattenCardUnits(addedRows);
  const removedUnits = flattenCardUnits(removedRows);
  const swappedCount = Math.min(addedUnits.length, removedUnits.length);
  const swapped: string[] = [];
  for (let idx = 0; idx < swappedCount; idx += 1) {
    swapped.push(`${removedUnits[idx]} -> ${addedUnits[idx]}`);
  }

  return {
    swapped,
    added: summarizeCardUnits(addedUnits.slice(swappedCount)),
    removed: summarizeCardUnits(removedUnits.slice(swappedCount)),
  };
}

function normalizeNameToken(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function isBasicLandName(value: string): boolean {
  return BASIC_LAND_NAME_KEYS.has(normalizeNameToken(value));
}

function chunkStringsInOrder(values: string[], chunkSize: number): string[][] {
  if (values.length === 0) {
    return [];
  }

  const orderedDeduped: string[] = [];
  const seen = new Set<string>();
  for (const rawValue of values) {
    const value = rawValue.trim();
    const key = normalizeNameToken(value);
    if (key === "" || seen.has(key)) {
      continue;
    }
    seen.add(key);
    orderedDeduped.push(value);
  }
  if (orderedDeduped.length === 0) {
    return [];
  }

  const safeChunkSize = Math.max(1, Math.trunc(chunkSize));
  const chunks: string[][] = [];
  for (let index = 0; index < orderedDeduped.length; index += safeChunkSize) {
    chunks.push(orderedDeduped.slice(index, index + safeChunkSize));
  }
  return chunks;
}

function sanitizeNameOverride(value: NameOverrideV1): NameOverrideV1 | null {
  const nameRaw = value.name_raw.trim().replace(/\s+/g, " ");
  if (nameRaw === "") {
    return null;
  }

  const oracleId = asString(value.resolved_oracle_id).trim();
  const resolvedName = asString(value.resolved_name).trim().replace(/\s+/g, " ");
  if (oracleId !== "") {
    return {
      name_raw: nameRaw,
      resolved_oracle_id: oracleId,
    };
  }
  if (resolvedName !== "") {
    return {
      name_raw: nameRaw,
      resolved_name: resolvedName,
    };
  }

  return null;
}

function normalizeNameOverrides(overrides: NameOverrideV1[]): NameOverrideV1[] {
  const out: NameOverrideV1[] = [];
  const seen = new Set<string>();
  for (const rawOverride of overrides) {
    const override = sanitizeNameOverride(rawOverride);
    if (!override) {
      continue;
    }

    const key = normalizeNameToken(override.name_raw);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(override);
  }
  return out;
}

function applySwapsDeterministically(cards: string[], swaps: DeckTuneSwapV1[], applyCount: number): string[] {
  const out = cards.slice();
  const boundedApplyCount = clampInteger(applyCount, 0, swaps.length);

  for (let idx = 0; idx < boundedApplyCount; idx += 1) {
    const swap = swaps[idx] ?? {};
    const cutName = asString(swap.cut_name).trim();
    const addName = asString(swap.add_name).trim();
    if (cutName === "" || addName === "") {
      continue;
    }

    const cutIndex = out.findIndex((name: string) => name === cutName);
    if (cutIndex >= 0) {
      out[cutIndex] = addName;
    }
  }

  return out;
}

function collapseCardNamesInInputOrder(cards: string[]): string {
  const counters = new Map<string, { name: string; count: number; firstIndex: number }>();

  cards.forEach((rawName: string, index: number) => {
    const name = rawName.trim();
    if (name === "") {
      return;
    }
    const key = normalizeNameToken(name);
    const existing = counters.get(key);
    if (!existing) {
      counters.set(key, {
        name,
        count: 1,
        firstIndex: index,
      });
      return;
    }
    existing.count += 1;
  });

  return Array.from(counters.values())
    .sort((left, right) => left.firstIndex - right.firstIndex)
    .map((row) => `${row.count} ${row.name}`)
    .join("\n");
}

function buildCommanderDecklistTextForApi(commander: string, cardsInput: string): string {
  const commanderName = commander.trim();
  if (commanderName === "") {
    return cardsInput;
  }

  const hasSectionHeaders = /^\s*commander\s*$/im.test(cardsInput) && /^\s*deck\s*$/im.test(cardsInput);
  if (hasSectionHeaders) {
    return cardsInput;
  }

  const deckLines = cardsInput
    .split(/\r?\n/)
    .map((line: string) => line.trim())
    .filter((line: string) => line !== "");

  return ["Commander", `1 ${commanderName}`, "Deck", ...deckLines].join("\n");
}

function parseCompletedDecklistText(decklistText: string): { commander: string | null; cards: string[] } {
  const lines = decklistText
    .split(/\r?\n/)
    .map((line: string) => line.trim())
    .filter((line: string) => line !== "");

  let section: "NONE" | "COMMANDER" | "DECK" = "NONE";
  let commander: string | null = null;
  const cards: string[] = [];

  for (const line of lines) {
    const lowered = line.toLowerCase();
    if (lowered === "commander") {
      section = "COMMANDER";
      continue;
    }
    if (lowered === "deck") {
      section = "DECK";
      continue;
    }

    const countMatch = line.match(/^(\d+)\s+(.+)$/);
    if (!countMatch) {
      continue;
    }

    const count = clampInteger(Number(countMatch[1]), 1, 250);
    const name = countMatch[2]?.trim() || "";
    if (name === "") {
      continue;
    }

    if (section === "COMMANDER" && commander === null) {
      commander = name;
      continue;
    }

    for (let i = 0; i < count; i += 1) {
      cards.push(name);
    }
  }

  return {
    commander,
    cards,
  };
}

function isLikelyLandAddition(row: DeckCompleteAddedCardV1): boolean {
  const cardName = asString(row.name);
  if (cardName === "") {
    return false;
  }

  const reasons = new Set(asStringArray(row.reasons_v1));
  if (reasons.has("ADD_BASIC_LAND_FILL_AUTO")) {
    return true;
  }
  if (["Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"].includes(cardName)) {
    return true;
  }
  return cardName.startsWith("Snow-Covered ");
}

function asStringList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return asStringArray(value);
  }
  const token = asString(value).trim();
  if (token === "") {
    return [];
  }
  return [token];
}

function toSortedUniqueStrings(values: string[]): string[] {
  const deduped = new Set<string>();
  for (const rawValue of values) {
    const token = rawValue.trim();
    if (token !== "") {
      deduped.add(token);
    }
  }
  return Array.from(deduped).sort((left: string, right: string) => left.localeCompare(right));
}

function buildDeckTrimRows(cutOrder: string[], buildResponse: BuildResponsePayload | null): DeckTrimResultRow[] {
  if (cutOrder.length === 0) {
    return [];
  }

  const result = asRecord(buildResponse?.result);
  const canonicalRows = asArray(result?.deck_cards_canonical_input_order)
    .map((entry: unknown) => asRecord(entry))
    .filter((entry: Record<string, unknown> | null): entry is Record<string, unknown> => Boolean(entry));
  const primitiveIndexBySlot = asRecord(result?.primitive_index_by_slot);

  const reasonRows = asRecord(
    result?.cut_order_reasons_v1 ??
      result?.cut_order_reasons_by_name ??
      result?.cut_reasons_by_name ??
      result?.cut_reasons_v1 ??
      result?.cut_order_reasons ??
      result?.cut_reasons,
  );
  const reasonsByNameKey = new Map<string, string[]>();
  if (reasonRows) {
    for (const [name, rawReasons] of Object.entries(reasonRows)) {
      const key = normalizeNameToken(name);
      if (key === "") {
        continue;
      }
      reasonsByNameKey.set(key, toSortedUniqueStrings(asStringList(rawReasons)));
    }
  }

  const metadataByNameKey = new Map<string, Array<{ reasons: string[]; primitives: string[] }>>();
  for (const row of canonicalRows) {
    const cardName = firstNonEmptyString(row.resolved_name, row.name, row.input, row.card_name, row.slot_name);
    const key = normalizeNameToken(cardName || "");
    if (key === "") {
      continue;
    }

    const slotId = asString(row.slot_id).trim();
    const primitives =
      slotId !== "" && primitiveIndexBySlot
        ? toSortedUniqueStrings(asStringList(primitiveIndexBySlot[slotId]))
        : [];
    const reasons = toSortedUniqueStrings([
      ...asStringList(row.reasons_v1),
      ...asStringList(row.reason_codes),
      ...asStringList(row.cut_reasons_v1),
      ...asStringList(row.cut_reasons),
    ]);

    const bucket = metadataByNameKey.get(key);
    const metadata = { reasons, primitives };
    if (bucket) {
      bucket.push(metadata);
    } else {
      metadataByNameKey.set(key, [metadata]);
    }
  }

  return cutOrder.map((cardName: string) => {
    const key = normalizeNameToken(cardName);
    const bucket = metadataByNameKey.get(key);
    const metadata = bucket && bucket.length > 0 ? bucket.shift() || null : null;
    const reasons = toSortedUniqueStrings([...(metadata?.reasons || []), ...(reasonsByNameKey.get(key) || [])]);

    return {
      cardName,
      reasons,
      primitives: metadata?.primitives || [],
    };
  });
}

function applyCutOrderToCardsInput(
  parsedRows: ReturnType<typeof parseDecklistInput>,
  cutOrder: string[],
  commanderName: string,
): string {
  const rowsByKey = new Map<string, { key: string; name: string; count: number; sourceOrder: number }>();

  for (const parsedRow of parsedRows) {
    const key = normalizeNameToken(parsedRow.name);
    if (key === "") {
      continue;
    }

    const safeCount = Number.isFinite(parsedRow.count) ? Math.max(1, Math.trunc(parsedRow.count)) : 1;
    const existing = rowsByKey.get(key);
    if (!existing) {
      rowsByKey.set(key, {
        key,
        name: parsedRow.name.trim(),
        count: safeCount,
        sourceOrder: parsedRow.source_order,
      });
      continue;
    }

    existing.count += safeCount;
    if (parsedRow.source_order < existing.sourceOrder) {
      existing.sourceOrder = parsedRow.source_order;
      existing.name = parsedRow.name.trim();
    }
  }

  const commanderKey = normalizeNameToken(commanderName);
  for (const cutName of cutOrder) {
    const cutKey = normalizeNameToken(cutName);
    if (cutKey === "" || cutKey === commanderKey) {
      continue;
    }

    const row = rowsByKey.get(cutKey);
    if (!row) {
      continue;
    }

    row.count -= 1;
    if (row.count <= 0) {
      rowsByKey.delete(cutKey);
    }
  }

  return Array.from(rowsByKey.values())
    .sort((left, right) => {
      if (left.sourceOrder !== right.sourceOrder) {
        return left.sourceOrder - right.sourceOrder;
      }
      return left.name.localeCompare(right.name);
    })
    .map((row) => `${row.count} ${row.name}`)
    .join("\n");
}

function buildDeckEditorCardHints(deckCards: DeckPanelCard[]): Record<string, DeckEditorCardHint> {
  const hintsByName: Record<string, DeckEditorCardHint> = {};

  for (const card of deckCards) {
    const key = card.name.trim().toLowerCase();
    if (key === "") {
      continue;
    }

    const existing = hintsByName[key];
    const oracleId = firstNonEmptyString(card.oracleId)?.trim() || "";
    const typeLine = firstNonEmptyString(card.typeLine);

    hintsByName[key] = {
      oracleId: existing?.oracleId || oracleId,
      typeLine: existing?.typeLine || typeLine || null,
    };
  }

  return hintsByName;
}

function normalizeDeckPanelCard(raw: unknown): DeckPanelCard | null {
  const row = asRecord(raw);
  if (row) {
    const name = firstNonEmptyString(row.resolved_name, row.name, row.input, row.card_name, row.slot_name, row.slot_id);
    if (name === null) {
      return null;
    }

    return {
      name,
      oracleId: firstNonEmptyString(row.resolved_oracle_id, row.oracle_id, row.card_oracle_id),
      typeLine: firstNonEmptyString(row.type_line, row.resolved_type_line, row.card_type_line),
      cmc: firstNumber(row.cmc, row.resolved_cmc, row.card_cmc, row.mana_value),
    };
  }

  const name = firstNonEmptyString(raw);
  if (name === null) {
    return null;
  }
  return { name };
}

function resolveDeckPanelCommander(buildResponse: BuildResponsePayload | null): DeckPanelCommander | null {
  const result = asRecord(buildResponse?.result);
  if (!result) {
    return null;
  }

  if (firstNonEmptyString(result.format) !== "commander") {
    return null;
  }

  const commanderResolved = asRecord(result.commander_resolved);
  const commanderCanonicalSlot = asRecord(result.commander_canonical_slot);

  const name = firstNonEmptyString(
    result.commander,
    commanderResolved?.name,
    commanderCanonicalSlot?.resolved_name,
    commanderCanonicalSlot?.input,
  );
  if (name === null) {
    return null;
  }

  return {
    name,
    oracleId: firstNonEmptyString(
      commanderResolved?.oracle_id,
      commanderResolved?.resolved_oracle_id,
      commanderCanonicalSlot?.resolved_oracle_id,
      commanderCanonicalSlot?.oracle_id,
    ),
  };
}

function resolveDeckPanelCards(buildResponse: BuildResponsePayload | null, commander: DeckPanelCommander | null): DeckPanelCard[] {
  const result = asRecord(buildResponse?.result);
  if (!result) {
    return [];
  }

  const commanderOracleId = (commander?.oracleId || "").trim();
  const commanderNameKey = (commander?.name || "").trim().toLowerCase();

  const playableRows = asArray(result.deck_cards_playable);
  const nonplayableRows = asArray(result.deck_cards_nonplayable);

  const candidateSources: unknown[][] = [];
  if (playableRows.length + nonplayableRows.length > 0) {
    candidateSources.push(playableRows, nonplayableRows);
  } else {
    candidateSources.push(
      asArray(result.cards_resolved),
      asArray(result.canonical_slots_all),
      asArray(result.deck_cards_canonical_input_order),
    );
  }

  for (const sourceRows of candidateSources) {
    const normalizedRows: DeckPanelCard[] = [];

    for (const rawRow of sourceRows) {
      const card = normalizeDeckPanelCard(rawRow);
      if (!card) {
        continue;
      }

      const oracleId = (card.oracleId || "").trim();
      const cardNameKey = card.name.trim().toLowerCase();

      if (commanderOracleId !== "" && oracleId !== "" && oracleId === commanderOracleId) {
        continue;
      }
      if (commanderNameKey !== "" && cardNameKey === commanderNameKey) {
        continue;
      }

      normalizedRows.push(card);
    }

    if (normalizedRows.length > 0) {
      return normalizedRows;
    }
  }

  return [];
}

function isExternalBackendWarningMode(searchValue: string): boolean {
  const params = new URLSearchParams(searchValue);
  return params.get("external_backend") === "1";
}

export default function WorkspaceView() {
  const showExternalBackendBanner = useMemo(() => isExternalBackendWarningMode(window.location.search), []);
  const [showExternalBackendHelp, setShowExternalBackendHelp] = useState(false);
  const [workspaceMode, setWorkspaceMode] = useState<WorkspaceMode>(() => {
    try {
      return normalizeWorkspaceMode(window.localStorage.getItem(WORKSPACE_MODE_STORAGE_KEY));
    } catch {
      return "EDIT";
    }
  });

  const [apiBase] = useState(DEFAULT_API_BASE);
  const [snapshotId, setSnapshotId] = useState(defaultSnapshotId);
  const [profileId, setProfileId] = useState(defaultProfileId);
  const [bracketId, setBracketId] = useState(defaultBracketId);
  // v1.7.6 — bracket selector popover open/close state. Owned by
  // WorkspaceView (not a child component) so the same trigger button can
  // both display the current bracket pill AND toggle the menu without an
  // extra ref/imperative handle.
  const [bracketSelectorOpen, setBracketSelectorOpen] = useState(false);
  // Phase 4.13.2 architectural refactor: replace useState + useRef +
  // two-racing-useEffects with a useReducer-backed pure state machine.
  // The reducer (lib/workspaceDeckState) carries `source` as a first-class
  // field + an `isHydrated` flag that gates persistence so default-state
  // writes never leak into mtgdb:workspace:active_deck_v1. See the
  // hydration + persistence useEffects further below.
  const [deckState, dispatchDeckAction] = useReducer(deckReducer, INITIAL_DECK_STATE);
  const commander = deckState.commander;
  const deckText = deckState.deckText;
  const deckTextRevision = deckState.deckTextRevision;
  const buildResponse = deckState.buildResponse as BuildResponsePayload | null;

  // Compatibility wrappers — DeckInputPanel adapter prop signature SHA
  // 18ecdac40880... BYTE-IDENTICAL per HARD #7 (the prop type is `(value:
  // string) => void`; these wrappers keep that contract).
  const setCommander = (value: string) => dispatchDeckAction({ type: "USER_EDIT_COMMANDER", commander: value });
  const setDeckText = (value: string) => dispatchDeckAction({ type: "USER_EDIT_DECK_TEXT", deckText: value });
  // The reducer auto-bumps deckTextRevision on USER_EDIT_DECK_TEXT /
  // HYDRATE_* / LOAD_SAVED_DECK — this wrapper is a no-op for backward
  // compat with existing call sites that paired setDeckText + this bumper
  // (e.g. `applyDeckText`).
  const setDeckTextRevision = (_updater: ((rev: number) => number) | number): void => {
    /* reducer auto-bumps; intentional no-op */
  };
  const setBuildResponse = (value: BuildResponsePayload | null): void => {
    if (value === null) {
      dispatchDeckAction({ type: "CLEAR_BUILD_RESPONSE" });
    } else {
      dispatchDeckAction({ type: "BUILD_SUCCESS", response: value as unknown as Record<string, unknown> });
    }
  };

  // Phase 4.13.2 single hydration useEffect with explicit precedence:
  // IMPORT (one-shot consume + clear; 4.6 contract preserved) > active-deck
  // slot (sticky; not cleared; Goldfish still consumes it; 4.13.1 contract
  // preserved) > HYDRATE_NO_SOURCE (preserves fallback values, marks
  // hydrated so persistence guard activates on first user edit).
  useEffect(() => {
    const staged = _readStagedImport();
    if (staged) {
      const cmdr = staged.commander && staged.commander !== "" ? staged.commander : INITIAL_DECK_STATE.commander;
      const list = staged.decklist && staged.decklist !== "" ? staged.decklist : INITIAL_DECK_STATE.deckText;
      const source = normalizeActiveDeckSource(staged.source) as DeckSource;
      dispatchDeckAction({ type: "HYDRATE_FROM_IMPORT_SLOT", commander: cmdr, decklist: list, source });
      _clearStagedImport();
      // eslint-disable-next-line no-console
      console.info(
        `[phase4.13.2 hydrate] IMPORT slot consumed: source=${staged.source} unknowns=${staged.unknowns.length}`,
      );
      return;
    }
    const restored = _restoreFromActiveDeckSlot();
    if (restored) {
      dispatchDeckAction({
        type: "HYDRATE_FROM_ACTIVE_SLOT",
        commander: restored.commander,
        decklist: restored.decklist,
        source: restored.source as DeckSource,
      });
      // eslint-disable-next-line no-console
      console.info(`[phase4.13.2 hydrate] active-deck slot restored: source=${restored.source}`);
      return;
    }
    dispatchDeckAction({ type: "HYDRATE_NO_SOURCE" });
  }, []);
  const [savedDecks, setSavedDecks] = useState<SavedDeckEntry[]>(() => {
    if (typeof window === "undefined") {
      return [];
    }

    try {
      const stored = window.localStorage.getItem(SAVED_DECKS_STORAGE_KEY);
      if (!stored) {
        return [];
      }

      const parsed = safeParseJson(stored);
      return normalizeSavedDeckEntries(parsed);
    } catch {
      return [];
    }
  });
  const [selectedSavedDeckName, setSelectedSavedDeckName] = useState(() => {
    if (typeof window === "undefined") {
      return "";
    }

    try {
      return normalizeSavedDeckName(window.localStorage.getItem(SELECTED_SAVED_DECK_STORAGE_KEY) || "");
    } catch {
      return "";
    }
  });

  const [, setValidationMessage] = useState<string | null>(null);
  const [runningSmartTrim, setRunningSmartTrim] = useState(false);
  const [runningSmartTune, setRunningSmartTune] = useState(false);
  const [runningSmartComplete, setRunningSmartComplete] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [apiErrorDetails, setApiErrorDetails] = useState<ApiErrorDetails | null>(null);
  const [isApiReachable, setIsApiReachable] = useState(false);
  const [lastValidatePassed, setLastValidatePassed] = useState(false);
  const [lastSmokeSucceeded, setLastSmokeSucceeded] = useState(false);
  const [lastTuneSucceeded, setLastTuneSucceeded] = useState(false);
  const [lastTrimSucceeded, setLastTrimSucceeded] = useState(false);

  // Phase 4.13.2: buildResponse + setBuildResponse moved earlier as
  // reducer-derived bindings + compatibility wrapper. Only requestPayload
  // remains here.
  const [requestPayload, setRequestPayload] = useState<BuildRequestPayload | null>(null);
  // Phase 4 BUNDLE Integration (4.13): parent page mode toggle. EDIT/TOOLS/
  // ANALYZE remain children of WORKSPACE per autonomous_repair_log #8.
  const [pageMode, setPageMode] = useState<"WORKSPACE" | "SEED_BUILDER">("WORKSPACE");
  // Right-rail combos drawer open state. Toggled by the workspace toolbar
  // "Combos" button; closed by default so the surface stays opt-in.
  const [combosDrawerOpen, setCombosDrawerOpen] = useState<boolean>(false);

  // Phase 4.13.2: Build button pending/error state derive from the reducer.
  // The Build click handler dispatches BUILD_PENDING → fetch → BUILD_SUCCESS
  // / BUILD_ERROR. No separate useState needed.
  const runningBuild = deckState.buildPending;
  const buildError = deckState.buildError;

  const [nameOverridesV1] = useState<NameOverrideV1[]>([]);
  const [, setSmartToolValidateResponse] = useState<DeckValidateResponsePayload | null>(null);
  const [, setSmartToolBlockMessage] = useState<string | null>(null);
  const [, setSmartToolBlockUnknowns] = useState<ValidateUnknownRow[]>([]);
  const [, setSmartToolBlockViolations] = useState<ValidateViolationRow[]>([]);

  const [mulliganModelId] = useState<MulliganModelId>(MULLIGAN_MODEL_OPTIONS[0]);
  const [maxSwaps, setMaxSwaps] = useState(DEFAULT_MAX_SWAPS);
  const [completeMaxAdds] = useState(DEFAULT_COMPLETE_MAX_ADDS);
  const [completeAllowBasicLands] = useState(DEFAULT_COMPLETE_ALLOW_BASIC_LANDS);
  const [completeLandTargetMode] = useState<DeckCompleteLandMode>("AUTO");

  const [deckTuneResponse, setDeckTuneResponse] = useState<DeckTuneResponseV1 | null>(null);
  const [completionResult, setCompletionResult] = useState<DeckCompleteResponseV1 | null>(null);
  // v1.3 Stage 1 + v1.4 Stage 1 micro-fix: snapshot of deckText taken at
  // the moment the user clicks "1. Complete deck", BEFORE any state
  // updates or dispatches fire. v1.3 used useState here but the setter
  // is async — by the time the diff useMemo ran the snapshot could have
  // already been overwritten by handleApplyCompletedDecklist's
  // USER_EDIT_DECK_TEXT dispatch (which sets deckText to the completed
  // text). v1.4 switches to useRef: synchronous read at click entry +
  // zero render-cycle risk. The ref is read inside derivedAddedRows
  // useMemo; the memo's `completedDecklistText` dep triggers recompute
  // when the response arrives.
  const preCompleteDeckTextRef = useRef<string>("");
  // v1.3 Stage 2: snapshot of last non-empty upgrade suggestions. The
  // reducer clears upgradeSuggestions on USER_EDIT_DECK_TEXT (deck
  // mutation invalidates), but Apply-row click DISPATCHES
  // USER_EDIT_DECK_TEXT — without this snapshot the panel unmounts
  // immediately after click and the "Applied ✓" badge + counter
  // never become visible (the v1.2 Stage 2 close-out false-pass).
  // The snapshot persists until the user clicks × Clear OR a new
  // Upgrade run replaces it via the useEffect below.
  const [upgradeSnapshotRows, setUpgradeSnapshotRows] = useState<ReadonlyArray<UpgradeSwapSuggestion> | null>(null);
  const [completionError, setCompletionError] = useState<string | null>(null);
  const [deckTrimResult, setDeckTrimResult] = useState<DeckTrimToolResult | null>(null);
  const [tuneSourceCards, setTuneSourceCards] = useState<string[]>([]);
  const [tuneSourceCommander, setTuneSourceCommander] = useState("");
  const [activeTool, setActiveTool] = useState<WorkspaceToolId>("DECK_TRIM");

  const [pendingCutOrder, setPendingCutOrder] = useState<string[]>([]);
  const [isCompletionBlockedModalOpen, setIsCompletionBlockedModalOpen] = useState(false);
  const [completionBlockedUnknowns, setCompletionBlockedUnknowns] = useState<ValidateUnknownRow[]>([]);
  const [completionBlockedViolations, setCompletionBlockedViolations] = useState<ValidateViolationRow[]>([]);
  const [completionBlockedStatus, setCompletionBlockedStatus] = useState("");
  const [completionBlockedToolLabel, setCompletionBlockedToolLabel] = useState("Smart Tool");
  const [isSnapshotNotReadyModalOpen, setIsSnapshotNotReadyModalOpen] = useState(false);
  const [snapshotNotReadyToolLabel, setSnapshotNotReadyToolLabel] = useState("Smart Tool");
  const [snapshotNotReadyStatus, setSnapshotNotReadyStatus] = useState("");
  const [snapshotNotReadyErrors, setSnapshotNotReadyErrors] = useState<SnapshotPreflightErrorRow[]>([]);

  const [historyEntries, setHistoryEntries] = useState<BuildHistoryEntry[]>([]);
  const [selectedHistoryEntryId, setSelectedHistoryEntryId] = useState<string | null>(null);
  const [isHistoryModalOpen, setIsHistoryModalOpen] = useState(false);
  const [savedDeckDialogMode, setSavedDeckDialogMode] = useState<SavedDeckDialogMode | null>(null);
  const [savedDeckDialogTargetName, setSavedDeckDialogTargetName] = useState("");
  const [savedDeckDialogNameInput, setSavedDeckDialogNameInput] = useState("");
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [errorDetailsOpenSignal] = useState(0);
  const [releaseChecklistCopyNotice, setReleaseChecklistCopyNotice] = useState<string | null>(null);
  const [releaseChecklistCopyError, setReleaseChecklistCopyError] = useState<string | null>(null);
  const [apiPingSummary, setApiPingSummary] = useState<ApiPingSummary>(DEFAULT_API_PING_SUMMARY);
  const [resolvedDeckCardHints, setResolvedDeckCardHints] = useState<Record<string, DeckEditorCardHint>>({});
  const [resolveNamesMissingNames, setResolveNamesMissingNames] = useState<string[]>([]);

  const [hoverCard, setHoverCard] = useState<HoverCard | null>(null);
  const [previewImageFailures, setPreviewImageFailures] = useState<Record<string, true>>({});
  const [missingImageOracleIds, setMissingImageOracleIds] = useState<Record<string, true>>({});

  const [isCardModalOpen, setIsCardModalOpen] = useState(false);
  const [cardModalOracleId, setCardModalOracleId] = useState<string | null>(null);
  const [cardModalList, setCardModalList] = useState<string[]>([]);
  const [cardModalIndex, setCardModalIndex] = useState(0);
  const savedDeckDialogDescriptionId = useId();
  const savedDeckDialogValidationId = useId();

  const historyCounterRef = useRef(0);
  const hoverPrefetchLruRef = useRef<Set<string>>(new Set<string>());
  const completionRequestIdRef = useRef(0);
  const resolveDeckNamesRequestIdRef = useRef(0);
  const lastDeckTextMutationReasonRef = useRef("initial");
  const savedDeckDialogRestoreFocusRef = useRef<HTMLElement | null>(null);
  const savedDeckDialogShellRef = useRef<HTMLDivElement | null>(null);
  // Phase 4.14 Stage 1: mirror completionResult + completionError into refs
  // so the unified Complete handler can read the post-await values
  // synchronously without a stale-closure race.
  const completionResultRef = useRef<DeckCompleteResponseV1 | null>(null);
  const completionErrorRef = useRef<string | null>(null);
  // v1.1 Stage 2: same pattern for deckTuneResponse so the unified Upgrade
  // handler reads post-await values without the React state-flush race.
  const deckTuneResponseRef = useRef<DeckTuneResponseV1 | null>(null);

  // Phase 4.14 Stage 1: keep ref + state in sync so the unified Complete
  // handler can observe the results without a stale-closure race.
  useEffect(() => {
    completionResultRef.current = completionResult;
  }, [completionResult]);
  useEffect(() => {
    completionErrorRef.current = completionError;
  }, [completionError]);

  const isEditMode = workspaceMode === "EDIT";
  const isToolsMode = workspaceMode === "TOOLS";
  // Phase 4.14 Stage 1: ANALYZE mode removed. Conditional sites that
  // previously gated rendering on `isAnalyzeMode` now treat ANALYZE-only
  // surfaces as TOOLS-or-EDIT-rendered (consistent with how users actually
  // navigated the workspace). The constant is retained as `false` so any
  // missed call site short-circuits cleanly rather than raising a
  // ReferenceError; the few intentional `isAnalyzeMode` predicates below
  // are equivalent to `false` and were ANALYZE-mode-only flair.
  const isAnalyzeMode = false;

  const parsedDeckRows = useMemo(() => parseDecklistInput(deckText), [deckText, deckTextRevision]);
  const deckTextLineCount = parsedDeckRows.length;
  const deckCardsInPayloadOrder = useMemo(() => expandDecklistRowsInInputOrder(parsedDeckRows), [parsedDeckRows]);
  const deckPanelCommander = useMemo(() => resolveDeckPanelCommander(buildResponse), [buildResponse]);
  const deckPanelCards = useMemo(
    () => resolveDeckPanelCards(buildResponse, deckPanelCommander),
    [buildResponse, deckPanelCommander],
  );
  const deckEditorCardHints = useMemo(() => {
    const buildDerivedHints = buildDeckEditorCardHints(deckPanelCards);
    return {
      ...resolvedDeckCardHints,
      ...buildDerivedHints,
    };
  }, [deckPanelCards, resolvedDeckCardHints]);
  const savedDeckNames = useMemo(() => savedDecks.map((entry: SavedDeckEntry) => entry.name), [savedDecks]);
  const normalizedSavedDeckDialogNameInput = useMemo(
    () => normalizeSavedDeckName(savedDeckDialogNameInput),
    [savedDeckDialogNameInput],
  );
  const normalizedSavedDeckDialogTargetName = useMemo(
    () => normalizeSavedDeckName(savedDeckDialogTargetName),
    [savedDeckDialogTargetName],
  );
  const savedDeckDialogExistingDeck = useMemo(() => {
    if (savedDeckDialogMode !== "SAVE" || normalizedSavedDeckDialogNameInput === "") {
      return null;
    }

    return (
      savedDecks.find(
        (entry: SavedDeckEntry) => entry.name.toLowerCase() === normalizedSavedDeckDialogNameInput.toLowerCase(),
      ) || null
    );
  }, [normalizedSavedDeckDialogNameInput, savedDeckDialogMode, savedDecks]);
  const saveDialogHasChanges = useMemo(() => {
    if (!savedDeckDialogExistingDeck) {
      return false;
    }
    const normalizedCommanderName = commander.trim();
    return savedDeckDialogExistingDeck.commander !== normalizedCommanderName || savedDeckDialogExistingDeck.deckText !== deckText;
  }, [commander, deckText, savedDeckDialogExistingDeck]);
  const savedDeckDialogAtCapacity = useMemo(() => {
    if (savedDeckDialogMode !== "SAVE" || normalizedSavedDeckDialogNameInput === "") {
      return false;
    }
    if (savedDeckDialogExistingDeck) {
      return false;
    }
    return savedDecks.length >= SAVED_DECKS_MAX;
  }, [normalizedSavedDeckDialogNameInput, savedDeckDialogExistingDeck, savedDeckDialogMode, savedDecks.length]);
  const savedDeckDialogRenameConflict = useMemo(() => {
    if (savedDeckDialogMode !== "RENAME" || normalizedSavedDeckDialogNameInput === "") {
      return null;
    }

    const normalizedTargetKey = normalizedSavedDeckDialogTargetName.toLowerCase();
    return (
      savedDecks.find((entry: SavedDeckEntry) => {
        const entryKey = entry.name.toLowerCase();
        return entryKey === normalizedSavedDeckDialogNameInput.toLowerCase() && entryKey !== normalizedTargetKey;
      }) || null
    );
  }, [normalizedSavedDeckDialogNameInput, normalizedSavedDeckDialogTargetName, savedDeckDialogMode, savedDecks]);
  const savedDeckDialogValidationMessage = useMemo(() => {
    if (!savedDeckDialogMode || savedDeckDialogMode === "DELETE") {
      return "";
    }

    if (normalizedSavedDeckDialogNameInput === "") {
      return savedDeckDialogMode === "RENAME" ? "Deck name is required to rename." : "Deck name is required to save.";
    }

    if (savedDeckDialogMode === "SAVE") {
      if (commander.trim() === "" || deckText.trim() === "") {
        return "Add a commander and deck cards before saving.";
      }
      if (savedDeckDialogAtCapacity) {
        return `Saved deck limit (${SAVED_DECKS_MAX}) reached. Delete a saved deck before creating a new one.`;
      }
      return "";
    }

    if (savedDeckDialogRenameConflict) {
      return `Saved deck "${savedDeckDialogRenameConflict.name}" already exists.`;
    }
    return "";
  }, [
    commander,
    deckText,
    normalizedSavedDeckDialogNameInput,
    savedDeckDialogAtCapacity,
    savedDeckDialogMode,
    savedDeckDialogRenameConflict,
  ]);
  const savedDeckDialogDescribedBy = useMemo(() => {
    if (savedDeckDialogValidationMessage === "") {
      return savedDeckDialogDescriptionId;
    }
    return `${savedDeckDialogDescriptionId} ${savedDeckDialogValidationId}`;
  }, [savedDeckDialogDescriptionId, savedDeckDialogValidationId, savedDeckDialogValidationMessage]);
  const savedDeckDialogSubmitLabel = useMemo(() => {
    if (savedDeckDialogMode === "DELETE") {
      return "Delete Deck";
    }
    if (savedDeckDialogMode === "RENAME") {
      return "Rename Deck";
    }
    if (savedDeckDialogExistingDeck && saveDialogHasChanges) {
      return "Overwrite Deck";
    }
    return "Save Deck";
  }, [saveDialogHasChanges, savedDeckDialogExistingDeck, savedDeckDialogMode]);
  const savedDeckDialogSubmitDisabled = useMemo(() => {
    if (!savedDeckDialogMode) {
      return true;
    }
    if (savedDeckDialogMode === "DELETE") {
      return normalizedSavedDeckDialogTargetName === "";
    }
    return savedDeckDialogValidationMessage !== "";
  }, [normalizedSavedDeckDialogTargetName, savedDeckDialogMode, savedDeckDialogValidationMessage]);
  const tuneSwapRows = useMemo(() => asDeckTuneSwapRows(deckTuneResponse?.recommended_swaps_v1), [deckTuneResponse]);
  const completeAddedRows = useMemo(() => asDeckCompleteAddedRows(completionResult?.added_cards_v1), [completionResult]);
  const completedDecklistText = useMemo(() => asString(completionResult?.completed_decklist_text_v1), [completionResult]);
  // Game-Changer name set derived from /deck/complete_v1's game_changers_v1.
  // Memoized as a Set<string> for O(1) lookup in AddedCardRow.
  const gameChangerNameSet = useMemo<ReadonlySet<string>>(() => {
    const raw = completionResult?.game_changers_v1;
    if (!Array.isArray(raw)) return new Set<string>();
    return new Set<string>(raw.filter((n): n is string => typeof n === "string" && n.trim() !== ""));
  }, [completionResult]);
  // Memoized typed combo arrays — single source of truth feeding BOTH the
  // toolbar Combos badge AND the right-anchored CombosDrawer. Re-derives
  // whenever completionResult changes so re-Complete refreshes the drawer
  // (Bug 3 — old combo data persisting after re-Complete). Materializing
  // the typed entries here also guarantees the drawer no longer null-renders
  // when the engine response shape is the legacy `unknown[]` (Bug 2 — drawer
  // empty despite the toolbar count showing combos available).
  const detectedCombosForDrawer = useMemo<ReadonlyArray<DetectedComboEntry>>(() => {
    const raw = completionResult?.detected_combos_v1;
    return Array.isArray(raw) ? (raw as ReadonlyArray<DetectedComboEntry>) : [];
  }, [completionResult]);
  const missingPartnersForDrawer = useMemo<ReadonlyArray<MissingPartnerEntry>>(() => {
    const raw = completionResult?.missing_partners_v1;
    return Array.isArray(raw) ? (raw as ReadonlyArray<MissingPartnerEntry>) : [];
  }, [completionResult]);
  // Toolbar badge count derives from the SAME memos the drawer consumes,
  // so the badge and the drawer body cannot diverge.
  const combosTotalCount = useMemo<number>(
    () => detectedCombosForDrawer.length + missingPartnersForDrawer.length,
    [detectedCombosForDrawer, missingPartnersForDrawer],
  );
  // v1.3 Stage 1: diff-based fallback for AddedCardsPanel. When the
  // engine returns empty added_cards_v1 but the completed_decklist_text_v1
  // is actually larger than what we sent in, derive the additions
  // ourselves from the text-diff so the panel still renders something
  // meaningful. Engine source preferred (richer reasons); diff is the
  // fallback (placeholder reason — tooltip explains).
  const derivedAddedRows = useMemo(() => {
    if (completedDecklistText === "") return [] as ReadonlyArray<DeckCompleteAddedCardV1>;
    // v1.4: read preCompleteDeckTextRef synchronously — captured at click
    // handler entry (handleUnifiedCompleteDeck) BEFORE any state updates.
    // useRef has no closure/timing issues; the memo recomputes whenever
    // completedDecklistText changes (response arrives).
    return computeDeckAdditions(preCompleteDeckTextRef.current ?? "", completedDecklistText);
  }, [completedDecklistText]);
  const addedRowsForPanel = useMemo<ReadonlyArray<DeckCompleteAddedCardV1>>(
    () => (completeAddedRows.length > 0 ? completeAddedRows : derivedAddedRows),
    [completeAddedRows, derivedAddedRows],
  );
  // For AddedCardsPanel "Why?" tooltip: which source produced the
  // currently-shown rows? Engine reasons are domain codes; diff source
  // uses the placeholder "added_during_completion".
  const addedRowsSource: "engine" | "diff" | "none" = useMemo(() => {
    if (completeAddedRows.length > 0) return "engine";
    if (derivedAddedRows.length > 0) return "diff";
    return "none";
  }, [completeAddedRows.length, derivedAddedRows.length]);
  // v1.3 Stage 2: capture last non-empty upgrade suggestions into a local
  // snapshot so the panel can keep rendering across Apply-induced
  // USER_EDIT_DECK_TEXT-triggered reducer wipes. See `upgradeSnapshotRows`
  // declaration above for the false-pass context.
  useEffect(() => {
    if (deckState.upgradeSuggestions !== null && deckState.upgradeSuggestions.length > 0) {
      setUpgradeSnapshotRows(deckState.upgradeSuggestions);
    }
  }, [deckState.upgradeSuggestions]);
  const upgradeRowsForPanel = useMemo<ReadonlyArray<UpgradeSwapSuggestion>>(
    () =>
      deckState.upgradeSuggestions && deckState.upgradeSuggestions.length > 0
        ? deckState.upgradeSuggestions
        : upgradeSnapshotRows ?? [],
    [deckState.upgradeSuggestions, upgradeSnapshotRows],
  );
  const completionCardsAddedCount = useMemo(() => {
    const explicitCount = firstNumber(completionResult?.cards_added_count);
    if (explicitCount !== null) {
      return Math.max(0, Math.trunc(explicitCount));
    }
    return completeAddedRows.length;
  }, [completionResult, completeAddedRows.length]);
  const completionLandsAddedCount = useMemo(() => {
    const explicitCount = firstNumber(completionResult?.lands_added_count);
    if (explicitCount !== null) {
      return Math.max(0, Math.trunc(explicitCount));
    }
    return completeAddedRows.filter((row: DeckCompleteAddedCardV1) => isLikelyLandAddition(row)).length;
  }, [completionResult, completeAddedRows]);
  const canApplyCompletedDecklist = useMemo(() => completedDecklistText.trim() !== "", [completedDecklistText]);
  const deckTrimRows = useMemo(() => buildDeckTrimRows(pendingCutOrder, buildResponse), [pendingCutOrder, buildResponse]);
  const hoverArtReleaseMetrics = useMemo(() => {
    const seenKeys = new Set<string>();
    let targetCount = 0;
    let resolvedCount = 0;
    let skippedBasicCount = 0;

    for (const row of parsedDeckRows) {
      const cardName = row.name.trim();
      const cardKey = normalizeNameToken(cardName);
      if (cardKey === "" || seenKeys.has(cardKey)) {
        continue;
      }
      seenKeys.add(cardKey);

      if (isBasicLandName(cardName)) {
        skippedBasicCount += 1;
        continue;
      }

      targetCount += 1;
      const hint = deckEditorCardHints[cardKey];
      if (hint && hint.oracleId.trim() !== "") {
        resolvedCount += 1;
      }
    }

    const artReadyPercent = targetCount === 0 ? 100 : Math.round((resolvedCount / targetCount) * 100);
    return {
      targetCount,
      resolvedCount,
      skippedBasicCount,
      artReadyPercent,
      isReady: targetCount === 0 || resolvedCount === targetCount,
    };
  }, [deckEditorCardHints, parsedDeckRows]);
  const snapshotNotReadyFixInstructions = useMemo(
    () => collectSnapshotPreflightFixInstructions(snapshotNotReadyErrors),
    [snapshotNotReadyErrors],
  );
  const currentDeckHash = useMemo(() => buildDeckHashV1(commander, deckText), [commander, deckText]);
  const currentBuildHash = useMemo(() => {
    const result = asRecord(buildResponse?.result);
    return firstNonEmptyString(buildResponse?.build_hash_v1, buildResponse?.build_hash, result?.build_hash) || "";
  }, [buildResponse]);
  const lastToolRequestResponseSummaries = useMemo(() => {
    return historyEntries.slice(0, 8).map((entry: BuildHistoryEntry) => {
      return {
        timestamp_iso: entry.timestamp_iso,
        tool_type: asString(entry.tool_type) || entry.status,
        status: entry.status,
        request_summary: {
          db_snapshot_id: entry.request_payload.db_snapshot_id,
          profile_id: entry.request_payload.profile_id,
          bracket_id: entry.request_payload.bracket_id,
          commander: entry.request_payload.commander,
          cards_count: entry.request_payload.cards.length,
        },
        response_summary: {
          summary_counts: entry.summary_counts || null,
          input_deck_hash: entry.input_deck_hash || null,
          output_deck_hash: entry.output_deck_hash || null,
        },
      };
    });
  }, [historyEntries]);
  const normalizedApiBase = useMemo(() => normalizeApiBase(apiBase), [apiBase]);
  const prefetchSnapshotId = useMemo(
    () => firstNonEmptyString(snapshotId, apiPingSummary.dbSnapshotId, buildResponse?.db_snapshot_id) || "",
    [apiPingSummary.dbSnapshotId, buildResponse?.db_snapshot_id, snapshotId],
  );
  const prefetchSnapshotImagesCommand = useMemo(
    () => buildPrefetchCardImagesCommand(prefetchSnapshotId),
    [prefetchSnapshotId],
  );
  // Phase 4.13.2 single persistence useEffect with explicit guard.
  // BLOCKED unless `isHydrated && source !== "fallback"` — this guard
  // eliminates the race-condition class of bugs that drove 4.12.1 / 4.13 /
  // 4.13.1 hotfixes. Default Krenko state never writes to the slot; the
  // first real user edit (USER_EDIT_*) upgrades source to "manual" and
  // unlocks persistence. Imported decks land via HYDRATE_FROM_IMPORT_SLOT
  // with their actual source ("archidekt" / "arena_text" / etc) and write
  // immediately. GoldfishView reads the slot directly — its 3-source
  // precedence chain (HARD #13) is unchanged.
  useEffect(() => {
    if (typeof window === "undefined") return;
    if (!deckState.isHydrated) return;
    if (deckState.source === "fallback") return;
    if (!deckState.commander || deckState.commander.trim() === "") return;
    if (!deckState.deckText || deckState.deckText.trim() === "") return;
    try {
      const payload = buildActiveDeckPayload({
        commander: deckState.commander,
        decklist: deckState.deckText,
        source: deckState.source,
      });
      window.localStorage.setItem(ACTIVE_DECK_STORAGE_KEY, JSON.stringify(payload));
    } catch {
      // QuotaExceededError or serialization failure — silent per
      // autonomous_repair_log #4 (storage adapter pattern).
    }
  }, [deckState.commander, deckState.deckText, deckState.source, deckState.buildResponse, deckState.isHydrated]);

  const missingImageCount = useMemo(() => Object.keys(missingImageOracleIds).length, [missingImageOracleIds]);
  const uiModeLabel: "DEV" | "PROD" = import.meta.env.DEV ? "DEV" : "PROD";
  const uiCommit = useMemo(() => {
    const env = import.meta.env as Record<string, unknown>;
    const candidates = [env.VITE_GIT_SHA, env.VITE_COMMIT_SHA, env.UI_COMMIT, env.COMMIT_SHA];
    for (const rawCandidate of candidates) {
      if (typeof rawCandidate !== "string") {
        continue;
      }
      const candidate = rawCandidate.trim();
      if (candidate !== "") {
        return candidate;
      }
    }
    return "-";
  }, []);
  const isAnyToolRunning = runningSmartTrim || runningSmartTune || runningSmartComplete;

  useEffect(() => {
    try {
      window.localStorage.setItem(WORKSPACE_MODE_STORAGE_KEY, workspaceMode);
    } catch {
      // Ignore persistence failures (privacy mode/quota).
    }
  }, [workspaceMode]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    try {
      if (savedDecks.length === 0) {
        window.localStorage.removeItem(SAVED_DECKS_STORAGE_KEY);
        return;
      }
      window.localStorage.setItem(SAVED_DECKS_STORAGE_KEY, JSON.stringify(savedDecks));
    } catch {
      // Ignore persistence failures (privacy mode/quota).
    }
  }, [savedDecks]);

  useEffect(() => {
    const normalizedSelection = normalizeSavedDeckName(selectedSavedDeckName);
    if (normalizedSelection === "") {
      return;
    }

    const matchingSavedDeck = savedDecks.find(
      (entry: SavedDeckEntry) => entry.name.toLowerCase() === normalizedSelection.toLowerCase(),
    );
    if (!matchingSavedDeck) {
      setSelectedSavedDeckName("");
      return;
    }

    if (selectedSavedDeckName !== matchingSavedDeck.name) {
      setSelectedSavedDeckName(matchingSavedDeck.name);
    }
  }, [savedDecks, selectedSavedDeckName]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    try {
      const normalizedSelection = normalizeSavedDeckName(selectedSavedDeckName);
      if (normalizedSelection === "") {
        window.localStorage.removeItem(SELECTED_SAVED_DECK_STORAGE_KEY);
        return;
      }
      window.localStorage.setItem(SELECTED_SAVED_DECK_STORAGE_KEY, normalizedSelection);
    } catch {
      // Ignore persistence failures (privacy mode/quota).
    }
  }, [selectedSavedDeckName]);

  useEffect(() => {
    if (!toastMessage) {
      return;
    }
    const timerId = window.setTimeout(() => {
      setToastMessage(null);
    }, DEV_SMOKE_TEST_TOAST_DURATION_MS);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [toastMessage]);

  useEffect(() => {
    if (typeof window === "undefined" || typeof document === "undefined" || !savedDeckDialogMode) {
      return;
    }

    const dialogShell = savedDeckDialogShellRef.current;
    const resolveFocusableElements = (): HTMLElement[] => {
      if (!dialogShell) {
        return [];
      }

      return Array.from(
        dialogShell.querySelectorAll<HTMLElement>(
          'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
        ),
      ).filter((element: HTMLElement) => element.getAttribute("aria-hidden") !== "true");
    };

    const preferredFocusTarget =
      savedDeckDialogMode === "DELETE"
        ? dialogShell?.querySelector<HTMLElement>('[data-saved-deck-submit="true"]')
        : dialogShell?.querySelector<HTMLElement>('[data-saved-deck-name-input="true"]');
    if (preferredFocusTarget && !preferredFocusTarget.hasAttribute("disabled")) {
      preferredFocusTarget.focus();
      if (preferredFocusTarget instanceof HTMLInputElement) {
        preferredFocusTarget.select();
      }
    } else {
      const focusableElements = resolveFocusableElements();
      if (focusableElements.length > 0) {
        focusableElements[0].focus();
      } else {
        dialogShell?.focus();
      }
    }

    function handleKeyDown(event: KeyboardEvent): void {
      if (event.key === "Escape") {
        event.preventDefault();
        closeSavedDeckDialog();
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const tabOrderedElements = resolveFocusableElements();
      if (tabOrderedElements.length === 0) {
        event.preventDefault();
        dialogShell?.focus();
        return;
      }

      const firstElement = tabOrderedElements[0];
      const lastElement = tabOrderedElements[tabOrderedElements.length - 1];
      const activeElement = document.activeElement;
      const activeWithinDialog = activeElement instanceof Node ? dialogShell?.contains(activeElement) : false;

      if (event.shiftKey) {
        if (!activeWithinDialog || activeElement === firstElement) {
          event.preventDefault();
          lastElement.focus();
        }
        return;
      }

      if (!activeWithinDialog || activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [savedDeckDialogMode]);

  function captureSavedDeckDialogFocusOrigin(): void {
    if (typeof document === "undefined") {
      savedDeckDialogRestoreFocusRef.current = null;
      return;
    }
    const activeElement = document.activeElement;
    savedDeckDialogRestoreFocusRef.current = activeElement instanceof HTMLElement ? activeElement : null;
  }

  useEffect(() => {
    if (savedDeckDialogMode) {
      return;
    }

    const focusTarget = savedDeckDialogRestoreFocusRef.current;
    savedDeckDialogRestoreFocusRef.current = null;
    if (!focusTarget || typeof focusTarget.focus !== "function") {
      return;
    }
    focusTarget.focus();
  }, [savedDeckDialogMode]);

  useEffect(() => {
    let disposed = false;
    const base = normalizeApiBase(apiBase);

    const probeApiReachability = async (): Promise<void> => {
      try {
        const response = await fetch(`${base}/snapshots?limit=1`, {
          method: "GET",
        });
        if (!disposed) {
          setIsApiReachable(response.ok);
        }
      } catch {
        if (!disposed) {
          setIsApiReachable(false);
        }
      }
    };

    void probeApiReachability();
    const timerId = window.setInterval(() => {
      void probeApiReachability();
    }, 30000);

    return () => {
      disposed = true;
      window.clearInterval(timerId);
    };
  }, [apiBase]);

  useEffect(() => {
    let disposed = false;
    const endpoints = ["/health", "/version"] as const;

    const probeApiPing = async (): Promise<void> => {
      for (const endpoint of endpoints) {
        try {
          const response = await fetch(`${normalizedApiBase}${endpoint}`, {
            method: "GET",
          });
          const text = await response.text();
          const parsed = safeParseJson(text);
          if (!response.ok) {
            continue;
          }

          const root = asRecord(parsed);
          const resultPayload = asRecord(root?.result);
          const versionPayload = asRecord(root?.version);
          const payload = resultPayload || versionPayload || root;
          const payloadPipelineVersions = asRecord(payload?.pipeline_versions);
          const rootPipelineVersions = asRecord(root?.pipeline_versions);

          const nextSummary: ApiPingSummary = {
            status: "OK",
            endpoint,
            engineVersion: firstNonEmptyString(payload?.engine_version, root?.engine_version) || "-",
            dbSnapshotId: firstNonEmptyString(payload?.db_snapshot_id, payload?.snapshot_id, root?.db_snapshot_id, root?.snapshot_id) || "-",
            rulesetVersion: firstNonEmptyString(payload?.ruleset_version, root?.ruleset_version) || "-",
            bracketDefinitionVersion:
              firstNonEmptyString(
                payload?.bracket_definition_version,
                payloadPipelineVersions?.bracket_definition_version,
                root?.bracket_definition_version,
                rootPipelineVersions?.bracket_definition_version,
              ) || "-",
          };
          if (!disposed) {
            setApiPingSummary(nextSummary);
          }
          return;
        } catch {
          // Fall through to next endpoint.
        }
      }

      if (!disposed) {
        setApiPingSummary({
          status: "ERROR",
          endpoint: "-",
          engineVersion: "-",
          dbSnapshotId: "-",
          rulesetVersion: "-",
          bracketDefinitionVersion: "-",
        });
      }
    };

    void probeApiPing();
    const timerId = window.setInterval(() => {
      void probeApiPing();
    }, 30000);

    return () => {
      disposed = true;
      window.clearInterval(timerId);
    };
  }, [normalizedApiBase]);

  useEffect(() => {
    if (snapshotId.trim() !== "") {
      return;
    }

    const base = normalizeApiBase(apiBase);
    let cancelled = false;

    void (async () => {
      try {
        const latestSnapshotId = await fetchLatestSnapshotIdFromApi(base);
        if (!cancelled && latestSnapshotId !== "") {
          setSnapshotId(latestSnapshotId);
        }
      } catch {
        // Intentionally silent; build button reports explicit error if snapshot lookup fails.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [apiBase, snapshotId]);

  useEffect(() => {
    const imageUrl = buildHoverPreviewImageUrl(normalizedApiBase, hoverCard?.oracle_id || "");
    if (imageUrl === "") {
      return;
    }

    setPreviewImageFailures((previous: Record<string, true>) => {
      if (!previous[imageUrl]) {
        return previous;
      }
      const next = { ...previous };
      delete next[imageUrl];
      return next;
    });

    const lru = hoverPrefetchLruRef.current;
    if (lru.has(imageUrl)) {
      // Refresh recency when revisiting an already-prefetched URL.
      lru.delete(imageUrl);
      lru.add(imageUrl);
      return;
    }

    lru.add(imageUrl);
    while (lru.size > HOVER_PREFETCH_LRU_LIMIT) {
      const oldest = lru.values().next().value;
      if (typeof oldest !== "string") {
        break;
      }
      lru.delete(oldest);
    }

    const prefetchImage = new Image();
    prefetchImage.src = imageUrl;
  }, [hoverCard?.oracle_id, normalizedApiBase]);

  function runLocalValidate(): { ok: boolean; message: string } {
    if (commander.trim() === "") {
      return { ok: false, message: "Commander is required." };
    }
    if (parsedDeckRows.length === 0) {
      return { ok: false, message: "Decklist must include at least one parsed card row." };
    }
    return {
      ok: true,
      message: `Validated locally: ${parsedDeckRows.length} parsed lines / ${deckCardsInPayloadOrder.length} cards in payload order.`,
    };
  }

  function collectResolveNamesTargets(commanderName: string, rows: ReturnType<typeof parseDecklistInput>): string[] {
    const targets: string[] = [];
    const seen = new Set<string>();

    const hasResolvedHint = (nameKey: string): boolean => {
      const hint = deckEditorCardHints[nameKey];
      return Boolean(hint && hint.oracleId.trim() !== "");
    };

    const commanderToken = commanderName.trim();
    if (commanderToken !== "") {
      const commanderKey = normalizeNameToken(commanderToken);
      if (commanderKey !== "") {
        seen.add(commanderKey);
        if (!hasResolvedHint(commanderKey)) {
          targets.push(commanderToken);
        }
      }
    }

    for (const row of rows) {
      const cardName = row.name.trim();
      const cardKey = normalizeNameToken(cardName);
      if (cardName === "" || cardKey === "" || seen.has(cardKey)) {
        continue;
      }
      seen.add(cardKey);
      if (hasResolvedHint(cardKey)) {
        continue;
      }
      targets.push(cardName);
    }

    return targets;
  }

  async function resolveDeckRowsAndStoreHints(
    rows: ReturnType<typeof parseDecklistInput>,
    snapshotIdOverride?: string,
    commanderOverride?: string,
  ): Promise<number> {
    const requestId = resolveDeckNamesRequestIdRef.current + 1;
    resolveDeckNamesRequestIdRef.current = requestId;

    const base = normalizeApiBase(apiBase);
    const resolvedSnapshotId = (snapshotIdOverride || snapshotId).trim() || (await ensureSmartToolSnapshotId(base));
    const targets = collectResolveNamesTargets(commanderOverride || commander, rows);
    if (targets.length === 0) {
      setResolveNamesMissingNames([]);
      return 0;
    }

    const resolvedHints: Record<string, DeckEditorCardHint> = {};
    const missingNames: string[] = [];
    const missingSeen = new Set<string>();
    const targetChunks = chunkStringsInOrder(targets, RESOLVE_NAMES_MAX_NAMES_PER_REQUEST);

    for (const namesChunk of targetChunks) {
      const requestPayload = {
        snapshot_id: resolvedSnapshotId,
        names: namesChunk,
      };

      let response: Response;
      try {
        response = await fetch(`${base}/cards/resolve_names`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: serializeRequestBodyForNetwork("/cards/resolve_names", requestPayload),
        });
      } catch (error) {
        const details = buildApiErrorDetails("/cards/resolve_names", null, "", null, {
          method: "POST",
          requestPayload,
          error,
        });
        setApiErrorDetails(details);
        throw new Error(formatApiErrorMessage(details));
      }

      const responseText = await response.text();
      const parsed = safeParseJson(responseText);
      if (!response.ok) {
        const details = buildApiErrorDetails("/cards/resolve_names", response, responseText, parsed, {
          method: "POST",
          requestPayload,
        });
        setApiErrorDetails(details);
        throw new Error(formatApiErrorMessage(details));
      }

      const payload = asRecord(parsed);
      const chunkMissingNames = extractResolveNamesMissingNames(payload);
      for (const missingName of chunkMissingNames) {
        const key = normalizeNameToken(missingName);
        if (key === "" || missingSeen.has(key)) {
          continue;
        }
        missingSeen.add(key);
        missingNames.push(missingName);
      }
      const resultRows = asArray(payload?.results);
      for (const rawRow of resultRows) {
        const row = asRecord(rawRow);
        if (!row) {
          continue;
        }
        const resolvedName = firstNonEmptyString(row.name, row.input);
        const oracleId = firstNonEmptyString(row.oracle_id);
        if (!resolvedName || !oracleId) {
          continue;
        }
        const key = normalizeNameToken(resolvedName);
        if (key === "") {
          continue;
        }

        resolvedHints[key] = {
          oracleId,
          typeLine: firstNonEmptyString(row.type_line),
        };
      }
    }

    if (requestId !== resolveDeckNamesRequestIdRef.current) {
      return 0;
    }

    setResolveNamesMissingNames(missingNames);

    const resolvedEntries = Object.entries(resolvedHints);
    if (resolvedEntries.length > 0) {
      setResolvedDeckCardHints((previous: Record<string, DeckEditorCardHint>) => {
        const next = {
          ...previous,
        };
        for (const [key, value] of resolvedEntries) {
          next[key] = {
            oracleId: value.oracleId,
            typeLine: value.typeLine,
          };
        }
        return next;
      });
    }

    return resolvedEntries.length;
  }

  function applyDeckTextAndResolveHints(
    nextDeckText: string,
    nextCommander: string,
    snapshotIdOverride?: string,
  ): void {
    applyDeckText(nextDeckText, "apply_deck_text_and_resolve_hints");
    if (nextCommander.trim() !== "") {
      setCommander(nextCommander.trim());
    }
    setBuildResponse(null);

    const nextRows = parseDecklistInput(nextDeckText);
    void resolveDeckRowsAndStoreHints(nextRows, snapshotIdOverride, nextCommander).catch((error) => {
      const message = error instanceof Error ? error.message : "Failed to resolve deck names for preview.";
      setRuntimeError(message);
    });
  }

  function applyDeckText(nextDeckText: string, reason: string): void {
    const normalizedReason = reason.trim() || "unknown";
    lastDeckTextMutationReasonRef.current = normalizedReason;
    setDeckText(nextDeckText);
    setDeckTextRevision((previous: number) => previous + 1);
    setHoverCard(null);
  }

  function handleSelectedSavedDeckNameChange(deckNameRaw: string): void {
    setSelectedSavedDeckName(normalizeSavedDeckName(deckNameRaw));
  }

  function closeSavedDeckDialog(): void {
    setSavedDeckDialogMode(null);
    setSavedDeckDialogTargetName("");
    setSavedDeckDialogNameInput("");
  }

  function saveDeckByName(deckNameRaw: string): boolean {
    const deckName = normalizeSavedDeckName(deckNameRaw);
    if (deckName === "") {
      setToastMessage("Deck name is required to save.");
      return false;
    }

    const normalizedCommanderName = commander.trim();
    if (normalizedCommanderName === "" || deckText.trim() === "") {
      setToastMessage("Add a commander and deck cards before saving.");
      return false;
    }

    const existingDeck = savedDecks.find((entry: SavedDeckEntry) => entry.name.toLowerCase() === deckName.toLowerCase());
    if (existingDeck) {
      const hasChanged = existingDeck.commander !== normalizedCommanderName || existingDeck.deckText !== deckText;
      if (!hasChanged) {
        setSelectedSavedDeckName(existingDeck.name);
        setToastMessage(`Deck "${existingDeck.name}" is already up to date.`);
        return true;
      }
    }

    if (!existingDeck && savedDecks.length >= SAVED_DECKS_MAX) {
      setToastMessage(`Saved deck limit (${SAVED_DECKS_MAX}) reached. Delete a saved deck before creating a new one.`);
      return false;
    }

    const now = Date.now();
    setSavedDecks((previous: SavedDeckEntry[]) => {
      const filtered = previous.filter((entry: SavedDeckEntry) => entry.name.toLowerCase() !== deckName.toLowerCase());
      const next: SavedDeckEntry[] = [
        {
          name: deckName,
          commander: normalizedCommanderName,
          deckText,
          updatedAtMs: now,
        },
        ...filtered,
      ];
      next.sort((left: SavedDeckEntry, right: SavedDeckEntry) => {
        if (left.updatedAtMs !== right.updatedAtMs) {
          return right.updatedAtMs - left.updatedAtMs;
        }
        return left.name.localeCompare(right.name);
      });
      return next.slice(0, SAVED_DECKS_MAX);
    });

    // Phase 4 BUNDLE Integration (4.13) Stage 2: ALSO persist via the
    // lib/decks/savedDecks.ts adapter (writes to mtgdb:decks:* per Phase
    // 4.12a). Legacy mtg_saved_decks_v1 stays as a fallback for backward
    // compat through this release per autonomous_repair_log #4. Errors are
    // swallowed so the legacy save path remains the source of truth.
    try {
      _persistDeckViaAdapter({
        name: deckName,
        decklist: deckText,
        commander_oracle_id: null,
        nowMs: now,
      });
    } catch {
      /* legacy slot still wrote successfully above; surface noise here would be misleading */
    }

    setSelectedSavedDeckName(deckName);
    setToastMessage(`Saved deck "${deckName}".`);
    return true;
  }

  function handleSaveDeck(): void {
    captureSavedDeckDialogFocusOrigin();
    const suggestedName = normalizeSavedDeckName(selectedSavedDeckName || commander || "My Deck");
    setSavedDeckDialogMode("SAVE");
    setSavedDeckDialogTargetName("");
    setSavedDeckDialogNameInput(suggestedName);
  }

  function handleLoadSavedDeck(deckNameRaw: string): void {
    const normalizedDeckName = normalizeSavedDeckName(deckNameRaw);
    if (normalizedDeckName === "") {
      return;
    }

    const savedDeck = savedDecks.find(
      (entry: SavedDeckEntry) => entry.name.toLowerCase() === normalizedDeckName.toLowerCase(),
    );
    if (!savedDeck) {
      setToastMessage(`Saved deck "${normalizedDeckName}" was not found.`);
      return;
    }

    setSelectedSavedDeckName(savedDeck.name);
    setCommander(savedDeck.commander);
    applyDeckText(savedDeck.deckText, "load_saved_deck");
    setBuildResponse(null);
    setDeckTuneResponse(null);
    setDeckTrimResult(null);
    setCompletionResult(null);
    setCompletionError(null);
    setToastMessage(`Opened deck "${savedDeck.name}".`);
  }

  function renameSavedDeckByName(deckNameRaw: string, nextDeckNameRaw: string): boolean {
    const normalizedDeckName = normalizeSavedDeckName(deckNameRaw);
    if (normalizedDeckName === "") {
      return false;
    }

    const existingDeck = savedDecks.find(
      (entry: SavedDeckEntry) => entry.name.toLowerCase() === normalizedDeckName.toLowerCase(),
    );
    if (!existingDeck) {
      setToastMessage(`Saved deck "${normalizedDeckName}" was not found.`);
      return false;
    }

    const renamedDeckName = normalizeSavedDeckName(nextDeckNameRaw);
    if (renamedDeckName === "") {
      setToastMessage("Deck name is required to rename.");
      return false;
    }

    if (renamedDeckName.toLowerCase() === existingDeck.name.toLowerCase()) {
      setSelectedSavedDeckName(renamedDeckName);
      if (renamedDeckName === existingDeck.name) {
        setToastMessage(`Deck "${existingDeck.name}" is already named that.`);
      } else {
        setSavedDecks((previous: SavedDeckEntry[]) => {
          return previous.map((entry: SavedDeckEntry) => {
            if (entry.name.toLowerCase() !== existingDeck.name.toLowerCase()) {
              return entry;
            }
            return {
              ...entry,
              name: renamedDeckName,
            };
          });
        });
        setToastMessage(`Renamed deck "${existingDeck.name}" to "${renamedDeckName}".`);
      }
      return true;
    }

    const conflictingDeck = savedDecks.find(
      (entry: SavedDeckEntry) => entry.name.toLowerCase() === renamedDeckName.toLowerCase(),
    );
    if (conflictingDeck) {
      setToastMessage(`Saved deck "${renamedDeckName}" already exists.`);
      return false;
    }

    const now = Date.now();
    setSavedDecks((previous: SavedDeckEntry[]) => {
      const next = previous.map((entry: SavedDeckEntry) => {
        if (entry.name.toLowerCase() !== existingDeck.name.toLowerCase()) {
          return entry;
        }
        return {
          ...entry,
          name: renamedDeckName,
          updatedAtMs: now,
        };
      });

      next.sort((left: SavedDeckEntry, right: SavedDeckEntry) => {
        if (left.updatedAtMs !== right.updatedAtMs) {
          return right.updatedAtMs - left.updatedAtMs;
        }
        return left.name.localeCompare(right.name);
      });
      return next;
    });

    setSelectedSavedDeckName(renamedDeckName);
    setToastMessage(`Renamed deck "${existingDeck.name}" to "${renamedDeckName}".`);
    return true;
  }

  function handleRenameSavedDeck(deckNameRaw: string): void {
    const normalizedDeckName = normalizeSavedDeckName(deckNameRaw);
    if (normalizedDeckName === "") {
      return;
    }

    const existingDeck = savedDecks.find(
      (entry: SavedDeckEntry) => entry.name.toLowerCase() === normalizedDeckName.toLowerCase(),
    );
    if (!existingDeck) {
      setToastMessage(`Saved deck "${normalizedDeckName}" was not found.`);
      return;
    }

    captureSavedDeckDialogFocusOrigin();
    setSavedDeckDialogMode("RENAME");
    setSavedDeckDialogTargetName(existingDeck.name);
    setSavedDeckDialogNameInput(existingDeck.name);
  }

  function deleteSavedDeckByName(deckNameRaw: string): boolean {
    const normalizedDeckName = normalizeSavedDeckName(deckNameRaw);
    if (normalizedDeckName === "") {
      return false;
    }

    const existingDeck = savedDecks.find(
      (entry: SavedDeckEntry) => entry.name.toLowerCase() === normalizedDeckName.toLowerCase(),
    );
    if (!existingDeck) {
      setToastMessage(`Saved deck "${normalizedDeckName}" was not found.`);
      return false;
    }

    setSavedDecks((previous: SavedDeckEntry[]) => {
      return previous.filter((entry: SavedDeckEntry) => entry.name.toLowerCase() !== existingDeck.name.toLowerCase());
    });

    if (selectedSavedDeckName.toLowerCase() === existingDeck.name.toLowerCase()) {
      setSelectedSavedDeckName("");
    }
    setToastMessage(`Deleted deck "${existingDeck.name}".`);
    return true;
  }

  function handleDeleteSavedDeck(deckNameRaw: string): void {
    const normalizedDeckName = normalizeSavedDeckName(deckNameRaw);
    if (normalizedDeckName === "") {
      return;
    }

    const existingDeck = savedDecks.find(
      (entry: SavedDeckEntry) => entry.name.toLowerCase() === normalizedDeckName.toLowerCase(),
    );
    if (!existingDeck) {
      setToastMessage(`Saved deck "${normalizedDeckName}" was not found.`);
      return;
    }

    captureSavedDeckDialogFocusOrigin();
    setSavedDeckDialogMode("DELETE");
    setSavedDeckDialogTargetName(existingDeck.name);
    setSavedDeckDialogNameInput(existingDeck.name);
  }

  function handleSubmitSavedDeckDialog(): void {
    if (savedDeckDialogSubmitDisabled) {
      return;
    }

    if (savedDeckDialogMode === "SAVE") {
      const didSave = saveDeckByName(savedDeckDialogNameInput);
      if (didSave) {
        closeSavedDeckDialog();
      }
      return;
    }

    if (savedDeckDialogMode === "RENAME") {
      const didRename = renameSavedDeckByName(savedDeckDialogTargetName, savedDeckDialogNameInput);
      if (didRename) {
        closeSavedDeckDialog();
      }
      return;
    }

    if (savedDeckDialogMode === "DELETE") {
      const didDelete = deleteSavedDeckByName(savedDeckDialogTargetName);
      if (didDelete) {
        closeSavedDeckDialog();
      }
    }
  }

  function persistSmartToolHistory(
    toolLabel: string,
    payloadCards: string[],
    details: Record<string, unknown>,
    statusLabel = "SMART_TOOL",
    snapshotIdOverride?: string,
    commanderOverride?: string,
    options: SmartToolHistoryOptions = {},
  ): void {
    const resolvedSnapshotId = (snapshotIdOverride || snapshotId).trim();
    const resolvedCommander = (commanderOverride || commander).trim();
    const inputCards = (options.inputCards && options.inputCards.length > 0 ? options.inputCards : payloadCards)
      .map((name: string) => name.trim())
      .filter((name: string) => name !== "");
    const outputCards = (options.outputCards && options.outputCards.length > 0 ? options.outputCards : inputCards)
      .map((name: string) => name.trim())
      .filter((name: string) => name !== "");
    const inputDeckText = options.inputDeckText || collapseCardNamesInInputOrder(inputCards);
    const outputDeckText = options.outputDeckText || collapseCardNamesInInputOrder(outputCards);

    const inputDeckHash = buildDeckHashV1(resolvedCommander, inputDeckText);
    const outputDeckHash = buildDeckHashV1(resolvedCommander, outputDeckText);
    const deckDiff = buildDeckDiff(inputCards, outputCards);

    const payload: BuildRequestPayload = {
      db_snapshot_id: resolvedSnapshotId,
      profile_id: profileId.trim(),
      bracket_id: bracketId.trim(),
      format: "commander",
      commander: resolvedCommander,
      cards: inputCards,
      engine_patches_v0: [],
    };

    const responseBody: BuildResponsePayload = {
      status: statusLabel,
      db_snapshot_id: resolvedSnapshotId,
      profile_id: payload.profile_id,
      bracket_id: payload.bracket_id,
      result: {
        smart_tool_v1: {
          tool: toolLabel,
          ...details,
        },
      },
    };

    const now = new Date();
    historyCounterRef.current += 1;
    const id = `${now.getTime()}-${historyCounterRef.current}`;
    const deckName = `${resolvedCommander || "Untitled deck"} · ${toolLabel}`;

    const nextEntry: BuildHistoryEntry = {
      id,
      timestamp_iso: now.toISOString(),
      timestamp_label: buildTimestampLabel(now),
      deck_name: deckName,
      commander_input: resolvedCommander,
      db_snapshot_id: resolvedSnapshotId,
      profile_id: payload.profile_id,
      bracket_id: payload.bracket_id,
      status: statusLabel,
      request_payload: payload,
      response_body: responseBody,
      tool_type: options.toolType || statusLabel,
      input_deck_hash: inputDeckHash,
      output_deck_hash: outputDeckHash,
      output_deck_text: outputDeckText,
      output_cards: outputCards,
      summary_counts: options.summaryCounts,
      deck_diff: deckDiff,
    };

    setHistoryEntries((previous: BuildHistoryEntry[]) => [nextEntry, ...previous].slice(0, 20));
    setSelectedHistoryEntryId(id);
  }

  async function ensureSmartToolSnapshotId(base: string): Promise<string> {
    let resolvedSnapshotId = snapshotId.trim();
    if (resolvedSnapshotId === "") {
      resolvedSnapshotId = await fetchLatestSnapshotIdFromApi(base);
      setSnapshotId(resolvedSnapshotId);
    }
    return resolvedSnapshotId;
  }

  function showValidateBlockedModal(
    toolLabel: string,
    status: string,
    unknowns: ValidateUnknownRow[],
    violations: ValidateViolationRow[],
  ): void {
    setCompletionBlockedToolLabel(toolLabel);
    setCompletionBlockedStatus(status || "UNKNOWN");
    setCompletionBlockedUnknowns(unknowns);
    setCompletionBlockedViolations(violations);
    setIsCompletionBlockedModalOpen(true);
  }

  function showSnapshotNotReadyModal(toolLabel: string, status: string, errors: SnapshotPreflightErrorRow[]): void {
    setIsCompletionBlockedModalOpen(false);
    setSnapshotNotReadyToolLabel(toolLabel);
    setSnapshotNotReadyStatus(status || "UNKNOWN");
    setSnapshotNotReadyErrors(errors);
    setIsSnapshotNotReadyModalOpen(true);
  }

  async function runSnapshotPreflightOrBlock(
    toolLabel: string,
    base: string,
    resolvedSnapshotId: string,
    requestDebug?: unknown,
  ): Promise<boolean> {
    const endpoint = `/snapshot/preflight/${encodeURIComponent(resolvedSnapshotId)}`;
    const requestPayload = {
      snapshot_id: resolvedSnapshotId,
      tool_label: toolLabel,
    };

    let preflightResponse: Response;
    try {
      preflightResponse = await fetch(`${base}${endpoint}`, {
        method: "GET",
      });
    } catch (error) {
      setIsApiReachable(false);
      const details = buildApiErrorDetails(endpoint, null, "", null, {
        method: "GET",
        requestPayload,
        requestDebug,
        error,
      });
      setApiErrorDetails(details);
      const message = error instanceof Error ? error.message : "Network request failed.";
      throw new Error(`Request failed for ${endpoint}: ${message}`);
    }

    const preflightText = await preflightResponse.text();
    const preflightParsed = safeParseJson(preflightText);
    setIsApiReachable(true);

    if (!preflightResponse.ok) {
      const details = buildApiErrorDetails(endpoint, preflightResponse, preflightText, preflightParsed, {
        method: "GET",
        requestPayload,
        requestDebug,
      });
      setApiErrorDetails(details);
      throw new Error(`HTTP ${preflightResponse.status} from ${endpoint}`);
    }

    const preflightRoot = asRecord(preflightParsed) ?? {};
    const preflightStatus = asString(preflightRoot.status) || "UNKNOWN";
    if (preflightStatus === "OK") {
      return true;
    }

    const preflightErrors = asSnapshotPreflightErrorRows(preflightRoot.errors);
    setSmartToolBlockUnknowns([]);
    setSmartToolBlockViolations([]);
    setSmartToolBlockMessage(`${toolLabel} blocked by snapshot preflight (${preflightStatus}).`);
    setApiErrorDetails(
      buildSyntheticApiErrorDetails(endpoint, 200, preflightRoot, {
        method: "GET",
        requestPayload,
        requestDebug,
      }),
    );
    showSnapshotNotReadyModal(toolLabel, preflightStatus, preflightErrors);
    return false;
  }

  async function runSmartToolPreflight(toolLabel: string): Promise<PreflightSmartToolResult | null> {
    setLastValidatePassed(false);
    const localValidation = runLocalValidate();
    setValidationMessage(localValidation.message);
    if (!localValidation.ok) {
      setRuntimeError(localValidation.message);
      return null;
    }

    const base = normalizeApiBase(apiBase);
    const resolvedSnapshotId = await ensureSmartToolSnapshotId(base);

    const validatePayload: DeckValidateRequestPayload = {
      db_snapshot_id: resolvedSnapshotId,
      raw_decklist_text: buildCommanderDecklistTextForApi(commander, deckText),
      format: "commander",
      profile_id: profileId.trim(),
      bracket_id: bracketId.trim(),
    };
    const commanderToken = commander.trim();
    if (commanderToken !== "") {
      validatePayload.commander = commanderToken;
    }

    const normalizedOverrides = normalizeNameOverrides(nameOverridesV1);
    if (normalizedOverrides.length > 0) {
      validatePayload.name_overrides_v1 = normalizedOverrides;
    }

    let validateResponse: Response;
    try {
      validateResponse = await fetch(`${base}/deck/validate`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: serializeRequestBodyForNetwork("/deck/validate", validatePayload),
      });
    } catch (error) {
      setIsApiReachable(false);
      const details = buildApiErrorDetails("/deck/validate", null, "", null, {
        method: "POST",
        requestPayload: validatePayload,
        error,
      });
      setApiErrorDetails(details);
      const message = error instanceof Error ? error.message : "Network request failed.";
      throw new Error(`Request failed for /deck/validate: ${message}`);
    }
    const validateText = await validateResponse.text();
    const validateParsed = safeParseJson(validateText);
    setIsApiReachable(true);

    if (!validateResponse.ok) {
      const details = buildApiErrorDetails("/deck/validate", validateResponse, validateText, validateParsed, {
        method: "POST",
        requestPayload: validatePayload,
      });
      setApiErrorDetails(details);
      throw new Error(`HTTP ${validateResponse.status} from /deck/validate`);
    }

    const validateRoot = (asRecord(validateParsed) ?? {}) as DeckValidateResponsePayload;
    setSmartToolValidateResponse(validateRoot);

    const unknowns = asValidateUnknownRows(validateRoot.unknowns);
    const violations = asValidateViolationRows(validateRoot.violations_v1);
    const status = asString(validateRoot.status);

    if (unknowns.length > 0 || violations.length > 0 || status !== "OK") {
      setSmartToolBlockUnknowns(unknowns);
      setSmartToolBlockViolations(violations);
      setSmartToolBlockMessage(
        `${toolLabel} blocked: resolve unknowns/violations via Add name override, then run the tool again.` +
          ` (validate status: ${status || "UNKNOWN"})`,
      );
      showValidateBlockedModal(toolLabel, status, unknowns, violations);
      const details = buildSyntheticApiErrorDetails("/deck/validate", 200, validateRoot, {
        method: "POST",
        requestPayload: validatePayload,
      });
      setApiErrorDetails(details);
      return null;
    }

    setSmartToolBlockUnknowns([]);
    setSmartToolBlockViolations([]);
    setSmartToolBlockMessage(null);
    setLastValidatePassed(true);

    const canonicalInput = asRecord(validateRoot.canonical_deck_input);
    return {
      snapshotId: resolvedSnapshotId,
      validateResponse: validateRoot,
      canonicalCommander: firstNonEmptyString(canonicalInput?.commander, commander) || commander,
      canonicalCards: asStringArray(canonicalInput?.cards),
    };
  }

  async function handleDeckTrimTool(): Promise<void> {
    setActiveTool("DECK_TRIM");
    setRunningSmartTrim(true);
    setLastTrimSucceeded(false);
    setRuntimeError(null);
    setApiErrorDetails(null);
    setDeckTrimResult(null);
    setPendingCutOrder([]);

    try {
      const preflight = await runSmartToolPreflight("Deck Trim");
      if (!preflight) {
        return;
      }

      const base = normalizeApiBase(apiBase);
      const buildCards = preflight.canonicalCards.length > 0 ? preflight.canonicalCards : deckCardsInPayloadOrder;
      const buildCommander = preflight.canonicalCommander.trim() || commander.trim();
      const buildPayload: BuildRequestPayload = {
        db_snapshot_id: preflight.snapshotId,
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
        format: "commander",
        commander: buildCommander,
        cards: buildCards,
        engine_patches_v0: [],
      };

      const requestedDeckHash = buildDeckHashFromCards(buildCommander, buildCards);
      const latestBuildDeckHash =
        requestPayload && buildResponse
          ? buildDeckHashFromCards(requestPayload.commander, requestPayload.cards)
          : "";
      const canReuseLatestBuild =
        Boolean(requestPayload && buildResponse) &&
        requestPayload!.db_snapshot_id.trim() === preflight.snapshotId &&
        requestPayload!.profile_id.trim() === profileId.trim() &&
        requestPayload!.bracket_id.trim() === bracketId.trim() &&
        latestBuildDeckHash === requestedDeckHash;

      let buildRoot: BuildResponsePayload;
      let buildRequestForState: BuildRequestPayload;
      let trimSource: "REUSED_BUILD" | "NEW_BUILD" = "NEW_BUILD";

      if (canReuseLatestBuild && buildResponse && requestPayload) {
        buildRoot = buildResponse;
        buildRequestForState = requestPayload;
        trimSource = "REUSED_BUILD";
        _phase46MarkBuildLatency(0, "reused");
      } else {
        let buildResponseRaw: Response;
        const _phase46BuildStartMs = (typeof performance !== "undefined" && performance.now) ? performance.now() : Date.now();
        try {
          buildResponseRaw = await fetch(`${base}/build`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: serializeRequestBodyForNetwork("/build", buildPayload),
          });
          const _phase46BuildEndMs = (typeof performance !== "undefined" && performance.now) ? performance.now() : Date.now();
          _phase46MarkBuildLatency(_phase46BuildEndMs - _phase46BuildStartMs, "fresh");
        } catch (error) {
          const details = buildApiErrorDetails("/build", null, "", null, {
            method: "POST",
            requestPayload: buildPayload,
            error,
          });
          setApiErrorDetails(details);
          throw new Error(formatApiErrorMessage(details));
        }

        const buildResponseText = await buildResponseRaw.text();
        const buildResponseParsed = safeParseJson(buildResponseText);
        if (!buildResponseRaw.ok) {
          const details = buildApiErrorDetails("/build", buildResponseRaw, buildResponseText, buildResponseParsed, {
            method: "POST",
            requestPayload: buildPayload,
          });
          setApiErrorDetails(details);
          throw new Error(formatApiErrorMessage(details));
        }

        buildRoot = (asRecord(buildResponseParsed) ?? {}) as BuildResponsePayload;
        buildRequestForState = buildPayload;
      }

      setBuildResponse(buildRoot);
      setRequestPayload(buildRequestForState);

      const result = asRecord(buildRoot.result);
      const deckStatus = (firstNonEmptyString(buildRoot.deck_status, result?.deck_status) || "").toUpperCase();
      const deckSizeTotal = firstNumber(buildRoot.deck_size_total, result?.deck_size_total);
      const cutOrder = (() => {
        const explicitCutOrder = asStringArray(buildRoot.cut_order ?? result?.cut_order);
        if (explicitCutOrder.length > 0) {
          return explicitCutOrder;
        }
        return asStringArray(buildRoot.cards_to_cut ?? result?.cards_to_cut);
      })();
      const cardsToCutRaw = firstNumber(
        buildRoot.cards_to_cut_count,
        result?.cards_to_cut_count,
        buildRoot.cards_to_cut,
        result?.cards_to_cut,
      );

      const isDeckAtOrBelowTarget =
        (deckSizeTotal !== null && deckSizeTotal <= DEFAULT_COMPLETE_TARGET_DECK_SIZE) ||
        deckStatus === "EXACT_100" ||
        deckStatus === "UNDER_100";
      if (isDeckAtOrBelowTarget) {
        setDeckTrimResult({
          status: deckStatus || "OK",
          cards_to_cut: 0,
          cut_order: [],
          can_apply: false,
          message: "Deck is already <=100",
          source: trimSource,
        });
        setLastTrimSucceeded(true);
        setValidationMessage("Deck is already <=100");
        return;
      }

      if (cutOrder.length === 0) {
        const message = "Deck Trim requires cut_order/cards_to_cut from build, but none was returned.";
        setDeckTrimResult({
          status: deckStatus || "OVER_100",
          cards_to_cut: 0,
          cut_order: [],
          can_apply: false,
          message,
          source: trimSource,
        });
        setRuntimeError(message);
        return;
      }

      const boundedCardsToCut =
        cardsToCutRaw === null ? cutOrder.length : clampInteger(Math.trunc(cardsToCutRaw), 0, cutOrder.length);
      const effectiveCutOrder = cutOrder.slice(0, boundedCardsToCut);
      if (effectiveCutOrder.length === 0) {
        const message = "Deck Trim returned zero cuts to apply.";
        setDeckTrimResult({
          status: deckStatus || "OVER_100",
          cards_to_cut: 0,
          cut_order: [],
          can_apply: false,
          message,
          source: trimSource,
        });
        setRuntimeError(message);
        return;
      }

      const projectedDeckText = applyCutOrderToCardsInput(
        parseDecklistInput(deckText),
        effectiveCutOrder,
        buildCommander,
      );
      const projectedCards = expandDecklistRowsInInputOrder(parseDecklistInput(projectedDeckText));

      setPendingCutOrder(effectiveCutOrder);
      setDeckTrimResult({
        status: deckStatus,
        cards_to_cut: effectiveCutOrder.length,
        cut_order: effectiveCutOrder,
        can_apply: true,
        source: trimSource,
      });
      setLastTrimSucceeded(true);

      persistSmartToolHistory(
        "Deck Trim",
        buildCards,
        {
          status: deckStatus,
          cards_to_cut: effectiveCutOrder.length,
          cut_order_count: effectiveCutOrder.length,
          source: trimSource,
        },
        "SMART_TOOL_DECK_TRIM",
        preflight.snapshotId,
        buildCommander,
        {
          toolType: "deck_trim",
          inputDeckText: deckText,
          outputDeckText: projectedDeckText,
          inputCards: buildCards,
          outputCards: projectedCards,
          summaryCounts: {
            cards_to_cut: effectiveCutOrder.length,
            cut_order_count: effectiveCutOrder.length,
          },
        },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown Deck Trim runtime error";
      setRuntimeError(message);
      setLastTrimSucceeded(false);
    } finally {
      setRunningSmartTrim(false);
    }
  }

  async function handleCopyDeckTrimCuts(): Promise<void> {
    if (pendingCutOrder.length === 0) {
      setRuntimeError("No Deck Trim cuts available to copy.");
      return;
    }

    try {
      await copyTextToClipboard(collapseCardNamesInInputOrder(pendingCutOrder));
      setToastMessage("Copied cuts.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to copy cuts.";
      setRuntimeError(message);
    }
  }

  function handleDismissDeckTrimResult(): void {
    setPendingCutOrder([]);
    setDeckTrimResult(null);
  }

  function handleApplyDeckTrimCuts(): void {
    if (pendingCutOrder.length === 0) {
      setRuntimeError("No Deck Trim cuts available to apply.");
      return;
    }

    const nextCardsInput = applyCutOrderToCardsInput(parsedDeckRows, pendingCutOrder, commander);
    const nextCards = expandDecklistRowsInInputOrder(parseDecklistInput(nextCardsInput));

    if (normalizeDeckTextForHash(nextCardsInput) === normalizeDeckTextForHash(deckText)) {
      const message = "Deck Trim apply produced no changes.";
      const details = buildSyntheticApiErrorDetails(
        "/build",
        200,
        {
          status: deckTrimResult?.status || "OVER_100",
          code: "DECK_TRIM_APPLY_NO_CHANGE",
          message,
          cut_order: pendingCutOrder,
        },
        {
          method: "POST",
          requestPayload: {
            action: "apply_deck_trim",
            cut_order: pendingCutOrder,
          },
        },
      );
      setApiErrorDetails(details);
      setRuntimeError(message);
      setToastMessage("Deck Trim apply failed");
      return;
    }

    applyDeckTextAndResolveHints(nextCardsInput, commander, snapshotId);
    setValidationMessage(`Applied ${pendingCutOrder.length} cut(s) from cut_order.`);
    setPendingCutOrder([]);
    setDeckTrimResult(null);

    persistSmartToolHistory(
      "Deck Trim Apply",
      deckCardsInPayloadOrder,
      {
        cuts_applied: pendingCutOrder.length,
      },
      "SMART_TOOL_DECK_TRIM_APPLY",
      snapshotId,
      commander,
      {
        toolType: "deck_trim",
        inputDeckText: deckText,
        outputDeckText: nextCardsInput,
        inputCards: deckCardsInPayloadOrder,
        outputCards: nextCards,
        summaryCounts: {
          cuts_applied: pendingCutOrder.length,
          removed_cards: Math.max(0, deckCardsInPayloadOrder.length - nextCards.length),
        },
      },
    );
  }

  async function handleManaTuneTool(): Promise<void> {
    setLastValidatePassed(false);
    setLastSmokeSucceeded(false);
    // v1.4 Stage 1: pre-Complete snapshot now lives in
    // preCompleteDeckTextRef, captured at handleUnifiedCompleteDeck
    // entry (synchronous, BEFORE any state updates). v1.3's setState
    // here was scheduled after several other render-triggering setters
    // (setCompletionResult(null), etc.) and could end up reflecting
    // state mutated by the subsequent handleApplyCompletedDecklist's
    // USER_EDIT_DECK_TEXT dispatch — yielding a near-empty diff.
    const requestId = completionRequestIdRef.current + 1;
    completionRequestIdRef.current = requestId;

    setRunningSmartComplete(true);
    setRuntimeError(null);
    setApiErrorDetails(null);
    // Phase 4.14.1: synchronous ref clears alongside the React state setters
    // so handleUnifiedCompleteDeck observes the cleared values without
    // depending on the render→useEffect chain (HARD #15 additive only).
    completionErrorRef.current = null;
    setCompletionError(null);
    completionResultRef.current = null;
    setCompletionResult(null);
    setIsCompletionBlockedModalOpen(false);
    setIsSnapshotNotReadyModalOpen(false);
    setSnapshotNotReadyToolLabel("Complete to 100");
    setSnapshotNotReadyStatus("");
    setSnapshotNotReadyErrors([]);
    setCompletionBlockedUnknowns([]);
    setCompletionBlockedViolations([]);
    setCompletionBlockedStatus("");

    try {
      const localValidation = runLocalValidate();
      setValidationMessage(localValidation.message);
      if (!localValidation.ok) {
        if (completionRequestIdRef.current !== requestId) {
          return;
        }
        setCompletionError(localValidation.message);
        setRuntimeError(localValidation.message);
        return;
      }

      const base = normalizeApiBase(apiBase);
      const resolvedSnapshotId = await ensureSmartToolSnapshotId(base);
      if (completionRequestIdRef.current !== requestId) {
        return;
      }

      const validatePayload: DeckValidateRequestPayload = {
        db_snapshot_id: resolvedSnapshotId,
        raw_decklist_text: buildCommanderDecklistTextForApi(commander, deckText),
        format: "commander",
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
      };
      const commanderToken = commander.trim();
      if (commanderToken !== "") {
        validatePayload.commander = commanderToken;
      }

      const normalizedOverrides = normalizeNameOverrides(nameOverridesV1);
      if (normalizedOverrides.length > 0) {
        validatePayload.name_overrides_v1 = normalizedOverrides;
      }

      let validateResponse: Response;
      try {
        validateResponse = await fetch(`${base}/deck/validate`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: serializeRequestBodyForNetwork("/deck/validate", validatePayload),
        });
      } catch (error) {
        if (completionRequestIdRef.current !== requestId) {
          return;
        }
        setIsApiReachable(false);
        const details = buildApiErrorDetails("/deck/validate", null, "", null, {
          method: "POST",
          requestPayload: validatePayload,
          error,
        });
        setApiErrorDetails(details);
        const message = error instanceof Error ? error.message : "Network request failed.";
        throw new Error(`Request failed for /deck/validate: ${message}`);
      }
      const validateText = await validateResponse.text();
      const validateParsed = safeParseJson(validateText);
      setIsApiReachable(true);
      if (completionRequestIdRef.current !== requestId) {
        return;
      }

      if (!validateResponse.ok) {
        const details = buildApiErrorDetails("/deck/validate", validateResponse, validateText, validateParsed, {
          method: "POST",
          requestPayload: validatePayload,
        });
        setApiErrorDetails(details);
        throw new Error(`HTTP ${validateResponse.status} from /deck/validate`);
      }

      const validateRoot = (asRecord(validateParsed) ?? {}) as DeckValidateResponsePayload;
      setSmartToolValidateResponse(validateRoot);
      const unknowns = asValidateUnknownRows(validateRoot.unknowns);
      const violations = asValidateViolationRows(validateRoot.violations_v1);
      const validateStatus = asString(validateRoot.status);

      if (unknowns.length > 0 || violations.length > 0 || validateStatus !== "OK") {
        showValidateBlockedModal("Complete to 100", validateStatus, unknowns, violations);
        const details = buildSyntheticApiErrorDetails("/deck/validate", 200, validateRoot, {
          method: "POST",
          requestPayload: validatePayload,
        });
        setApiErrorDetails(details);
        setCompletionError(`Complete to 100 blocked by validate (${validateStatus || "UNKNOWN"}).`);
        return;
      }

      setSmartToolBlockUnknowns([]);
      setSmartToolBlockViolations([]);
      setSmartToolBlockMessage(null);
      setLastValidatePassed(true);

      const preflightReady = await runSnapshotPreflightOrBlock("Complete to 100", base, resolvedSnapshotId, {
        api_base: base,
        snapshot_id: resolvedSnapshotId,
        stage: "before_complete_v1",
        tool: "COMPLETE_TO_100",
      });
      if (completionRequestIdRef.current !== requestId) {
        return;
      }
      if (!preflightReady) {
        setCompletionError("Complete to 100 blocked: snapshot not ready.");
        setValidationMessage("Complete to 100 blocked: snapshot not ready.");
        return;
      }

      const payload: DeckCompleteRequestPayload = {
        db_snapshot_id: resolvedSnapshotId,
        raw_decklist_text: buildCommanderDecklistTextForApi(commander, deckText),
        format: "commander",
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
        mulligan_model_id: mulliganModelId,
        target_deck_size: DEFAULT_COMPLETE_TARGET_DECK_SIZE,
        max_adds: clampInteger(completeMaxAdds, 1, MAX_COMPLETE_ADDS),
        allow_basic_lands: Boolean(completeAllowBasicLands),
        land_target_mode: completeLandTargetMode === "NONE" ? "NONE" : "AUTO",
      };

      if (commander.trim() !== "") {
        payload.commander = commander.trim();
      }

      if (normalizedOverrides.length > 0) {
        payload.name_overrides_v1 = normalizedOverrides;
      }

      const completeRequestDebug = {
        api_base: base,
        line_count: countNonEmptyTextLines(payload.raw_decklist_text),
        first120Chars: payload.raw_decklist_text.slice(0, 120),
      };
      const completeBodySent = serializeRequestBodyForNetwork("/deck/complete_v1", payload);

      let response: Response;
      try {
        response = await fetch(`${base}/deck/complete_v1`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: completeBodySent,
        });
      } catch (error) {
        if (completionRequestIdRef.current !== requestId) {
          return;
        }
        const details = buildApiErrorDetails("/deck/complete_v1", null, "", null, {
          method: "POST",
          requestPayload: completeBodySent,
          requestDebug: completeRequestDebug,
          error,
        });
        setApiErrorDetails(details);
        const message = error instanceof Error ? error.message : "Network request failed.";
        throw new Error(`Request failed for /deck/complete_v1: ${message}`);
      }
      const text = await response.text();
      const parsed = safeParseJson(text);
      if (completionRequestIdRef.current !== requestId) {
        return;
      }

      if (!response.ok) {
        const details = buildApiErrorDetails("/deck/complete_v1", response, text, parsed, {
          method: "POST",
          requestPayload: completeBodySent,
          requestDebug: completeRequestDebug,
        });
        setApiErrorDetails(details);
        throw new Error(`HTTP ${response.status} from /deck/complete_v1`);
      }

      const root = (asRecord(parsed) ?? {}) as DeckCompleteResponseV1;
      // Phase 4.14.1: synchronous ref write alongside the React state
      // setter so handleUnifiedCompleteDeck (the await-then-read consumer)
      // sees the correct value without depending on React's
      // render→useEffect→ref-mirror chain having flushed. User-visible
      // toolbar behavior unchanged (HARD #15 — additive instrumentation
      // only). Same pattern applied to setCompletionError below.
      completionResultRef.current = root;
      setCompletionResult(root);
      completionErrorRef.current = null;
      setCompletionError(null);

      const canonicalInput = asRecord(validateRoot.canonical_deck_input);
      const canonicalCards = asStringArray(canonicalInput?.cards);
      const canonicalCommanderForHistory = firstNonEmptyString(canonicalInput?.commander, commander) || commander;
      const completionDeckText = asString(root.completed_decklist_text_v1);
      const completionOutputCards =
        completionDeckText.trim() === ""
          ? canonicalCards.length > 0
            ? canonicalCards
            : deckCardsInPayloadOrder
          : expandDecklistRowsInInputOrder(parseDecklistInput(completionDeckText));
      const addedRowsCount = asDeckCompleteAddedRows(root.added_cards_v1).length;

      persistSmartToolHistory(
        "Complete to 100",
        canonicalCards.length > 0 ? canonicalCards : deckCardsInPayloadOrder,
        {
          status: asString(root.status),
          target_deck_size: DEFAULT_COMPLETE_TARGET_DECK_SIZE,
          added_cards_count: addedRowsCount,
        },
        "SMART_TOOL_COMPLETE_TO_100",
        resolvedSnapshotId,
        canonicalCommanderForHistory,
        {
          toolType: "complete_to_100",
          inputDeckText: deckText,
          outputDeckText: completionDeckText || deckText,
          inputCards: canonicalCards.length > 0 ? canonicalCards : deckCardsInPayloadOrder,
          outputCards: completionOutputCards,
          summaryCounts: {
            added_cards_count: addedRowsCount,
            lands_added_count: Math.max(0, Math.trunc(firstNumber(root.lands_added_count) || 0)),
          },
        },
      );
      setLastSmokeSucceeded(true);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown Complete to 100 runtime error";
      if (completionRequestIdRef.current !== requestId) {
        return;
      }
      setRuntimeError(message);
      // Phase 4.14.1: synchronous ref write so handleUnifiedCompleteDeck
      // sees the error immediately after await resolves.
      completionErrorRef.current = message;
      setCompletionError(message);
      setCompletionResult(null);
      setLastSmokeSucceeded(false);
    } finally {
      if (completionRequestIdRef.current === requestId) {
        setRunningSmartComplete(false);
      }
    }
  }

  function handleApplyCompletedDecklist(): void {
    // v1.2 Stage 3: clear stale apply-time error state (incl. the
    // "Apply Complete blocked: completed deck has N lines" message at the
    // strict-larger gate below) before re-validating. Without this reset,
    // a prior failed attempt's red-bar message persists across a fresh
    // Apply attempt, misleading the user into thinking the new attempt
    // already failed.
    setCompletionError(null);
    setRuntimeError(null);
    setApiErrorDetails(null);

    const inputDeckText = deckText;
    const inputCards = deckCardsInPayloadOrder;
    const currentLineCount = countNonEmptyTextLines(deckText);
    const completionStatus = asString(completionResult?.status) || "UNKNOWN";
    const completedDecklistText = asString(completionResult?.completed_decklist_text_v1);
    if (completedDecklistText.trim() === "") {
      const message = "No completed_decklist_text_v1 available to apply.";
      const details = buildSyntheticApiErrorDetails(
        "/deck/complete_v1",
        200,
        {
          status: completionStatus,
          code: "APPLY_COMPLETE_MISSING_DECKLIST_TEXT",
          message,
          completion_result_v1: completionResult,
        },
        {
          method: "POST",
          requestPayload: {
            action: "apply_complete",
            current_line_count: currentLineCount,
          },
        },
      );
      setApiErrorDetails(details);
      setCompletionError(message);
      setRuntimeError(message);
      setToastMessage("Apply Complete failed");
      return;
    }

    const parsedDecklist = parseCompletedDecklistText(completedDecklistText);
    if (parsedDecklist.cards.length === 0) {
      const message = "Completed decklist did not include any deck cards to apply.";
      const details = buildSyntheticApiErrorDetails(
        "/deck/complete_v1",
        200,
        {
          status: completionStatus,
          code: "APPLY_COMPLETE_EMPTY_DECK_ROWS",
          message,
          completion_result_v1: completionResult,
        },
        {
          method: "POST",
          requestPayload: {
            action: "apply_complete",
            current_line_count: currentLineCount,
            completed_decklist_text_v1: completedDecklistText,
          },
        },
      );
      setApiErrorDetails(details);
      setCompletionError(message);
      setRuntimeError(message);
      setToastMessage("Apply Complete failed");
      return;
    }

    const completedRows = parseDecklistInput(completedDecklistText);
    const completedLineCount = countNonEmptyTextLines(completedDecklistText);
    if (completedLineCount <= currentLineCount) {
      const message =
        `Apply Complete blocked: completed deck has ${completedLineCount} lines and must exceed current ${currentLineCount} lines.`;
      const details = buildSyntheticApiErrorDetails(
        "/deck/complete_v1",
        200,
        {
          status: completionStatus,
          code: "APPLY_COMPLETE_NOT_STRICTLY_LARGER",
          message,
          current_line_count: currentLineCount,
          completed_line_count: completedLineCount,
          completion_result_v1: completionResult,
        },
        {
          method: "POST",
          requestPayload: {
            action: "apply_complete",
            current_line_count: currentLineCount,
            completed_line_count: completedLineCount,
            completed_decklist_text_v1: completedDecklistText,
          },
        },
      );
      setApiErrorDetails(details);
      setCompletionError(message);
      setRuntimeError(message);
      setToastMessage("Apply Complete failed");
      return;
    }

    const nextCommander = parsedDecklist.commander || commander;
    applyDeckText(completedDecklistText, "apply_complete");
    if (nextCommander.trim() !== "") {
      setCommander(nextCommander.trim());
    }
    setBuildResponse(null);

    void resolveDeckRowsAndStoreHints(completedRows, snapshotId, nextCommander).catch((error) => {
      const message = error instanceof Error ? error.message : "Failed to resolve deck names for preview.";
      setRuntimeError(message);
    });

    setCompletionResult(null);
    setCompletionError(null);
    setValidationMessage("Applied completed decklist from Complete to 100.");

    persistSmartToolHistory(
      "Complete to 100 Apply",
      parsedDecklist.cards,
      {
        applied_cards: parsedDecklist.cards.length,
      },
      "SMART_TOOL_COMPLETE_APPLY",
      snapshotId,
      nextCommander,
      {
        toolType: "COMPLETE_TO_100_APPLY",
        inputDeckText,
        outputDeckText: completedDecklistText,
        inputCards,
        outputCards: parsedDecklist.cards,
        summaryCounts: {
          applied_cards: parsedDecklist.cards.length,
        },
      },
    );
  }

  // Phase 4.14 Stage 1: unified "Complete deck" handler. Replaces the
  // two-button Complete-to-100 + Apply-Complete flow (single click runs
  // /deck/complete_v1 then auto-applies the result). Dispatches reducer
  // COMPLETE_PENDING / COMPLETE_SUCCESS / COMPLETE_ERROR actions per
  // Phase 4.14 contract. Existing handleManaTuneTool +
  // handleApplyCompletedDecklist stay BYTE-IDENTICAL (HARD #15) — this
  // wrapper invokes them in sequence and observes their outcomes via
  // completionResult / completionError state.
  async function handleUnifiedCompleteDeck(): Promise<void> {
    if (deckState.completePending) return;
    // v1.4 Stage 1: capture the pre-Complete deck text IMMEDIATELY at
    // click-handler entry — BEFORE any state updates, dispatches, or
    // fetches fire. useRef.current = ... is synchronous; the diff
    // useMemo reads this when /deck/complete_v1 resolves and
    // completedDecklistText changes. Without this, v1.3's useState
    // setter could be overwritten by the subsequent
    // handleApplyCompletedDecklist USER_EDIT_DECK_TEXT dispatch,
    // yielding a near-empty diff (only commander entry).
    //
    // v1.5 Stage 2: prepend Commander+name+Deck banner so the snapshot
    // shape matches the engine response's `completed_decklist_text_v1`
    // (which always emits Commander/Deck sections). Without the
    // prepend, the diff would see the commander as a "new" addition
    // in the response (yielding an off-by-one in the entry count and
    // a misleading commander entry in AddedCardsPanel). The new
    // banner-strip extensions in lib/deckDiff.ts (Stage 1) ensure the
    // synthesized "Commander\n1 ...\nDeck\n" prefix gets stripped on
    // both sides of the diff.
    const trimmedCommander = commander.trim();
    preCompleteDeckTextRef.current =
      trimmedCommander !== ""
        ? `Commander\n1 ${trimmedCommander}\nDeck\n${deckText}`
        : deckText;
    // v1.2 Stage 3: also clear at the unified-entry boundary so the prior
    // "Apply Complete blocked" message doesn't persist while the new
    // /deck/complete_v1 request is in flight.
    setCompletionError(null);
    setRuntimeError(null);
    dispatchDeckAction({ type: "COMPLETE_PENDING" });
    try {
      await handleManaTuneTool();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Complete failed";
      dispatchDeckAction({ type: "COMPLETE_ERROR", error: message });
      return;
    }
    // handleManaTuneTool sets completionResult on success; if it surfaced
    // an error via setCompletionError, treat that as the failure path.
    // Read the current refs synchronously after await — React's setters
    // queued during handleManaTuneTool have flushed by now.
    const finalCompletedText = completionResultRef.current?.completed_decklist_text_v1;
    const finalCompletionError = completionErrorRef.current;
    if (finalCompletionError) {
      dispatchDeckAction({ type: "COMPLETE_ERROR", error: finalCompletionError });
      return;
    }
    if (typeof finalCompletedText === "string" && finalCompletedText.trim() !== "") {
      // v1.6.3 Stage 2 — replace immediate-apply (handleApplyCompletedDecklist
      // + COMPLETE_SUCCESS, both of which mutated deckText to the full
      // completed text) with stage-then-review: dispatch STAGE_PROPOSED_ADDS
      // with the engine's added_cards_v1 array. User reviews per-card
      // accept/reject in AddedCardsPanel; APPLY_ACCEPTED_ADDS commits
      // the accepted subset. Deck only mutates on explicit apply per
      // the v1.6.3 spec. handleApplyCompletedDecklist + COMPLETE_SUCCESS
      // handlers BYTE-IDENTICAL — just no longer called from this path.
      const addedFromEngine = completionResultRef.current?.added_cards_v1;
      const stagedAdds: Array<{ card_name: string; reasons?: ReadonlyArray<string> }> = [];
      if (Array.isArray(addedFromEngine)) {
        for (const row of addedFromEngine) {
          if (!row || typeof row !== "object") continue;
          const rec = row as { name?: unknown; reasons_v1?: unknown };
          const name = typeof rec.name === "string" ? rec.name.trim() : "";
          if (name === "") continue;
          const reasons = Array.isArray(rec.reasons_v1)
            ? rec.reasons_v1.filter((r): r is string => typeof r === "string")
            : [];
          stagedAdds.push({ card_name: name, reasons });
        }
      }
      dispatchDeckAction({ type: "STAGE_PROPOSED_ADDS", adds: stagedAdds });
    } else {
      // Engine returned no completed text — surface as soft error.
      dispatchDeckAction({ type: "COMPLETE_ERROR", error: "Complete returned no decklist." });
    }
  }

  // v1.1 Stage 2: unified "Upgrade Deck" handler. Top-level entry point for
  // /deck/tune_v1; invokes the existing handlePowerTuneTool flow (HARD #13
  // — existing TOOLS-panel "Run Power Tune" button BYTE-IDENTICAL) and
  // additionally dispatches reducer UPGRADE_PENDING/SUCCESS/ERROR so the
  // top-level numbered toolbar surfaces state + the new UpgradeSuggestionsList
  // panel reads recommended_swaps_v1 from reducer state.
  async function handleUnifiedUpgradeDeck(): Promise<void> {
    if (deckState.upgradePending) return;
    dispatchDeckAction({ type: "UPGRADE_PENDING" });
    try {
      await handlePowerTuneTool();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Upgrade failed";
      dispatchDeckAction({ type: "UPGRADE_ERROR", error: message });
      return;
    }
    // handlePowerTuneTool sets deckTuneResponse on success OR setRuntimeError
    // on failure. Read the result via deckTuneResponseRef.current (mirrored
    // alongside the React state setter at line 3684 — same synchronous-write
    // pattern Phase 4.14.1 added for handleManaTuneTool to avoid the
    // render→useEffect stale-closure race).
    const tuneRoot = deckTuneResponseRef.current;
    const swaps = asDeckTuneSwapRows(tuneRoot?.recommended_swaps_v1);
    dispatchDeckAction({
      type: "UPGRADE_SUCCESS",
      suggestions: (Array.isArray(swaps) ? swaps : []) as ReadonlyArray<UpgradeSwapSuggestion>,
      nowIso: new Date().toISOString(),
    });
  }

  async function handlePowerTuneTool(): Promise<void> {
    setActiveTool("POWER_TUNE");
    setRunningSmartTune(true);
    setLastTuneSucceeded(false);
    setRuntimeError(null);
    setApiErrorDetails(null);
    // v1.1 Stage 2: clear the ref synchronously alongside the React state
    // setter — symmetric with the success-path write below.
    deckTuneResponseRef.current = null;
    setDeckTuneResponse(null);

    try {
      const preflight = await runSmartToolPreflight("Power Tune");
      if (!preflight) {
        return;
      }

      const base = normalizeApiBase(apiBase);
      const payload: DeckTuneRequestPayload = {
        db_snapshot_id: preflight.snapshotId,
        raw_decklist_text: buildCommanderDecklistTextForApi(commander, deckText),
        format: "commander",
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
        mulligan_model_id: mulliganModelId,
        max_swaps: clampInteger(maxSwaps, 1, MAX_SWAPS_LIMIT),
      };

      const commanderToken = commander.trim();
      if (commanderToken !== "") {
        payload.commander = commanderToken;
      }

      const normalizedOverrides = normalizeNameOverrides(nameOverridesV1);
      if (normalizedOverrides.length > 0) {
        payload.name_overrides_v1 = normalizedOverrides;
      }

      let response: Response;
      try {
        response = await fetch(`${base}/deck/tune_v1`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: serializeRequestBodyForNetwork("/deck/tune_v1", payload),
        });
      } catch (error) {
        const details = buildApiErrorDetails("/deck/tune_v1", null, "", null, {
          method: "POST",
          requestPayload: payload,
          error,
        });
        setApiErrorDetails(details);
        const message = error instanceof Error ? error.message : "Network request failed.";
        throw new Error(`Request failed for /deck/tune_v1: ${message}`);
      }
      const text = await response.text();
      const parsed = safeParseJson(text);

      if (!response.ok) {
        const details = buildApiErrorDetails("/deck/tune_v1", response, text, parsed, {
          method: "POST",
          requestPayload: payload,
        });
        setApiErrorDetails(details);
        throw new Error(`HTTP ${response.status} from /deck/tune_v1`);
      }

      const root = (asRecord(parsed) ?? {}) as DeckTuneResponseV1;
      // v1.1 Stage 2: synchronous ref write alongside the React state setter
      // — same pattern Phase 4.14.1 established for completionResultRef.
      // User-visible toolbar behavior unchanged (HARD #15 — additive
      // instrumentation only).
      deckTuneResponseRef.current = root;
      setDeckTuneResponse(root);
      setLastTuneSucceeded(true);
      setTuneSourceCards(preflight.canonicalCards.length > 0 ? preflight.canonicalCards : deckCardsInPayloadOrder);
      setTuneSourceCommander(preflight.canonicalCommander);

      const tuneInputCards = preflight.canonicalCards.length > 0 ? preflight.canonicalCards : deckCardsInPayloadOrder;
      const recommendedSwaps = asDeckTuneSwapRows(root.recommended_swaps_v1);
      const projectedOutputCards = applySwapsDeterministically(tuneInputCards, recommendedSwaps, recommendedSwaps.length);

      persistSmartToolHistory(
        "Power Tune",
        tuneInputCards,
        {
          status: asString(root.status),
          swap_count: recommendedSwaps.length,
        },
        "SMART_TOOL_POWER_TUNE",
        preflight.snapshotId,
        preflight.canonicalCommander,
        {
          toolType: "power_tune",
          inputDeckText: deckText,
          outputDeckText: collapseCardNamesInInputOrder(projectedOutputCards),
          inputCards: tuneInputCards,
          outputCards: projectedOutputCards,
          summaryCounts: {
            swap_count: recommendedSwaps.length,
          },
        },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown Power Tune runtime error";
      setRuntimeError(message);
      setLastTuneSucceeded(false);
      setDeckTuneResponse(null);
    } finally {
      setRunningSmartTune(false);
    }
  }

  async function handleCopyPowerTuneSwaps(): Promise<void> {
    const swaps = asDeckTuneSwapRows(deckTuneResponse?.recommended_swaps_v1);
    if (swaps.length === 0) {
      setRuntimeError("No recommended swaps available to copy.");
      return;
    }

    const text = swaps
      .map((swap: DeckTuneSwapV1) => {
        const cutName = asString(swap.cut_name).trim() || "(no cut)";
        const addName = asString(swap.add_name).trim() || "(no add)";
        return `${cutName} -> ${addName}`;
      })
      .join("\n");

    try {
      await copyTextToClipboard(text);
      setToastMessage("Copied swaps.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to copy swaps.";
      setRuntimeError(message);
    }
  }

  function handleDismissPowerTuneResult(): void {
    setDeckTuneResponse(null);
    setTuneSourceCards([]);
    setTuneSourceCommander("");
  }

  function handleApplyPowerTuneSwaps(): void {
    const inputDeckText = deckText;
    const swaps = asDeckTuneSwapRows(deckTuneResponse?.recommended_swaps_v1);
    if (swaps.length === 0) {
      setRuntimeError("No recommended_swaps_v1 available to apply.");
      return;
    }

    const sourceCards = deckCardsInPayloadOrder.length > 0 ? deckCardsInPayloadOrder : tuneSourceCards;
    const tunedCards = applySwapsDeterministically(sourceCards, swaps, swaps.length);
    const tunedDeckText = collapseCardNamesInInputOrder(tunedCards);
    const nextCommander = tuneSourceCommander.trim() || commander;
    applyDeckTextAndResolveHints(tunedDeckText, nextCommander, snapshotId);
    setDeckTuneResponse(null);
    setTuneSourceCards([]);
    setTuneSourceCommander("");

    setValidationMessage(`Applied ${swaps.length} recommended swap(s) from Power Tune.`);

    persistSmartToolHistory(
      "Power Tune Apply",
      sourceCards,
      {
        swaps_applied: swaps.length,
      },
      "SMART_TOOL_POWER_APPLY",
      snapshotId,
      nextCommander,
      {
        toolType: "power_tune",
        inputDeckText,
        outputDeckText: tunedDeckText,
        inputCards: sourceCards,
        outputCards: tunedCards,
        summaryCounts: {
          swaps_applied: swaps.length,
        },
      },
    );
  }

  function handleSelectHistoryEntry(entryId: string) {
    const entry = historyEntries.find((row: BuildHistoryEntry) => row.id === entryId);
    if (!entry) {
      return;
    }

    setSelectedHistoryEntryId(entryId);
    setBuildResponse(entry.response_body);
    setRequestPayload(entry.request_payload);
    setCommander(entry.commander_input);
    setSnapshotId(entry.db_snapshot_id);
    setProfileId(entry.profile_id);
    setBracketId(entry.bracket_id);

    const restoredDeckText =
      typeof entry.output_deck_text === "string" && entry.output_deck_text.trim() !== ""
        ? entry.output_deck_text
        : buildCardsInputFromPayloadCards(entry.request_payload.cards);
    applyDeckText(restoredDeckText, "history_restore_entry");

    const restoredRows = parseDecklistInput(restoredDeckText);
    void resolveDeckRowsAndStoreHints(restoredRows, entry.db_snapshot_id, entry.commander_input).catch((error) => {
      const message = error instanceof Error ? error.message : "Failed to resolve deck names after loading history entry.";
      setRuntimeError(message);
    });

    setValidationMessage(`Loaded build from history @ ${entry.timestamp_label}.`);
    setRuntimeError(null);
  }

  async function handleCopyBlockedUnknowns(): Promise<void> {
    const unknownLines = completionBlockedUnknowns.map((unknown: ValidateUnknownRow, index: number) => {
      const code = asString(unknown.reason_code) || "UNKNOWN";
      const nameRaw = asString(unknown.name_raw);
      return `${index + 1}. ${code}${nameRaw ? ` :: ${nameRaw}` : ""}`;
    });
    const violationLines = completionBlockedViolations.map((violation: ValidateViolationRow, index: number) => {
      const code = asString(violation.code) || "VIOLATION";
      const message = asString(violation.message);
      return `${index + 1}. ${code}${message ? ` :: ${message}` : ""}`;
    });

    const copyText = [
      `Tool: ${completionBlockedToolLabel}`,
      `Validate status: ${completionBlockedStatus || "UNKNOWN"}`,
      "",
      "Unknowns:",
      unknownLines.length > 0 ? unknownLines.join("\n") : "(none)",
      "",
      "Violations:",
      violationLines.length > 0 ? violationLines.join("\n") : "(none)",
    ].join("\n");

    if (!navigator.clipboard || typeof navigator.clipboard.writeText !== "function") {
      setRuntimeError("Clipboard API unavailable in this browser context.");
      return;
    }

    try {
      await navigator.clipboard.writeText(copyText);
      setToastMessage("Copied unknowns/violations for reporting.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to copy unknowns.";
      setRuntimeError(message);
    }
  }

  async function handleCopyReleaseChecklistBugBundle(): Promise<void> {
    setReleaseChecklistCopyNotice(null);
    setReleaseChecklistCopyError(null);

    const parsedRequestDebug =
      apiErrorDetails && apiErrorDetails.requestDebugText.trim() !== ""
        ? asRecord(safeParseJson(apiErrorDetails.requestDebugText))
        : null;
    const requestDebugPayload =
      parsedRequestDebug ??
      (apiErrorDetails && apiErrorDetails.requestDebugText.trim() !== ""
        ? { raw_text_debug_only: apiErrorDetails.requestDebugText }
        : null);
    const requestBodySentText =
      apiErrorDetails === null
        ? ""
        : apiErrorDetails.requestBodyText || apiErrorDetails.requestPayloadText || "";

    const lastErrorPayload =
      apiErrorDetails === null
        ? null
        : {
            endpoint: apiErrorDetails.endpoint,
            status_code: apiErrorDetails.statusCode,
            method: apiErrorDetails.method || "POST",
            request_id: apiErrorDetails.requestId,
            request_body_sent_text: requestBodySentText,
            request_debug: requestDebugPayload,
            ...(apiErrorDetails.requestPayloadText.trim() !== "" && apiErrorDetails.requestPayloadText !== requestBodySentText
              ? { request_payload_text_debug_only: apiErrorDetails.requestPayloadText }
              : {}),
            response_payload_text: apiErrorDetails.responseJsonText || "",
            stack_trace: apiErrorDetails.stackTrace || "",
          };

    const bundle = {
      deckText,
      commander: commander.trim(),
      ids: {
        snapshot_id: snapshotId.trim(),
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
      },
      hashes: {
        build_hash: currentBuildHash || null,
        deck_hash: currentDeckHash,
      },
      checks: {
        api_reachable: isApiReachable,
        snapshot_selected: snapshotId.trim() !== "",
        validate_passes: lastValidatePassed,
        complete_works: lastSmokeSucceeded,
        tune_works: lastTuneSucceeded,
        trim_works: lastTrimSucceeded,
        hover_art_works: hoverArtReleaseMetrics.isReady,
        hover_art_resolved: hoverArtReleaseMetrics.resolvedCount,
        hover_art_target: hoverArtReleaseMetrics.targetCount,
        hover_art_percent: hoverArtReleaseMetrics.artReadyPercent,
        hover_art_basics_skipped: hoverArtReleaseMetrics.skippedBasicCount,
      },
      last_error_payload: lastErrorPayload,
      last_tool_request_response_summaries: lastToolRequestResponseSummaries,
    };

    try {
      await copyTextToClipboard(JSON.stringify(bundle, null, 2));
      setReleaseChecklistCopyNotice("Copied release bug bundle.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to copy release bug bundle.";
      setReleaseChecklistCopyError(message);
    }
  }

  function clearMissingImageForOracle(oracleIdRaw: string): void {
    const oracleId = oracleIdRaw.trim();
    if (oracleId === "") {
      return;
    }

    setMissingImageOracleIds((previous: Record<string, true>) => {
      if (!previous[oracleId]) {
        return previous;
      }
      const next = { ...previous };
      delete next[oracleId];
      return next;
    });
  }

  function markPreviewImageFailure(imageUrl: string, oracleIdRaw: string): void {
    setPreviewImageFailures((previous: Record<string, true>) => {
      if (previous[imageUrl]) {
        return previous;
      }
      return {
        ...previous,
        [imageUrl]: true,
      };
    });

    const oracleId = oracleIdRaw.trim();
    if (imageUrl === "" || oracleId === "") {
      return;
    }

    void (async () => {
      try {
        const response = await fetch(imageUrl, { method: "GET" });
        if (response.status !== 404) {
          if (response.ok) {
            clearMissingImageForOracle(oracleId);
          }
          return;
        }

        const text = await response.text();
        const parsed = safeParseJson(text);
        const root = asRecord(parsed);
        const errorCode = firstNonEmptyString(root?.code, root?.status) || "";
        if (errorCode !== "IMAGE_URI_MISSING") {
          return;
        }

        setMissingImageOracleIds((previous: Record<string, true>) => {
          if (previous[oracleId]) {
            return previous;
          }
          return {
            ...previous,
            [oracleId]: true,
          };
        });
      } catch {
        // Keep UI resilient if image diagnostics probe fails.
      }
    })();
  }

  function buildCardModalList(oracleId: string, oracleIdsContext?: string[]): string[] {
    const seen = new Set<string>();
    const nextList: string[] = [];

    for (const rawOracleId of oracleIdsContext || []) {
      const value = rawOracleId.trim();
      if (value === "" || seen.has(value)) {
        continue;
      }
      seen.add(value);
      nextList.push(value);
    }

    if (!seen.has(oracleId)) {
      nextList.unshift(oracleId);
    }

    return nextList;
  }

  function openCardModal(oracleIdRaw: string, oracleIdsContext?: string[]): void {
    const oracleId = oracleIdRaw.trim();
    if (oracleId === "") {
      return;
    }

    const nextList = buildCardModalList(oracleId, oracleIdsContext);
    const nextIndex = Math.max(0, nextList.indexOf(oracleId));

    setCardModalList(nextList);
    setCardModalIndex(nextIndex);
    setCardModalOracleId(nextList[nextIndex] || oracleId);
    setIsCardModalOpen(true);
  }

  function closeCardModal(): void {
    setIsCardModalOpen(false);
    setCardModalOracleId(null);
    setCardModalList([]);
    setCardModalIndex(0);
  }

  function goPrev(): void {
    if (cardModalList.length <= 1) {
      return;
    }

    const nextIndex = (cardModalIndex - 1 + cardModalList.length) % cardModalList.length;
    setCardModalIndex(nextIndex);
    setCardModalOracleId(cardModalList[nextIndex] || null);
  }

  function openDiagnosticsView(): void {
    window.location.hash = "#diagnostics";
  }

  function goNext(): void {
    if (cardModalList.length <= 1) {
      return;
    }

    const nextIndex = (cardModalIndex + 1) % cardModalList.length;
    setCardModalIndex(nextIndex);
    setCardModalOracleId(cardModalList[nextIndex] || null);
  }

  return (
    <div className="workspace-root">
      {/* v1.6.1 hotfix Stage 2: <LeftRail /> render hoisted to AppRouter
          as a persistent sibling so the hamburger persists across all
          7 ViewId routes. Removing it here doesn't change WorkspaceView's
          layout — LeftRail was a fixed-position overlay (z-modal) outside
          the normal flow, so the `.workspace-root` flex layout is BYTE-
          IDENTICAL. */}
      <main className="workspace-main-content">
        <div className="workspace-shell">
          <header className="workspace-header">
            {/* v1.7.5 UX polish: dropped the "MTG Engine Harness · Phase 2"
                kicker + the engineer-speak subtitle entirely. Users were
                seeing them on dev-server runs (which is how the app is
                served locally). The kicker provided zero user value; the
                subtitle now reads in plain English. */}
            <h1>Workspace</h1>
            <p className="workspace-subtitle">Build, tune, and playtest your Commander decks.</p>
          </header>

          {/* Phase 4 BUNDLE Integration (4.13): wire the orphaned panels.
              Mode tabs are PARENT-LEVEL (Workspace vs Seed Builder) per
              autonomous_repair_log #8; existing EDIT/TOOLS/ANALYZE remain
              children of Workspace mode. Panels render additively above the
              existing tree without modifying it.
              Phase 4.13.1 Stage 2: standalone "Build" button wired here so
              users have an explicit trigger that populates the dashboards. */}
          <section className="phase4-bundle-integration" aria-label="Phase 4 panels">
            {/* Phase 4.14 Stage 2: status pill — derived from reducer state.
                Variants per spec: source==="fallback" → neutral "No deck loaded";
                !isCompleted → info "{cmdr} · {N} cards · imported from {source}";
                isCompleted && !buildResponse → info "{cmdr} · {N} cards · ready to build";
                buildResponse → success "{cmdr} · {N} cards · built · sufficiency {summary}".

                v1.6 Stage 3: status pill is now ONE OF SEVERAL pills in a clean
                metric pill row (Commander / Card count / Bracket / Status).
                Status pill text composer (v1.4) BYTE-IDENTICAL per HARD #17 —
                only the surrounding layout + sibling pills are new. Deck name
                (commander) is the largest typography on the row. The dev-jargon
                HeaderChips block (snapshot: - / profile: - / ui_mode: DEV / etc.)
                is gated behind import.meta.env.DEV below. */}
            {(() => {
              const sufficiencyStatus = (() => {
                const summary = extractSufficiencySummary(buildResponse);
                if (!summary || typeof summary.status !== "string") return null;
                return summary.status;
              })();
              const pill = buildWorkspacePillText({
                source: deckState.source,
                isHydrated: deckState.isHydrated,
                isCompleted: deckState.isCompleted,
                hasBuildResponse: deckState.buildResponse !== null,
                commander: commander,
                cardCount: parsedDeckRows.length,
                sufficiencyStatus,
                // v1.1 Stage 3: surface upgrade suggestion count in the pill
                // when UPGRADE_SUCCESS populated reducer state.
                upgradeSuggestionsCount: deckState.upgradeSuggestions?.length ?? 0,
              });
              const cardCount = parsedDeckRows.length;
              const trimmedCommander = commander.trim();
              // v1.6.4 Stage 3: hide fallback Krenko values from the
              // metric pill row after USER_CLEAR_DECK. INITIAL_STATE
              // commander="Krenko, Mob Boss" + deckText=5-card fallback,
              // so the deckState reads non-empty even when source ===
              // "fallback". Source-gate the visible-deck display so the
              // empty/cleared state reads "No deck loaded" cleanly.
              const hasRealDeck = deckState.source !== "fallback";
              return (
                <div
                  className="mb-token-3"
                  role="banner"
                  aria-label="Deck metric pill header"
                  data-v16-stage="metric-pill-header"
                >
                  {/* v1.6: deck name = largest typography. Shows commander
                      verbatim when set, "No deck loaded" placeholder otherwise.
                      v1.6.2 Stage 2: × clear-deck button rendered next to the
                      deck name when state.source !== "fallback" (i.e. a real
                      deck is loaded). Dispatches USER_CLEAR_DECK which resets
                      to INITIAL_STATE values but keeps isHydrated:true so the
                      hydration useEffect doesn't immediately re-fire. */}
                  <div className="flex items-center gap-token-2 mb-token-2">
                    {/* v1.6.5 fix: when no deck is loaded, the empty-state
                        CTA card below owns the "No deck loaded" messaging.
                        Render a neutral section header here instead of a
                        duplicate "No deck loaded" string — eliminates the
                        duplication noted in UI overhaul punch list. */}
                    <h2
                      className="text-2xl font-bold text-text-primary truncate"
                      title={hasRealDeck && trimmedCommander !== "" ? trimmedCommander : "Workspace"}
                    >
                      {hasRealDeck && trimmedCommander !== "" ? trimmedCommander : "Workspace"}
                    </h2>
                    {deckState.source !== "fallback" ? (
                      <button
                        type="button"
                        onClick={() => { dispatchDeckAction({ type: "USER_CLEAR_DECK" }); setDeckTuneResponse(null); setUpgradeSnapshotRows(null); }}
                        aria-label="Clear deck"
                        title="Clear deck"
                        className="inline-flex items-center justify-center w-6 h-6 rounded-token-sm text-text-secondary hover:text-text-primary hover:bg-bg-elev-2 transition-colors duration-fast focus:outline-none focus-visible:shadow-focus-ring"
                        data-v162-stage="clear-deck-button"
                      >
                        <svg
                          width="16"
                          height="16"
                          viewBox="0 0 24 24"
                          fill="none"
                          stroke="currentColor"
                          strokeWidth="2.5"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          aria-hidden="true"
                        >
                          <line x1="6" y1="6" x2="18" y2="18" />
                          <line x1="18" y1="6" x2="6" y2="18" />
                        </svg>
                      </button>
                    ) : null}
                  </div>
                  <div
                    className="flex flex-wrap items-center gap-token-2"
                    role="group"
                    aria-label="Deck metric pills"
                  >
                    {hasRealDeck && trimmedCommander !== "" ? (
                      <Badge variant="info" aria-label={`Commander: ${trimmedCommander}`}>
                        Commander · {trimmedCommander}
                      </Badge>
                    ) : null}
                    {hasRealDeck ? (
                      <Badge variant="neutral" aria-label={`${cardCount} cards in deck`}>
                        {cardCount} card{cardCount === 1 ? "" : "s"}
                      </Badge>
                    ) : null}
                    {/* v1.7.5 UX polish: gate Bracket pill + status pill on
                        hasRealDeck. With no deck loaded, neither metadata
                        is meaningful and the empty-state CTA card below
                        already provides the "No deck loaded" status.
                        v1.7.6: Bracket pill is now an interactive
                        selector trigger — clicking opens a popover with
                        the five canonical brackets (B1-B5). Selecting one
                        invokes setBracketId AND triggers the local
                        violations filter in BracketViolationsBanner so
                        the banner self-hides when the user raises the
                        bracket past B2, addressing the user-visible gap
                        called out in the v1.7.5 release notes ("raise the
                        bracket to B3+ to allow combo strategies" — the
                        suggestion was previously unactionable). */}
                    {hasRealDeck && bracketId.trim() !== "" ? (
                      <div className="relative inline-flex">
                        <button
                          type="button"
                          data-v176-stage="bracket-selector-trigger"
                          aria-haspopup="menu"
                          aria-expanded={bracketSelectorOpen}
                          aria-label={`Bracket ${bracketId} — click to change`}
                          onClick={() => setBracketSelectorOpen((v) => !v)}
                          className={[
                            "inline-flex items-center px-token-2 py-token-1",
                            "text-xs font-medium rounded-token-sm border",
                            "bg-accent/15 text-accent border-accent/40",
                            "hover:bg-accent/25 focus:outline-none focus-visible:shadow-focus-ring",
                            "transition-colors duration-fast cursor-pointer",
                          ].join(" ")}
                        >
                          Bracket · {bracketId}
                          <span aria-hidden="true" className="ml-token-1">▾</span>
                        </button>
                        {bracketSelectorOpen ? (
                          <div
                            data-v176-stage="bracket-selector-menu"
                            role="menu"
                            aria-label="Choose deck bracket"
                            className={[
                              "absolute top-full left-0 mt-token-1 z-50",
                              "min-w-[260px] max-w-[320px]",
                              "bg-bg-elev-2 border border-glass-border",
                              "rounded-token-sm shadow-lg",
                              "py-token-1",
                            ].join(" ")}
                          >
                            {([
                              ["B1", "Exhibition", "No 2-card infinite combos"],
                              ["B2", "Core", "No infinite / game-winning combos"],
                              ["B3", "Upgraded", "Combos allowed but limited"],
                              ["B4", "Optimized", "Combos allowed — high power"],
                              ["B5", "cEDH", "Competitive — anything goes"],
                            ] as ReadonlyArray<readonly [string, string, string]>).map(
                              ([id, name, desc]) => {
                                const isActive = id === bracketId;
                                return (
                                  <button
                                    key={id}
                                    type="button"
                                    role="menuitemradio"
                                    aria-checked={isActive}
                                    onClick={() => {
                                      setBracketId(id);
                                      setBracketSelectorOpen(false);
                                    }}
                                    className={[
                                      "flex w-full items-baseline gap-token-2",
                                      "px-token-3 py-token-2 text-left",
                                      "hover:bg-bg-elev-3 focus:outline-none focus-visible:bg-bg-elev-3",
                                      isActive ? "bg-accent/10" : "",
                                    ].join(" ")}
                                  >
                                    <span className="font-mono text-sm font-bold text-text-primary">
                                      {id}
                                    </span>
                                    <span className="text-sm text-text-primary">
                                      {name}
                                    </span>
                                    <span className="ml-auto text-xs text-text-muted">
                                      {desc}
                                    </span>
                                  </button>
                                );
                              },
                            )}
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                    {hasRealDeck ? (
                      <Badge
                        variant={pill.variant === "success" ? "success" : pill.variant === "info" ? "info" : "neutral"}
                        aria-label="Workspace status"
                      >
                        {pill.text}
                      </Badge>
                    ) : null}
                  </div>
                </div>
              );
            })()}

            {/* v1.6 Stage 2: TWO visually-distinct groups separated by a
                vertical divider — Mode tabs (left) use toggle-state styling
                via workspace-mode-tab classes (existing CSS); Action buttons
                (right) use the Button primitive's "primary" variant so they
                read as primary CTAs, NOT mode toggles. Previously all five
                buttons used the same workspace-mode-tab class, making them
                visually indistinguishable. */}
            <div
              className="flex flex-wrap items-center gap-token-2 mb-token-2"
              data-v16-stage="toolbar-semantic-separation"
            >
              <div
                className="flex flex-wrap gap-token-2"
                role="tablist"
                aria-label="Page mode"
                data-v16-group="mode-tabs"
              >
                <button
                  type="button"
                  role="tab"
                  aria-selected={pageMode === "WORKSPACE"}
                  className={`workspace-mode-tab ${pageMode === "WORKSPACE" ? "workspace-mode-tab-active" : ""}`}
                  onClick={() => setPageMode("WORKSPACE")}
                >
                  Workspace
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={pageMode === "SEED_BUILDER"}
                  className={`workspace-mode-tab ${pageMode === "SEED_BUILDER" ? "workspace-mode-tab-active" : ""}`}
                  onClick={() => setPageMode("SEED_BUILDER")}
                >
                  Seed Builder
                </button>
              </div>
              {/* v1.6 Stage 2 separator: visible vertical divider between
                  Mode tabs and Action buttons. Hidden at flex-wrap widths
                  so vertical-stacked layout doesn't show a stray rule. */}
              <span
                className="hidden sm:inline-block h-8 w-px bg-glass-border mx-token-2"
                aria-hidden="true"
                data-v16-group="toolbar-divider"
              />
              <div
                className="flex flex-wrap items-center gap-token-2"
                role="group"
                aria-label="Deck actions"
                data-v16-group="action-buttons"
              >
                {/* Phase 4.14 Stage 1+2: numbered "1. Complete deck" → unified
                    Complete + Apply (single click). Tooltip text hardcoded per
                    spec — describes the action, not engine output, so Decision
                    10 doesn't restrict. v1.6 Stage 2: now uses Button primitive
                    variant=primary so it visually reads as a primary action,
                    NOT a mode toggle. */}
                <Button
                  variant="primary"
                  size="md"
                  disabled={deckState.source === "fallback" || deckState.completePending || commander.trim() === "" || deckText.trim() === ""}
                  onClick={() => {
                    void handleUnifiedCompleteDeck();
                  }}
                  title={deckState.source === "fallback" ? "Load a deck first." : "Asks the engine to fill the deck to 99 cards by adding suggested staples. ~3 sec."}
                  aria-label="Step 1: Complete deck — fills to 99 cards"
                >
                  {deckState.completePending ? "Completing…" : "1. Complete deck"}
                </Button>
                {deckState.completeError ? (
                  <span className="text-xs text-amber-300" role="status">
                    {deckState.completeError}
                  </span>
                ) : null}
                {/* Phase 4.14 Stage 2: numbered "2. Build" + tooltip. */}
                <Button
                  variant="primary"
                  size="md"
                  disabled={deckState.source === "fallback" || runningBuild || commander.trim() === "" || deckText.trim() === ""}
                  onClick={async () => {
                    if (runningBuild) return;
                    dispatchDeckAction({ type: "BUILD_PENDING" });
                    const body = buildBuildRequestBody({
                      snapshotId: snapshotId,
                      profileId: profileId,
                      bracketId: bracketId,
                      commander: commander,
                      cards: deckCardsInPayloadOrder,
                    });
                    const result = await callBuildEndpoint(apiBase, body);
                    if (result.ok) {
                      dispatchDeckAction({
                        type: "BUILD_SUCCESS",
                        response: result.response as Record<string, unknown>,
                      });
                    } else {
                      dispatchDeckAction({ type: "BUILD_ERROR", error: result.error });
                      setToastMessage(result.error);
                    }
                  }}
                  title="Runs the full sufficiency + recommendation pipeline. ~2 sec."
                  aria-label="Step 2: Build — runs sufficiency + recommendation pipeline"
                >
                  {runningBuild ? "Building…" : "2. Build"}
                </Button>
                {buildError ? (
                  <span className="text-xs text-amber-300" role="status">
                    {buildError}
                  </span>
                ) : null}
                {/* v1.1 Stage 2: numbered "3. Upgrade Deck" — top-level entry
                    point for /deck/tune_v1; existing TOOLS-panel "Run Power
                    Tune" button BYTE-IDENTICAL per HARD #13. Tooltip text
                    hardcoded per Decision 10 (describes the action, not
                    engine output). */}
                <Button
                  variant="primary"
                  size="md"
                  disabled={deckState.source === "fallback" || deckState.upgradePending || isAnyToolRunning || commander.trim() === "" || deckText.trim() === ""}
                  onClick={() => {
                    void handleUnifiedUpgradeDeck();
                  }}
                  title="Asks the engine to suggest swaps that would improve this deck — bracket-aware. Powered by /deck/tune_v1. ~3 sec."
                  aria-label="Step 3: Upgrade Deck — get swap suggestions"
                >
                  {deckState.upgradePending ? "Upgrading…" : "3. Upgrade Deck"}
                </Button>
                {deckState.upgradeError ? (
                  <span className="text-xs text-amber-300" role="status">
                    {deckState.upgradeError}
                  </span>
                ) : null}
                {/* Combos drawer toggle — opens the right-anchored
                    CombosDrawer wrapper around DeckCombosPanel. The
                    count badge sums detected + missing partner pairs
                    surfaced by /deck/complete_v1; renders no badge
                    when zero so the button stays uncluttered. */}
                <Button
                  variant="secondary"
                  size="md"
                  onClick={() => setCombosDrawerOpen((prev) => !prev)}
                  title="Show detected combos and missing-partner suggestions for this deck."
                  aria-label={combosDrawerOpen ? "Close Combos drawer" : "Open Combos drawer"}
                  aria-expanded={combosDrawerOpen}
                  data-testid="combos-drawer-toggle"
                >
                  <span className="inline-flex items-center gap-token-1">
                    Combos
                    {combosTotalCount > 0 ? (
                      <Badge variant="info" aria-label={`${combosTotalCount} combo entries`}>
                        {combosTotalCount}
                      </Badge>
                    ) : null}
                  </span>
                </Button>
              </div>
            </div>

            {pageMode === "WORKSPACE" ? (
              <div className="flex flex-col gap-token-3 mb-token-3">
                {/* v1.7.5 — bracket-combo violation banner FIRST so the
                    user sees the warning BEFORE they scroll through the
                    pending-review queue and accidentally apply additions
                    to an already-bracket-illegal deck. Self-hides on
                    legacy responses and B3+ decks (no violations). */}
                <BracketViolationsBanner
                  violations={
                    Array.isArray(completionResult?.violations_v1)
                      ? (completionResult?.violations_v1 as never)
                      : []
                  }
                  status={
                    typeof completionResult?.status === "string"
                      ? (completionResult?.status as string)
                      : ""
                  }
                  currentBracketId={bracketId}
                />

                {/* v1.1 Stage 1: AddedCardsPanel surfaces /deck/complete_v1's
                    added_cards_v1 prominently — was previously buried.
                    v1.3 Stage 1: falls back to text-diff `derivedAddedRows`
                    when the engine returns empty added_cards_v1 (gap in
                    some baseline-status paths). `addedRowsForPanel` is the
                    merged source — prefer engine (richer reasons) → diff.
                    v1.6.3 Stage 2: when deckState.pendingAdds has rows,
                    AddedCardsPanel renders pending-review mode instead.
                    v1.7 Stage 5 Deliverable A: when the engine returned
                    zero added_cards_v1 but the completed_decklist_text_v1
                    differs from input, switch to partial-completion mode
                    (introduced in v1.7 Stage 1) so the user gets a bulk
                    "Apply All" affordance instead of read-only rows. */}
                {deckState.pendingAdds.length > 0 ? (
                  <AddedCardsPanel
                    rows={[]}
                    gameChangers={gameChangerNameSet}
                    pendingAdds={deckState.pendingAdds}
                    onTogglePendingAdd={(index) =>
                      dispatchDeckAction({ type: "TOGGLE_PROPOSED_ADD", index })
                    }
                    onApplyAccepted={() =>
                      dispatchDeckAction({ type: "APPLY_ACCEPTED_ADDS" })
                    }
                    onAcceptAll={() => {
                      // Flip each rejected row's accepted=true by dispatching
                      // a TOGGLE for each row whose flag is currently false.
                      deckState.pendingAdds.forEach((row, i) => {
                        if (!row.accepted) {
                          dispatchDeckAction({ type: "TOGGLE_PROPOSED_ADD", index: i });
                        }
                      });
                    }}
                    onRejectAll={() => {
                      deckState.pendingAdds.forEach((row, i) => {
                        if (row.accepted) {
                          dispatchDeckAction({ type: "TOGGLE_PROPOSED_ADD", index: i });
                        }
                      });
                    }}
                    onDismissPending={() =>
                      dispatchDeckAction({ type: "DISMISS_PROPOSED_ADDS" })
                    }
                  />
                ) : completeAddedRows.length > 0 ? (
                  <AddedCardsPanel
                    rows={completeAddedRows}
                    source="engine"
                    gameChangers={gameChangerNameSet}
                  />
                ) : completedDecklistText !== "" && derivedAddedRows.length > 0 ? (
                  // v1.7 Stage 5 Deliverable A — partial-completion mode.
                  // AddedCardsPanel computes its own diff from deckText +
                  // completedDecklistText props and surfaces a bulk Apply
                  // All affordance. The onApplyAllPartial handler reuses
                  // the v1.6.3 STAGE_PROPOSED_ADDS + APPLY_ACCEPTED_ADDS
                  // pair so no new reducer action is introduced.
                  <AddedCardsPanel
                    rows={[]}
                    gameChangers={gameChangerNameSet}
                    deckText={preCompleteDeckTextRef.current ?? ""}
                    completedDecklistText={completedDecklistText}
                    onApplyAllPartial={() => {
                      const adds = derivedAddedRows
                        .map((row) => ({
                          card_name: typeof row.name === "string" ? row.name : "",
                        }))
                        .filter((row) => row.card_name !== "");
                      if (adds.length === 0) return;
                      dispatchDeckAction({ type: "STAGE_PROPOSED_ADDS", adds });
                      dispatchDeckAction({ type: "APPLY_ACCEPTED_ADDS" });
                    }}
                  />
                ) : null}

                {/* Combos surface moved to the right-anchored CombosDrawer
                    toggled by the workspace toolbar "Combos" button. The
                    drawer wraps DeckCombosPanel — the same panel that
                    previously rendered inline here. Props are sourced
                    from completionResult exactly as before; the only
                    layout change is the dismissible drawer surface. */}
                <CombosDrawer
                  open={combosDrawerOpen}
                  onOpenChange={setCombosDrawerOpen}
                  detected_combos_v1={detectedCombosForDrawer}
                  missing_partners_v1={missingPartnersForDrawer}
                />

                {/* Phase 2.1d — DeckThemesPanel surfaces deck_themes_v1
                    from /deck/complete_v1. Null-render contract preserved
                    for legacy responses and decks with no classified themes.
                    Reads completionResult directly — same source as the
                    combos panel above. */}
                <DeckThemesPanel
                  deck_themes_v1={
                    Array.isArray(completionResult?.deck_themes_v1)
                      ? (completionResult?.deck_themes_v1 as never)
                      : []
                  }
                />

                {/* v1.1 Stage 2: UpgradeSuggestionsList surfaces /deck/tune_v1's
                    recommended_swaps_v1 (separate data source from 4.8
                    SwapSuggestionsList — show both when both populated).
                    Reads upgradeSuggestions from reducer state; per-row
                    Apply dispatches USER_EDIT_DECK_TEXT.
                    v1.3 Stage 2: uses `upgradeRowsForPanel` (current ?? snapshot)
                    so the panel stays mounted across Apply-induced reducer wipes;
                    the snapshot also clears via × Clear's onClear callback. */}
                {upgradeRowsForPanel.length > 0 ? (
                  <UpgradeSuggestionsList
                    rows={upgradeRowsForPanel}
                    decklistText={deckText}
                    onDecklistChange={(nextText) => {
                      dispatchDeckAction({ type: "USER_EDIT_DECK_TEXT", deckText: nextText });
                    }}
                    onClear={() => {
                      setUpgradeSnapshotRows(null);
                      dispatchDeckAction({ type: "CLEAR_UPGRADE_SUGGESTIONS" });
                    }}
                  />
                ) : null}

                {shouldShowSufficiencyDashboard(buildResponse) ? (
                  <div className="phase4-sufficiency-wrap">
                    <SufficiencyDashboard summary={extractSufficiencySummary(buildResponse)} />
                    {shouldShowSwapSuggestions(buildResponse) ? (
                      <SwapSuggestionsList swaps={extractSwapSuggestions(buildResponse) as ReadonlyArray<SwapSuggestion>} />
                    ) : null}
                  </div>
                ) : null}

                {shouldShowCommanderRecommendation(commander) ? (
                  <CommanderRecommendationPanel
                    recommendation={extractCommanderRecommendation(buildResponse)}
                    onPick={(candidate) => setCommander(candidate.name)}
                  />
                ) : null}

                {/* Phase 4.14 Stage 3: GroupedDeckList render call removed.
                    The DeckEditorPanel + DeckPanel two-column visual list
                    (with working CardHoverPreview) below is the canonical
                    deck view. The 4.6 components remain in the codebase
                    BYTE-IDENTICAL per HARD #14 — only this render call
                    was removed. */}
              </div>
            ) : (
              <div className="mb-token-3">
                <SeedBuilderPanel
                  apiBase={normalizedApiBase}
                  snapshotId={(snapshotId || "").trim()}
                  profileId={(profileId || "").trim() || "focused"}
                  initialBracketId={(bracketId || "").trim() || "B2"}
                  initialSeedText={deckText}
                  initialCommander={commander}
                  recommendationSlot={
                    <CommanderRecommendationPanel
                      recommendation={extractCommanderRecommendation(buildResponse)}
                      onPick={(candidate) => setCommander(candidate.name)}
                    />
                  }
                />
              </div>
            )}
          </section>

          {showExternalBackendBanner ? (
            <GlassPanel className="workspace-external-backend-banner">
              <div className="workspace-external-backend-banner-row">
                <p className="workspace-external-backend-message">
                  Using an existing backend already running on port 8000. Desktop overrides (DB/UI/cache paths) may not be
                  applied.
                </p>
                <div className="workspace-external-backend-banner-actions">
                  <button
                    type="button"
                    className="workspace-link-button"
                    onClick={() => {
                      setShowExternalBackendHelp((previous: boolean) => !previous);
                    }}
                  >
                    {showExternalBackendHelp ? "Hide Help" : "Help"}
                  </button>
                  <button
                    type="button"
                    className="workspace-link-button"
                    onClick={() => {
                      window.location.reload();
                    }}
                  >
                    Retry (after you stop the other backend)
                  </button>
                </div>
              </div>

              {showExternalBackendHelp ? (
                <p className="workspace-external-backend-help">
                  Desktop intentionally does not terminate externally started backends. Stop the process currently bound to
                  port 8000, then use Retry so desktop can relaunch with managed DB/UI/image-cache overrides.
                </p>
              ) : null}
            </GlassPanel>
          ) : null}

          <GlassPanel className="workspace-topbar-panel">
            <div className={`workspace-topbar-grid ${!isAnalyzeMode ? "workspace-topbar-grid-minimal" : ""}`}>
              {/* v1.7.5 UX polish: HeaderChips (snapshot/profile/bracket/
                  status/ui_mode/api_ping row) is now gated on an explicit
                  localStorage opt-in `mtgdb:show_dev_chips=true`, because
                  the prior `import.meta.env.DEV` gate was always TRUE on
                  the user's local dev server runs. Confirmed in the
                  2026-05-16 walk that the chips were leaking to users.
                  Devs who want them back: open DevTools console and run
                  `localStorage.setItem('mtgdb:show_dev_chips','true')`
                  then refresh. Same info also surfaces in the user-facing
                  metric pill row (Commander / Card count / Bracket /
                  Status) when a deck is loaded. */}
              {(() => {
                try {
                  return typeof window !== "undefined" &&
                    window.localStorage?.getItem("mtgdb:show_dev_chips") === "true";
                } catch {
                  return false;
                }
              })() ? (
                <HeaderChips
                  buildResponse={buildResponse}
                  loading={isAnyToolRunning}
                  compact={!isAnalyzeMode}
                  apiBase={normalizedApiBase}
                  uiMode={uiModeLabel}
                  uiCommit={uiCommit}
                  apiPingSummary={apiPingSummary}
                  className="workspace-topbar-block"
                />
              ) : null}
              {isAnalyzeMode ? (
                <StatusBar
                  buildResponse={buildResponse}
                  loading={isAnyToolRunning}
                  runtimeError={runtimeError}
                  apiErrorDetails={apiErrorDetails}
                  errorDetailsOpenSignal={errorDetailsOpenSignal}
                  className="workspace-topbar-block workspace-topbar-status"
                />
              ) : null}
            </div>

            {/* v1.6.2 Stage 4: EDIT/TOOLS pills wrapped with a small
                "View" label so they read as a mode-selector group, not
                stranded orphan pills. workspace-mode-tabs / workspace-mode-tab
                classes preserved BYTE-IDENTICAL.
                v1.7.5 UX polish: hide the entire view-mode row when no
                deck is loaded — neither EDIT nor TOOLS makes sense without
                an active deck, and the empty-state CTA card already covers
                the "no deck" path. Confirmed in 2026-05-16 walk that this
                row was visible at empty state. */}
            {deckState.source !== "fallback" ? (
              <div
                className="flex items-center gap-token-2 mt-token-2"
                data-v162-stage="workspace-mode-row"
              >
                <span
                  className="text-xs uppercase tracking-wider text-text-muted"
                  aria-hidden="true"
                >
                  View
                </span>
                <div className="workspace-mode-tabs" role="tablist" aria-label="Workspace mode">
                  {WORKSPACE_MODE_OPTIONS.map((mode: WorkspaceMode) => (
                    <button
                      key={mode}
                      type="button"
                      role="tab"
                      aria-selected={workspaceMode === mode}
                      className={`workspace-mode-tab ${workspaceMode === mode ? "workspace-mode-tab-active" : ""}`}
                      onClick={() => {
                        setWorkspaceMode(mode);
                      }}
                    >
                      {mode}
                    </button>
                  ))}
                </div>
              </div>
            ) : null}

            {/* v1.6.2 Stage 4: removed bare "Resolved: N/M (art ready)"
                line — redundant with the cleaner "Hover art works" PASS/FAIL
                indicator inside the Analyze view (line ~5102). The
                hoverArtReleaseMetrics memo + its consumers (telemetry export
                + PASS/FAIL indicator) are load-bearing and stay verbatim. */}
            {isAnalyzeMode && missingImageCount > 0 ? (
              <div className="workspace-chip-row">
                <span className="workspace-chip workspace-chip-alert">Image missing for {missingImageCount} cards</span>
                <button
                  type="button"
                  className="workspace-link-button"
                  title={prefetchSnapshotImagesCommand}
                  onClick={() => {
                    void copyTextToClipboard(prefetchSnapshotImagesCommand)
                      .then(() => {
                        setToastMessage("Prefetch Snapshot Images command copied.");
                      })
                      .catch(() => {
                        setToastMessage("Failed to copy prefetch command.");
                      });
                  }}
                >
                  Prefetch Snapshot Images
                </button>
              </div>
            ) : null}
          </GlassPanel>

          <div className={`workspace-grid workspace-grid-${workspaceMode.toLowerCase()}`}>
            {isAnalyzeMode ? (
              <aside className="workspace-col-left">
                <div className="workspace-left-stack">
                  <section id="workspace-diagnostics" className="workspace-section-anchor">
                    <section className="workspace-panel">
                      <details open className="workspace-collapsible">
                        <summary>Diagnostics</summary>
                        <p className="workspace-muted">Open the original Diagnostics harness in the diagnostics route.</p>
                        <button type="button" onClick={openDiagnosticsView}>
                          Open Diagnostics
                        </button>
                      </details>
                    </section>
                  </section>

                  <BuildHistoryPanel
                    entries={historyEntries}
                    selectedEntryId={selectedHistoryEntryId}
                    onSelectEntry={handleSelectHistoryEntry}
                  />
                </div>
              </aside>
            ) : null}

            {isEditMode || isAnalyzeMode || isToolsMode ? (
            <section className="workspace-col-center">
              <div className="workspace-center-stack">
                {/* v1.6 Stage 4: empty-state card replaces the Krenko/synthetic
                    fallback deck panel when the reducer hasn't loaded a real
                    deck. Reducer INITIAL_STATE preserved BYTE-IDENTICAL per
                    HARD #9 (other components defense-in-depth); only the
                    WorkspaceView render path gates on `source === "fallback"`
                    + `isHydrated`. CTAs route to #import + switch to Seed
                    Builder mode. */}
                {isEditMode && deckState.source === "fallback" ? (
                  <GlassPanel className="workspace-empty-state-card" data-v16-stage="empty-state-card">
                    <div className="text-center p-panel-pad">
                      <h2 className="text-2xl font-bold text-text-primary mb-token-2">
                        No deck loaded
                      </h2>
                      <p className="text-text-secondary mb-token-3">
                        Import a deck from Archidekt, Arena, MTGO, or plain text — or start a new deck from a seed.
                      </p>
                      <div className="flex flex-wrap justify-center gap-token-2">
                        <Button
                          variant="primary"
                          size="md"
                          onClick={() => {
                            window.location.hash = "#import";
                          }}
                          aria-label="Import a deck"
                        >
                          Import a deck
                        </Button>
                        <Button
                          variant="secondary"
                          size="md"
                          onClick={() => setPageMode("SEED_BUILDER")}
                          aria-label="Start from a seed"
                        >
                          Start from a seed
                        </Button>
                      </div>
                    </div>
                  </GlassPanel>
                ) : isEditMode ? (
                  <DeckEditorPanel
                    apiBase={apiBase}
                    snapshotId={snapshotId}
                    commanderName={commander}
                    commanderOracleId={deckPanelCommander?.oracleId || null}
                    cardsInput={deckText}
                    parsedDeckRows={parsedDeckRows}
                    deckLineCount={deckTextLineCount}
                    deckTextRevision={deckTextRevision}
                    cardHintsByName={deckEditorCardHints}
                    gameChangers={gameChangerNameSet}
                    savedDeckNames={savedDeckNames}
                    selectedSavedDeckName={selectedSavedDeckName}
                    onSelectedSavedDeckNameChange={handleSelectedSavedDeckNameChange}
                    onSaveDeck={handleSaveDeck}
                    onLoadSavedDeck={handleLoadSavedDeck}
                    onRenameSavedDeck={handleRenameSavedDeck}
                    onDeleteSavedDeck={handleDeleteSavedDeck}
                    onCardsInputChange={(value: string) => {
                      applyDeckText(value, "deck_editor_input");
                    }}
                    onHoverCard={setHoverCard}
                    onResolveNamesMissingChange={setResolveNamesMissingNames}
                    onOpenCard={openCardModal}
                    onCommanderChange={(nextCommanderName: string) => {
                      const normalizedCommanderName = nextCommanderName.trim();
                      if (normalizedCommanderName === "" || normalizedCommanderName === commander.trim()) {
                        return;
                      }
                      setCommander(normalizedCommanderName);
                      setBuildResponse(null);
                      setHoverCard(null);
                    }}
                    /* Phase 4.14.1: legacy "Complete to 100" + "Apply
                       Complete" button props omitted — DeckEditorPanel
                       conditionally renders both on prop presence
                       (`{onCompleteTo100 ? ... : null}`), so omitting the
                       props HIDES the buttons from the user-visible UI
                       without modifying DeckEditorPanel internals (HARD #8
                       holds — the panel's internal code is BYTE-IDENTICAL).
                       The unified "1. Complete deck" button in the Phase 4
                       panel section above is the canonical action.
                       handleManaTuneTool + handleApplyCompletedDecklist
                       stay in the file — handleUnifiedCompleteDeck still
                       invokes them via the await pattern (HARD #15 — the
                       handlers themselves are BYTE-IDENTICAL behavior). */
                    runningCompleteTo100={runningSmartComplete}
                    disableCompleteActions={isAnyToolRunning}
                    canApplyCompletedDecklist={canApplyCompletedDecklist}
                    completionStatus={asString(completionResult?.status)}
                    /* v1.4 Stage 2: completionAddedCards + completionLandsAdded
                       props omitted. The legacy "Added cards: N / Added lands:
                       N" chips inside DeckEditorPanel (lines 1113-1114) render
                       only when these are passed (gated on
                       normalizedCompletionAddedCards !== null). v1.1
                       AddedCardsPanel is the canonical card-level visual now;
                       the bare numeric chips were misleading because v1.3's
                       snapshot timing bug yielded "Added cards: 0" even when
                       22 were actually added. HARD #8 preserved:
                       DeckEditorPanel BYTE-IDENTICAL — we use the
                       prop-omission escape hatch (same precedent as Phase 6
                       partial Stage 0 omitting onCompleteTo100). */
                    completionError={completionError}
                  />
                ) : null}

                {isToolsMode ? (
                  <GlassPanel className="workspace-tools-main">
                    <div className="workspace-tools-button-row" role="tablist" aria-label="Tools">
                      <button
                        type="button"
                        role="tab"
                        aria-selected={activeTool === "DECK_TRIM"}
                        className={`workspace-tool-action-button ${activeTool === "DECK_TRIM" ? "workspace-tool-action-button-active" : ""}`}
                        onClick={() => {
                          setActiveTool("DECK_TRIM");
                        }}
                      >
                        {runningSmartTrim ? "Deck Trim..." : "Deck Trim"}
                      </button>

                      <button
                        type="button"
                        role="tab"
                        aria-selected={activeTool === "POWER_TUNE"}
                        className={`workspace-tool-action-button ${activeTool === "POWER_TUNE" ? "workspace-tool-action-button-active" : ""}`}
                        onClick={() => {
                          setActiveTool("POWER_TUNE");
                        }}
                      >
                        {runningSmartTune ? "Power Tune..." : "Power Tune"}
                      </button>

                      <button
                        type="button"
                        className="workspace-tool-action-button"
                        aria-haspopup="dialog"
                        onClick={() => {
                          setIsHistoryModalOpen(true);
                        }}
                      >
                        History
                      </button>

                    </div>

                    <section className="workspace-tool-results" aria-live="polite">
                    {activeTool === "POWER_TUNE" ? (
                      <section className="workspace-tool-panel">
                        <div className="workspace-tool-panel-header">
                          <h3>Power Tune</h3>
                          <div className="workspace-tool-controls-row">
                            <label className="workspace-field workspace-tool-number-field">
                              <span>Max swaps</span>
                              <input
                                type="number"
                                min={1}
                                max={MAX_SWAPS_LIMIT}
                                value={maxSwaps}
                                onChange={(event) => {
                                  setMaxSwaps(clampInteger(Number(event.target.value), 1, MAX_SWAPS_LIMIT));
                                }}
                                disabled={runningSmartTune || isAnyToolRunning}
                              />
                            </label>
                            <button
                              type="button"
                              className="workspace-tool-run-button"
                              onClick={() => {
                                void handlePowerTuneTool();
                              }}
                              disabled={runningSmartTune || isAnyToolRunning}
                            >
                              {runningSmartTune ? "Running Power Tune..." : "Run Power Tune"}
                            </button>
                          </div>
                        </div>
                        {!deckTuneResponse ? (
                          <p className="workspace-muted">Run Power Tune to view swaps and apply changes.</p>
                        ) : tuneSwapRows.length === 0 ? (
                          <p className="workspace-muted">No recommended swaps were returned.</p>
                        ) : (
                          <>
                            <h4>Swap Preview ({tuneSwapRows.length} swaps)</h4>
                            <ul className="workspace-compact-list workspace-scroll-list">
                              {tuneSwapRows.map((swap: DeckTuneSwapV1, index: number) => {
                                const cutName = asString(swap.cut_name) || "(no cut)";
                                const addName = asString(swap.add_name) || "(no add)";
                                const reasons = toSortedUniqueStrings(asStringList(swap.reasons_v1));
                                const primitivesAdded = toSortedUniqueStrings(asStringList(asRecord(swap as unknown)?.primitives_added_v1));
                                const primitivesRemoved = toSortedUniqueStrings(asStringList(asRecord(swap as unknown)?.primitives_removed_v1));
                                const scoreDelta = firstNumber(asRecord(swap as unknown)?.total_score_delta_v1);
                                const coherenceDelta = firstNumber(asRecord(swap as unknown)?.coherence_delta_v1);
                                const addHint = deckEditorCardHints[normalizeNameToken(addName)];
                                return (
                                  <li
                                    key={`tool-power-swap-${index}`}
                                    onMouseEnter={() => {
                                      setHoverCard({
                                        name: addName,
                                        oracle_id: addHint?.oracleId || "",
                                        type_line: addHint?.typeLine || null,
                                        primitive_tags: [],
                                        source: "deck",
                                      });
                                    }}
                                    onMouseLeave={() => {
                                      setHoverCard(null);
                                    }}
                                  >
                                    <strong>{cutName}</strong> → <strong>{addName}</strong>
                                    {/* v1.6.4 Stage 2: translate raw engine reason codes
                                        through the v1.6.2 justificationLabels map before
                                        display. Raw codes preserved on a separate `title`
                                        attribute for power-user hover. */}
                                    {reasons.length > 0 ? (
                                      <div
                                        className="workspace-muted"
                                        title={reasons.join(", ")}
                                      >
                                        why: {reasons.map((r) => translateJustification(r)).join(", ")}
                                      </div>
                                    ) : null}
                                    {scoreDelta !== null ? <div className="workspace-muted">score Δ: {scoreDelta.toFixed(6)}</div> : null}
                                    {coherenceDelta !== null ? <div className="workspace-muted">coherence Δ: {coherenceDelta.toFixed(6)}</div> : null}
                                    {primitivesAdded.length > 0 ? (
                                      <div className="workspace-muted">primitives +: {primitivesAdded.join(", ")}</div>
                                    ) : null}
                                    {primitivesRemoved.length > 0 ? (
                                      <div className="workspace-muted">primitives -: {primitivesRemoved.join(", ")}</div>
                                    ) : null}
                                  </li>
                                );
                              })}
                            </ul>
                          </>
                        )}

                        {deckTuneResponse ? (
                          <div className="workspace-action-row">
                            <button
                              type="button"
                              className="workspace-tool-run-button"
                              onClick={handleApplyPowerTuneSwaps}
                              disabled={tuneSwapRows.length === 0}
                            >
                              Apply All Swaps
                            </button>
                            <button
                              type="button"
                              onClick={() => {
                                void handleCopyPowerTuneSwaps();
                              }}
                              disabled={tuneSwapRows.length === 0}
                            >
                              Copy Swaps
                            </button>
                            <button type="button" className="workspace-link-button" onClick={handleDismissPowerTuneResult}>
                              Dismiss
                            </button>
                          </div>
                        ) : null}
                      </section>
                    ) : null}

                    {activeTool === "DECK_TRIM" ? (
                      <section className="workspace-tool-panel">
                        <div className="workspace-tool-panel-header">
                          <h3>Deck Trim</h3>
                          <button
                            type="button"
                            className="workspace-tool-run-button"
                            onClick={() => {
                              void handleDeckTrimTool();
                            }}
                            disabled={runningSmartTrim || isAnyToolRunning}
                          >
                            {runningSmartTrim ? "Running Deck Trim..." : "Run Deck Trim"}
                          </button>
                        </div>
                        {deckTrimResult ? (
                          <div className="workspace-chip-row">
                            <span className="workspace-chip">status: {deckTrimResult.status || "UNKNOWN"}</span>
                            <span className="workspace-chip">cards_to_cut: {deckTrimResult.cards_to_cut}</span>
                            <span className="workspace-chip">cut_order_count: {deckTrimResult.cut_order.length}</span>
                            {deckTrimResult.source ? <span className="workspace-chip">source: {deckTrimResult.source}</span> : null}
                          </div>
                        ) : null}
                        {!deckTrimResult ? (
                          <p className="workspace-muted">Run Deck Trim to populate cut_order from the latest build.</p>
                        ) : deckTrimRows.length === 0 ? (
                          <p className="workspace-muted">{deckTrimResult.message || "No cuts available."}</p>
                        ) : (
                          <>
                            <h4>Cut Preview ({deckTrimResult.cut_order.length} cuts)</h4>
                            <ul className="workspace-compact-list workspace-scroll-list">
                              {deckTrimRows.map((row: DeckTrimResultRow, index: number) => {
                                const cardHint = deckEditorCardHints[normalizeNameToken(row.cardName)];
                                return (
                                  <li
                                    key={`tool-trim-cut-${index}`}
                                    onMouseEnter={() => {
                                      setHoverCard({
                                        name: row.cardName,
                                        oracle_id: cardHint?.oracleId || "",
                                        type_line: cardHint?.typeLine || null,
                                        primitive_tags: row.primitives,
                                        source: "deck",
                                      });
                                    }}
                                    onMouseLeave={() => {
                                      setHoverCard(null);
                                    }}
                                  >
                                    <strong>{row.cardName}</strong>
                                    {row.reasons.length > 0 ? (
                                      <div className="workspace-muted">why: {row.reasons.join(", ")}</div>
                                    ) : (
                                      <div className="workspace-muted">why: deterministic cut_order from OVER_100 build.</div>
                                    )}
                                    {row.primitives.length > 0 ? (
                                      <div className="workspace-muted">primitives: {row.primitives.join(", ")}</div>
                                    ) : null}
                                  </li>
                                );
                              })}
                            </ul>
                          </>
                        )}

                        {deckTrimResult ? (
                          <div className="workspace-action-row">
                            <button
                              type="button"
                              className="workspace-tool-run-button"
                              onClick={handleApplyDeckTrimCuts}
                              disabled={!deckTrimResult.can_apply || pendingCutOrder.length === 0}
                            >
                              Apply Cuts
                            </button>
                            <button
                              type="button"
                              onClick={() => {
                                void handleCopyDeckTrimCuts();
                              }}
                              disabled={pendingCutOrder.length === 0}
                            >
                              Copy Cuts
                            </button>
                            <button type="button" className="workspace-link-button" onClick={handleDismissDeckTrimResult}>
                              Dismiss
                            </button>
                          </div>
                        ) : null}
                      </section>
                    ) : null}

                    </section>
                  </GlassPanel>
                ) : null}

                {isAnalyzeMode ? (
                  <>
                    <GlassPanel>
                      <section className="workspace-panel-content">
                        <details open className="workspace-collapsible">
                          <summary>Release Checklist</summary>

                          <ul className="workspace-compact-list">
                            <li>
                              <span className={`workspace-status-dot ${isApiReachable ? "status-ok" : "status-error"}`}>
                                {isApiReachable ? "PASS" : "FAIL"}
                              </span>{" "}
                              API reachable
                            </li>
                            <li>
                              <span className={`workspace-status-dot ${snapshotId.trim() !== "" ? "status-ok" : "status-error"}`}>
                                {snapshotId.trim() !== "" ? "PASS" : "FAIL"}
                              </span>{" "}
                              Snapshot selected
                            </li>
                            <li>
                              <span className={`workspace-status-dot ${lastValidatePassed ? "status-ok" : "status-error"}`}>
                                {lastValidatePassed ? "PASS" : "FAIL"}
                              </span>{" "}
                              Validate passes
                            </li>
                            <li>
                              <span className={`workspace-status-dot ${lastSmokeSucceeded ? "status-ok" : "status-error"}`}>
                                {lastSmokeSucceeded ? "PASS" : "FAIL"}
                              </span>{" "}
                              Complete works (last run success)
                            </li>
                            <li>
                              <span className={`workspace-status-dot ${lastTuneSucceeded ? "status-ok" : "status-error"}`}>
                                {lastTuneSucceeded ? "PASS" : "FAIL"}
                              </span>{" "}
                              Tune works
                            </li>
                            <li>
                              <span className={`workspace-status-dot ${lastTrimSucceeded ? "status-ok" : "status-error"}`}>
                                {lastTrimSucceeded ? "PASS" : "FAIL"}
                              </span>{" "}
                              Trim works
                            </li>
                            <li>
                              <span className={`workspace-status-dot ${hoverArtReleaseMetrics.isReady ? "status-ok" : "status-error"}`}>
                                {hoverArtReleaseMetrics.isReady ? "PASS" : "FAIL"}
                              </span>{" "}
                              Hover art works ({hoverArtReleaseMetrics.resolvedCount}/{hoverArtReleaseMetrics.targetCount} resolved, {hoverArtReleaseMetrics.artReadyPercent}%
                              {hoverArtReleaseMetrics.skippedBasicCount > 0
                                ? `, basics skipped: ${hoverArtReleaseMetrics.skippedBasicCount}`
                                : ""}
                              )
                            </li>
                          </ul>

                          <div className="workspace-action-row">
                            <button
                              type="button"
                              onClick={() => {
                                void handleCopyReleaseChecklistBugBundle();
                              }}
                            >
                              Copy Bug Bundle
                            </button>
                            {releaseChecklistCopyNotice ? <span className="workspace-copy-notice">{releaseChecklistCopyNotice}</span> : null}
                            {releaseChecklistCopyError ? <span className="workspace-error-inline">{releaseChecklistCopyError}</span> : null}
                          </div>
                        </details>
                      </section>
                    </GlassPanel>

                    <GlassPanel>
                      <PrimitiveExplorerPanel
                        buildResponse={buildResponse}
                        onHoverCard={setHoverCard}
                        onCardClick={openCardModal}
                      />
                    </GlassPanel>

                    <GlassPanel>
                      <CanonicalSlotsPanel buildResponse={buildResponse} />
                    </GlassPanel>

                    <section id="workspace-unknowns-panel" className="workspace-section-anchor">
                      <GlassPanel>
                        <UnknownsPatchesPanel
                          buildResponse={buildResponse}
                          requestPayload={requestPayload}
                          resolveNamesMissingNames={resolveNamesMissingNames}
                        />
                      </GlassPanel>
                    </section>
                  </>
                ) : null}
              </div>
            </section>
            ) : null}

          </div>
        </div>
      </main>

      {!isCardModalOpen ? (
        <HoverCardPreview
          apiBase={normalizedApiBase}
          hoverCard={hoverCard}
          previewImageFailures={previewImageFailures}
          markPreviewImageFailure={markPreviewImageFailure}
          clearMissingImageForOracle={clearMissingImageForOracle}
        />
      ) : null}

      <CardModal
        apiBase={normalizedApiBase}
        isOpen={isCardModalOpen}
        oracleId={cardModalOracleId}
        oracleIds={cardModalList}
        index={cardModalIndex}
        onClose={closeCardModal}
        onPrev={cardModalList.length > 1 ? goPrev : undefined}
        onNext={cardModalList.length > 1 ? goNext : undefined}
      />

      {savedDeckDialogMode ? (
        <div
          className="card-modal-scrim"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              closeSavedDeckDialog();
            }
          }}
        >
          <div
            className="card-modal-shell smart-tools-modal-shell workspace-saved-deck-modal-shell"
            ref={savedDeckDialogShellRef}
            role="dialog"
            aria-modal="true"
            aria-describedby={savedDeckDialogDescribedBy}
            tabIndex={-1}
            aria-label={
              savedDeckDialogMode === "SAVE"
                ? "Save deck"
                : savedDeckDialogMode === "RENAME"
                  ? "Rename deck"
                  : "Delete deck"
            }
          >
            <div className="smart-tools-modal-header">
              <h3>
                {savedDeckDialogMode === "SAVE"
                  ? "Save deck"
                  : savedDeckDialogMode === "RENAME"
                    ? "Rename deck"
                    : "Delete deck"}
              </h3>
              <button type="button" className="workspace-link-button" onClick={closeSavedDeckDialog}>
                Close
              </button>
            </div>

            <form
              className="workspace-saved-deck-modal-form"
              onSubmit={(event) => {
                event.preventDefault();
                handleSubmitSavedDeckDialog();
              }}
            >
              <p id={savedDeckDialogDescriptionId} className="workspace-muted workspace-saved-deck-modal-copy">
                {savedDeckDialogMode === "SAVE"
                  ? "Choose a deck name. Reusing a name updates that saved deck."
                  : savedDeckDialogMode === "RENAME"
                    ? `Rename "${savedDeckDialogTargetName}" to a new deck name.`
                    : `Delete "${savedDeckDialogTargetName}" from saved decks?`}
              </p>

              {savedDeckDialogMode === "SAVE" && savedDeckDialogExistingDeck ? (
                <p className="workspace-muted workspace-saved-deck-modal-copy">
                  {saveDialogHasChanges
                    ? `Saving will overwrite "${savedDeckDialogExistingDeck.name}".`
                    : `"${savedDeckDialogExistingDeck.name}" is already up to date.`}
                </p>
              ) : null}

              {savedDeckDialogValidationMessage !== "" ? (
                <p
                  id={savedDeckDialogValidationId}
                  className="workspace-saved-deck-modal-validation"
                  role="status"
                  aria-live="polite"
                >
                  {savedDeckDialogValidationMessage}
                </p>
              ) : null}

              {savedDeckDialogMode === "DELETE" ? (
                <p className="workspace-saved-deck-modal-target">{savedDeckDialogTargetName}</p>
              ) : (
                <label className="workspace-field">
                  <span>Deck name</span>
                  <input
                    type="text"
                    value={savedDeckDialogNameInput}
                    onChange={(event) => {
                      setSavedDeckDialogNameInput(event.target.value);
                    }}
                    placeholder="Deck name"
                    data-saved-deck-name-input="true"
                    aria-invalid={savedDeckDialogValidationMessage !== ""}
                    aria-describedby={savedDeckDialogValidationMessage !== "" ? savedDeckDialogValidationId : undefined}
                    autoFocus
                  />
                </label>
              )}

              <div className="workspace-action-row workspace-saved-deck-modal-actions">
                <button type="button" className="workspace-link-button" onClick={closeSavedDeckDialog}>
                  Cancel
                </button>
                <button
                  type="submit"
                  className="workspace-link-button workspace-saved-deck-modal-submit"
                  data-saved-deck-submit="true"
                  disabled={savedDeckDialogSubmitDisabled}
                  autoFocus={savedDeckDialogMode === "DELETE"}
                >
                  {savedDeckDialogSubmitLabel}
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}

      {isHistoryModalOpen ? (
        <div
          className="card-modal-scrim"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              setIsHistoryModalOpen(false);
            }
          }}
        >
          <div className="card-modal-shell workspace-history-modal-shell" role="dialog" aria-modal="true" aria-label="Build History">
            <div className="smart-tools-modal-header">
              <h3>Build History</h3>
              <button
                type="button"
                className="workspace-link-button"
                onClick={() => {
                  setIsHistoryModalOpen(false);
                }}
              >
                Close
              </button>
            </div>

            <BuildHistoryPanel
              entries={historyEntries}
              selectedEntryId={selectedHistoryEntryId}
              onSelectEntry={handleSelectHistoryEntry}
            />
          </div>
        </div>
      ) : null}

      {isCompletionBlockedModalOpen ? (
        <div
          className="card-modal-scrim"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              setIsCompletionBlockedModalOpen(false);
            }
          }}
        >
          <div
            className="card-modal-shell smart-tools-modal-shell"
            role="dialog"
            aria-modal="true"
            aria-label={`${completionBlockedToolLabel} blocked`}
          >
            <div className="smart-tools-modal-header">
              <h3>{completionBlockedToolLabel} blocked</h3>
              <button
                type="button"
                className="workspace-link-button"
                onClick={() => {
                  setIsCompletionBlockedModalOpen(false);
                }}
              >
                Close
              </button>
            </div>

            <p>
              Validate status: <strong>{completionBlockedStatus || "UNKNOWN"}</strong>
            </p>

            <p className="workspace-muted">
              Resolve listed unknowns/violations in deck input or overrides, then run the tool again. The engine will not
              guess unresolved cards.
            </p>

            <div className="workspace-action-row">
              <button
                type="button"
                className="workspace-link-button"
                onClick={() => {
                  void handleCopyBlockedUnknowns();
                }}
              >
                Copy unknowns
              </button>
            </div>

            {completionBlockedUnknowns.length > 0 ? (
              <div>
                <h4>Unknowns</h4>
                <ul className="workspace-compact-list workspace-scroll-list">
                  {completionBlockedUnknowns.map((unknown: ValidateUnknownRow, index: number) => (
                    <li key={`complete-block-unknown-${index}`}>
                      <strong>{asString(unknown.reason_code) || "UNKNOWN"}</strong>
                      {asString(unknown.name_raw) ? ` :: ${asString(unknown.name_raw)}` : ""}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}

            {completionBlockedViolations.length > 0 ? (
              <div>
                <h4>Violations</h4>
                <ul className="workspace-compact-list workspace-scroll-list">
                  {completionBlockedViolations.map((violation: ValidateViolationRow, index: number) => {
                    const lineNos = Array.isArray(violation.line_nos)
                      ? violation.line_nos
                          .filter((value: number) => Number.isFinite(value))
                          .map((value: number) => String(Math.trunc(value)))
                          .join(", ")
                      : "";

                    return (
                      <li key={`complete-block-violation-${index}`}>
                        <strong>{asString(violation.code) || "VIOLATION"}</strong>
                        {asString(violation.message) ? ` :: ${asString(violation.message)}` : ""}
                        {lineNos ? ` (lines: ${lineNos})` : ""}
                      </li>
                    );
                  })}
                </ul>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {isSnapshotNotReadyModalOpen ? (
        <div
          className="card-modal-scrim"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              setIsSnapshotNotReadyModalOpen(false);
            }
          }}
        >
          <div className="card-modal-shell smart-tools-modal-shell" role="dialog" aria-modal="true" aria-label="Snapshot not ready">
            <div className="smart-tools-modal-header">
              <h3>Snapshot not ready</h3>
              <button
                type="button"
                className="workspace-link-button"
                onClick={() => {
                  setIsSnapshotNotReadyModalOpen(false);
                }}
              >
                Close
              </button>
            </div>

            <p>
              Tool: <strong>{snapshotNotReadyToolLabel || "Smart Tool"}</strong>
            </p>

            <p>
              Preflight status: <strong>{snapshotNotReadyStatus || "UNKNOWN"}</strong>
            </p>

            <p className="workspace-muted">
              Snapshot preflight reported blocking errors. Resolve these issues before running Complete.
            </p>

            {snapshotNotReadyErrors.length > 0 ? (
              <div>
                <h4>Errors</h4>
                <ul className="workspace-compact-list workspace-scroll-list">
                  {snapshotNotReadyErrors.map((row: SnapshotPreflightErrorRow, index: number) => (
                    <li key={`snapshot-preflight-error-${index}`}>
                      <strong>{asString(row.code) || "UNKNOWN"}</strong>
                      {asString(row.message) ? ` :: ${asString(row.message)}` : ""}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <p className="workspace-muted">No explicit preflight errors were returned.</p>
            )}

            <div>
              <h4>Fix instructions</h4>
              <ul className="workspace-compact-list">
                {snapshotNotReadyFixInstructions.length > 0 ? (
                  snapshotNotReadyFixInstructions.map((instruction: string, index: number) => (
                    <li key={`snapshot-preflight-fix-${index}`}>{instruction}</li>
                  ))
                ) : (
                  <li>Review preflight error codes/messages, fix snapshot readiness, then rerun the tool.</li>
                )}
              </ul>
            </div>
          </div>
        </div>
      ) : null}

      {toastMessage ? (
        <div className="workspace-toast" role="status" aria-live="polite">
          {toastMessage}
        </div>
      ) : null}
    </div>
  );
}
