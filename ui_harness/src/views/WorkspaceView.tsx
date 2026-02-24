import { useEffect, useMemo, useRef, useState } from "react";

import fixtureBuildResult from "../../fixtures/build_result.json";
import BuildHistoryPanel from "../components/BuildHistoryPanel";
import CanonicalSlotsPanel from "../components/CanonicalSlotsPanel";
import CardModal from "../components/CardModal";
import ArtDock from "../components/cards/ArtDock";
import DeckEditorPanel, { type DeckEditorCardHint } from "../components/deck/DeckEditorPanel";
import type { DeckPanelCard, DeckPanelCommander } from "../components/deck/DeckPanel";
import HeaderChips from "../components/HeaderChips";
import LeftRail from "../components/layout/LeftRail";
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
const defaultProfileId = firstNonEmptyString(fixtureRoot?.profile_id) || "focused";
const defaultBracketId = firstNonEmptyString(fixtureRoot?.bracket_id) || "B2";
const defaultCommander = firstNonEmptyString(getBuildResultCommander(fixtureRoot)) || "Krenko, Mob Boss";
const HOVER_PREFETCH_LRU_LIMIT = 200;
const DEFAULT_MAX_SWAPS = 5;
const DEFAULT_COMPLETE_TARGET_DECK_SIZE = 100;
const DEFAULT_COMPLETE_MAX_ADDS = 200;
const DEFAULT_COMPLETE_ALLOW_BASIC_LANDS = true;
const MAX_SWAPS_LIMIT = 50;
const MAX_COMPLETE_ADDS = 500;
const RESOLVE_NAMES_MAX_NAMES_PER_REQUEST = 200;
const DEV_SMOKE_TEST_TOAST_DURATION_MS = 5000;
const DEV_SMOKE_STALE_REQUEST_MESSAGE = "__DEV_SMOKE_STALE_REQUEST__";
const DEV_SMOKE_TEST_COMMANDER = "Krenko, Mob Boss";
const DEV_SMOKE_TEST_CARDS = [
  "Sol Ring",
  "Arcane Signet",
  "Goblin Matron",
  "Skirk Prospector",
  "Impact Tremors",
  "Goblin Warchief",
  "Goblin Chieftain",
  "Skullclamp",
] as const;
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

const MULLIGAN_MODEL_OPTIONS = ["NORMAL"] as const;
type MulliganModelId = (typeof MULLIGAN_MODEL_OPTIONS)[number];

const COMPLETE_LAND_MODE_OPTIONS = ["AUTO", "NONE"] as const;
type DeckCompleteLandMode = (typeof COMPLETE_LAND_MODE_OPTIONS)[number];

type WorkspaceMode = "EDIT" | "TOOLS" | "ANALYZE";
const WORKSPACE_MODE_STORAGE_KEY = "mtg_workspace_mode_v1";
const WORKSPACE_MODE_OPTIONS: WorkspaceMode[] = ["EDIT", "TOOLS", "ANALYZE"];

type WorkspaceToolId = "DECK_TRIM" | "MANA_TUNE" | "POWER_TUNE";

function normalizeWorkspaceMode(value: string | null | undefined): WorkspaceMode {
  const token = typeof value === "string" ? value.trim().toUpperCase() : "";
  if (token === "TOOLS") {
    return "TOOLS";
  }
  if (token === "ANALYZE") {
    return "ANALYZE";
  }
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

function getBuildResultCommander(root: Record<string, unknown> | null): string | null {
  if (!root) {
    return null;
  }
  const result = asRecord(root.result);
  return firstNonEmptyString(result?.commander);
}

function buildCardsInputFromPayloadCards(cards: string[]): string {
  return cards.map((name: string) => `1 ${name}`).join("\n");
}

function buildTimestampLabel(now: Date): string {
  return `${now.toLocaleDateString()} ${now.toLocaleTimeString()}`;
}

function buildHoverPreviewImageUrl(oracleIdRaw: string): string {
  const oracleId = oracleIdRaw.trim();
  if (oracleId === "") {
    return "";
  }
  return `/cards/image/${encodeURIComponent(oracleId)}?size=normal`;
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
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
  const requestBodyText = toErrorPayloadText(options?.requestPayload);
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
  const requestBodyText = toErrorPayloadText(options?.requestPayload);
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

function parseCompletedDecklistCounts(decklistText: string): {
  commanderCount: number;
  deckCount: number;
  totalCount: number;
} {
  const lines = decklistText
    .split(/\r?\n/)
    .map((line: string) => line.trim())
    .filter((line: string) => line !== "");

  let section: "NONE" | "COMMANDER" | "DECK" = "NONE";
  let commanderCount = 0;
  let deckCount = 0;

  for (const line of lines) {
    if (line.toLowerCase() === "commander") {
      section = "COMMANDER";
      continue;
    }
    if (line.toLowerCase() === "deck") {
      section = "DECK";
      continue;
    }
    if (!/^\d+\s+.+$/.test(line)) {
      continue;
    }

    if (section === "COMMANDER") {
      commanderCount += 1;
    } else if (section === "DECK") {
      deckCount += 1;
    }
  }

  return {
    commanderCount,
    deckCount,
    totalCount: commanderCount + deckCount,
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
  const [commander, setCommander] = useState(defaultCommander);
  const [deckText, setDeckText] = useState<string>(
    ["1 Sol Ring", "1 Arcane Signet", "Goblin Matron", "Skirk Prospector", "Impact Tremors"].join("\n"),
  );
  const [deckTextRevision, setDeckTextRevision] = useState(0);

  const [, setValidationMessage] = useState<string | null>(null);
  const [runningSmartTrim, setRunningSmartTrim] = useState(false);
  const [runningSmartTune, setRunningSmartTune] = useState(false);
  const [runningSmartComplete, setRunningSmartComplete] = useState(false);
  const [runningDevSmokeCompleteApply, setRunningDevSmokeCompleteApply] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [apiErrorDetails, setApiErrorDetails] = useState<ApiErrorDetails | null>(null);
  const [isApiReachable, setIsApiReachable] = useState(false);
  const [lastValidatePassed, setLastValidatePassed] = useState(false);
  const [lastSmokeSucceeded, setLastSmokeSucceeded] = useState(false);
  const [lastTuneSucceeded, setLastTuneSucceeded] = useState(false);
  const [lastTrimSucceeded, setLastTrimSucceeded] = useState(false);

  const [buildResponse, setBuildResponse] = useState<BuildResponsePayload | null>(null);
  const [requestPayload, setRequestPayload] = useState<BuildRequestPayload | null>(null);

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
  const [, setCompletionError] = useState<string | null>(null);
  const [deckTrimResult, setDeckTrimResult] = useState<DeckTrimToolResult | null>(null);
  const [tuneSourceCards, setTuneSourceCards] = useState<string[]>([]);
  const [tuneSourceCommander, setTuneSourceCommander] = useState("");
  const [activeTool, setActiveTool] = useState<WorkspaceToolId>("MANA_TUNE");

  const [pendingCutOrder, setPendingCutOrder] = useState<string[]>([]);
  const [isCompletionBlockedModalOpen, setIsCompletionBlockedModalOpen] = useState(false);
  const [completionBlockedUnknowns, setCompletionBlockedUnknowns] = useState<ValidateUnknownRow[]>([]);
  const [completionBlockedViolations, setCompletionBlockedViolations] = useState<ValidateViolationRow[]>([]);
  const [completionBlockedStatus, setCompletionBlockedStatus] = useState("");
  const [completionBlockedToolLabel, setCompletionBlockedToolLabel] = useState("Smart Tool");

  const [historyEntries, setHistoryEntries] = useState<BuildHistoryEntry[]>([]);
  const [selectedHistoryEntryId, setSelectedHistoryEntryId] = useState<string | null>(null);
  const [isHistoryModalOpen, setIsHistoryModalOpen] = useState(false);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [errorDetailsOpenSignal, setErrorDetailsOpenSignal] = useState(0);
  const [releaseChecklistCopyNotice, setReleaseChecklistCopyNotice] = useState<string | null>(null);
  const [releaseChecklistCopyError, setReleaseChecklistCopyError] = useState<string | null>(null);
  const [apiPingSummary, setApiPingSummary] = useState<ApiPingSummary>(DEFAULT_API_PING_SUMMARY);
  const [resolvedDeckCardHints, setResolvedDeckCardHints] = useState<Record<string, DeckEditorCardHint>>({});
  const [resolveNamesMissingNames, setResolveNamesMissingNames] = useState<string[]>([]);

  const [hoverCard, setHoverCard] = useState<HoverCard | null>(null);
  const [previewImageFailures, setPreviewImageFailures] = useState<Record<string, true>>({});

  const [isCardModalOpen, setIsCardModalOpen] = useState(false);
  const [cardModalOracleId, setCardModalOracleId] = useState<string | null>(null);
  const [cardModalList, setCardModalList] = useState<string[]>([]);
  const [cardModalIndex, setCardModalIndex] = useState(0);

  const historyCounterRef = useRef(0);
  const hoverPrefetchLruRef = useRef<Set<string>>(new Set<string>());
  const completionRequestIdRef = useRef(0);
  const resolveDeckNamesRequestIdRef = useRef(0);
  const devSmokeRequestIdRef = useRef(0);
  const lastDeckTextMutationReasonRef = useRef("initial");

  const isEditMode = workspaceMode === "EDIT";
  const isToolsMode = workspaceMode === "TOOLS";
  const isAnalyzeMode = workspaceMode === "ANALYZE";
  const isDevMode = import.meta.env.DEV;

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
  const tuneSwapRows = useMemo(() => asDeckTuneSwapRows(deckTuneResponse?.recommended_swaps_v1), [deckTuneResponse]);
  const completeAddedRows = useMemo(() => asDeckCompleteAddedRows(completionResult?.added_cards_v1), [completionResult]);
  const completedDecklistText = useMemo(() => asString(completionResult?.completed_decklist_text_v1), [completionResult]);
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
  const hasCompletionLandsAddedCount = useMemo(
    () => firstNumber(completionResult?.lands_added_count) !== null,
    [completionResult],
  );
  const completionDecklistCounts = useMemo(
    () => parseCompletedDecklistCounts(completedDecklistText),
    [completedDecklistText],
  );
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
  const isAnyToolRunning = runningSmartTrim || runningSmartTune || runningSmartComplete || runningDevSmokeCompleteApply;

  useEffect(() => {
    try {
      window.localStorage.setItem(WORKSPACE_MODE_STORAGE_KEY, workspaceMode);
    } catch {
      // Ignore persistence failures (privacy mode/quota).
    }
  }, [workspaceMode]);

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
    const imageUrl = buildHoverPreviewImageUrl(hoverCard?.oracle_id || "");
    if (imageUrl === "") {
      return;
    }

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
  }, [hoverCard?.oracle_id]);

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

    const commanderToken = commanderName.trim();
    if (commanderToken !== "") {
      const commanderKey = normalizeNameToken(commanderToken);
      if (commanderKey !== "") {
        seen.add(commanderKey);
        targets.push(commanderToken);
      }
    }

    for (const row of rows) {
      const cardName = row.name.trim();
      const cardKey = normalizeNameToken(cardName);
      if (cardName === "" || cardKey === "" || seen.has(cardKey)) {
        continue;
      }
      seen.add(cardKey);
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
          body: JSON.stringify(requestPayload),
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
        body: JSON.stringify(validatePayload),
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
      } else {
        let buildResponseRaw: Response;
        try {
          buildResponseRaw = await fetch(`${base}/build`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(buildPayload),
          });
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
    setActiveTool("MANA_TUNE");
    setLastValidatePassed(false);
    const requestId = completionRequestIdRef.current + 1;
    completionRequestIdRef.current = requestId;

    setRunningSmartComplete(true);
    setRuntimeError(null);
    setApiErrorDetails(null);
    setCompletionError(null);
    setCompletionResult(null);
    setIsCompletionBlockedModalOpen(false);
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
          body: JSON.stringify(validatePayload),
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
        showValidateBlockedModal("Mana Tune", validateStatus, unknowns, violations);
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

      let response: Response;
      try {
        response = await fetch(`${base}/deck/complete_v1`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        });
      } catch (error) {
        if (completionRequestIdRef.current !== requestId) {
          return;
        }
        const details = buildApiErrorDetails("/deck/complete_v1", null, "", null, {
          method: "POST",
          requestPayload: payload,
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
          requestPayload: payload,
        });
        setApiErrorDetails(details);
        throw new Error(`HTTP ${response.status} from /deck/complete_v1`);
      }

      const root = (asRecord(parsed) ?? {}) as DeckCompleteResponseV1;
      setCompletionResult(root);
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
          toolType: "MANA_TUNE",
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
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown Mana Tune runtime error";
      if (completionRequestIdRef.current !== requestId) {
        return;
      }
      setRuntimeError(message);
      setCompletionError(message);
      setCompletionResult(null);
    } finally {
      if (completionRequestIdRef.current === requestId) {
        setRunningSmartComplete(false);
      }
    }
  }

  function handleApplyCompletedDecklist(): void {
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
        toolType: "MANA_TUNE_APPLY",
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

  async function handleCopyCompletedDecklist(): Promise<void> {
    const completedDecklistText = asString(completionResult?.completed_decklist_text_v1);
    if (completedDecklistText.trim() === "") {
      setRuntimeError("No completed_decklist_text_v1 available to copy.");
      return;
    }

    try {
      await copyTextToClipboard(completedDecklistText);
      setToastMessage("Copied completed decklist.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to copy completed decklist.";
      setRuntimeError(message);
    }
  }

  function handleDismissCompletionResult(): void {
    setCompletionResult(null);
    setCompletionError(null);
  }

  async function handleDevSmokeTestCompleteApply(): Promise<void> {
    if (!isDevMode) {
      return;
    }

    const requestId = devSmokeRequestIdRef.current + 1;
    devSmokeRequestIdRef.current = requestId;

    const isActiveRequest = (): boolean => devSmokeRequestIdRef.current === requestId;
    const throwIfStale = (): void => {
      if (!isActiveRequest()) {
        throw new Error(DEV_SMOKE_STALE_REQUEST_MESSAGE);
      }
    };

    const smokeCommander = DEV_SMOKE_TEST_COMMANDER;
    const smokeInputCards = DEV_SMOKE_TEST_CARDS.map((name: string) => name.trim()).filter((name: string) => name !== "");
    const smokeDeckText = collapseCardNamesInInputOrder(smokeInputCards);
    const smokeText = smokeDeckText;
    const smokeLineCountBefore = parseDecklistInput(smokeText).length;
    const base = normalizeApiBase(apiBase);
    const smokeRequestDebug = {
      api_base: base,
      line_count: countNonEmptyTextLines(smokeText),
      first120Chars: smokeText.slice(0, 120),
    };
    const smokeErrorFallbackPayload: DeckCompleteRequestPayload = {
      db_snapshot_id: snapshotId.trim() || "(auto-latest)",
      raw_decklist_text: smokeText,
      format: "commander",
      profile_id: profileId.trim(),
      bracket_id: bracketId.trim(),
      mulligan_model_id: mulliganModelId,
      target_deck_size: DEFAULT_COMPLETE_TARGET_DECK_SIZE,
      max_adds: clampInteger(completeMaxAdds, 1, MAX_COMPLETE_ADDS),
      allow_basic_lands: true,
      land_target_mode: "AUTO",
      commander: smokeCommander,
    };

    setActiveTool("MANA_TUNE");
    setLastSmokeSucceeded(false);
    setLastValidatePassed(false);
    setRunningDevSmokeCompleteApply(true);
    setRuntimeError(null);
    setApiErrorDetails(null);
    setCompletionError(null);
    setCompletionResult(null);
    setValidationMessage("Running DEV smoke test: Complete + Apply...");
    setCommander(smokeCommander);
    applyDeckText(smokeDeckText, "smoke_seed_input");
    setBuildResponse(null);
    setDeckTuneResponse(null);
    setDeckTrimResult(null);
    setPendingCutOrder([]);
    setSmartToolValidateResponse(null);
    setSmartToolBlockMessage(null);
    setSmartToolBlockUnknowns([]);
    setSmartToolBlockViolations([]);
    setIsCompletionBlockedModalOpen(false);
    setCompletionBlockedToolLabel("Smoke Test");
    setCompletionBlockedUnknowns([]);
    setCompletionBlockedViolations([]);
    setCompletionBlockedStatus("");
    const normalizedOverrides = normalizeNameOverrides(nameOverridesV1);

    const postJson = async (
      endpoint: string,
      payload: unknown,
      requestDebug?: Record<string, unknown>,
    ): Promise<unknown> => {
      throwIfStale();
      let response: Response;
      try {
        response = await fetch(`${base}${endpoint}`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        });
      } catch (error) {
        throwIfStale();
        setIsApiReachable(false);
        const details = buildApiErrorDetails(endpoint, null, "", null, {
          method: "POST",
          requestPayload: payload,
          requestDebug,
          error,
        });
        setApiErrorDetails(details);
        const networkMessage = error instanceof Error ? error.message : "Network request failed.";
        throw new Error(`${endpoint} failed (status: network)\n${networkMessage}`);
      }

      throwIfStale();
      const responseText = await response.text();
      setIsApiReachable(true);
      throwIfStale();
      const parsed = safeParseJson(responseText);
      if (!response.ok) {
        const details = buildApiErrorDetails(endpoint, response, responseText, parsed, {
          method: "POST",
          requestPayload: payload,
          requestDebug,
        });
        setApiErrorDetails(details);
        throw new Error(formatApiErrorMessage(details));
      }

      return parsed;
    };

    try {
      const resolvedSnapshotId = await ensureSmartToolSnapshotId(base);
      throwIfStale();

      const validatePayload: DeckValidateRequestPayload = {
        db_snapshot_id: resolvedSnapshotId,
        raw_decklist_text: smokeText,
        format: "commander",
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
        commander: smokeCommander,
      };
      if (normalizedOverrides.length > 0) {
        validatePayload.name_overrides_v1 = normalizedOverrides;
      }

      const validateParsed = await postJson("/deck/validate", validatePayload, smokeRequestDebug);
      throwIfStale();
      const validateRoot = (asRecord(validateParsed) ?? {}) as DeckValidateResponsePayload;
      setSmartToolValidateResponse(validateRoot);

      const validateUnknowns = asValidateUnknownRows(validateRoot.unknowns);
      const validateViolations = asValidateViolationRows(validateRoot.violations_v1);
      const validateStatus = asString(validateRoot.status) || "UNKNOWN";
      if (validateUnknowns.length > 0 || validateViolations.length > 0 || validateStatus !== "OK") {
        setSmartToolBlockUnknowns(validateUnknowns);
        setSmartToolBlockViolations(validateViolations);
        setSmartToolBlockMessage(`Smoke Test blocked by validate (${validateStatus}).`);
        showValidateBlockedModal("Smoke Test", validateStatus, validateUnknowns, validateViolations);
        const details = buildSyntheticApiErrorDetails("/deck/validate", 200, validateRoot, {
          method: "POST",
          requestPayload: validatePayload,
          requestDebug: smokeRequestDebug,
        });
        setApiErrorDetails(details);
        throw new Error(`/deck/validate failed (status: ${validateStatus})\n${details.responseJsonText || "(empty)"}`);
      }

      setLastValidatePassed(true);

      setSmartToolBlockUnknowns([]);
      setSmartToolBlockViolations([]);
      setSmartToolBlockMessage(null);

      const completeEndpointPayload: DeckCompleteRequestPayload = {
        db_snapshot_id: resolvedSnapshotId,
        raw_decklist_text: smokeText,
        format: "commander",
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
        mulligan_model_id: mulliganModelId,
        target_deck_size: DEFAULT_COMPLETE_TARGET_DECK_SIZE,
        max_adds: clampInteger(completeMaxAdds, 1, MAX_COMPLETE_ADDS),
        allow_basic_lands: true,
        land_target_mode: "AUTO",
        commander: smokeCommander,
      };
      if (normalizedOverrides.length > 0) {
        completeEndpointPayload.name_overrides_v1 = normalizedOverrides;
      }

      const completeParsed = await postJson("/deck/complete_v1", completeEndpointPayload, smokeRequestDebug);
      throwIfStale();
      const completeRoot = (asRecord(completeParsed) ?? {}) as DeckCompleteResponseV1;
      const completionStatus = asString(completeRoot.status) || "UNKNOWN";
      const completedDecklistText = asString(completeRoot.completed_decklist_text_v1);
      if (completionStatus !== "OK" || completedDecklistText.trim() === "") {
        const details = buildSyntheticApiErrorDetails("/deck/complete_v1", 200, completeRoot, {
          method: "POST",
          requestPayload: completeEndpointPayload,
          requestDebug: smokeRequestDebug,
        });
        setApiErrorDetails(details);
        throw new Error(`/deck/complete_v1 failed (status: ${completionStatus})\n${details.responseJsonText || "(empty)"}`);
      }

      setCompletionResult(completeRoot);
      setCompletionError(null);

      const parsedCompletedDecklist = parseCompletedDecklistText(completedDecklistText);
      const resolvedCommander = parsedCompletedDecklist.commander || smokeCommander;
      const parsedAppliedRows = parseDecklistInput(completedDecklistText);
      if (parsedAppliedRows.length === 0) {
        const details = buildSyntheticApiErrorDetails("/deck/complete_v1", 200, completeRoot, {
          method: "POST",
          requestPayload: completeEndpointPayload,
          requestDebug: smokeRequestDebug,
        });
        setApiErrorDetails(details);
        throw new Error(`/deck/complete_v1 failed (status: ${completionStatus})\n${details.responseJsonText || "(empty)"}`);
      }

      applyDeckText(completedDecklistText, "smoke_auto_apply_complete");
      setCommander(resolvedCommander);
      setBuildResponse(null);

      const resolveResultsCount = await resolveDeckRowsAndStoreHints(
        parsedAppliedRows,
        resolvedSnapshotId,
        resolvedCommander,
      );
      throwIfStale();

      const appliedCards = expandDecklistRowsInInputOrder(parsedAppliedRows);
      persistSmartToolHistory(
        "Smoke: Complete+Apply",
        smokeInputCards,
        {
          tool: "mana_tune",
          completion_result_v1: completeRoot,
          completion_status: completionStatus,
          applied_lines: parsedAppliedRows.length,
          resolve_names_count: resolveResultsCount,
        },
        "SMART_TOOL_MANA_TUNE_SMOKE",
        resolvedSnapshotId,
        resolvedCommander,
        {
          toolType: "mana_tune",
          inputDeckText: smokeDeckText,
          outputDeckText: completedDecklistText,
          inputCards: smokeInputCards,
          outputCards: appliedCards,
          summaryCounts: {
            applied_lines: parsedAppliedRows.length,
            resolve_names_count: resolveResultsCount,
          },
        },
      );

      const toast = `Smoke pass: deck now has ${parsedAppliedRows.length} lines (was ${smokeLineCountBefore})`;
      setValidationMessage(toast);
      setToastMessage(toast);
      setLastSmokeSucceeded(true);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown smoke test runtime error";
      if (message === DEV_SMOKE_STALE_REQUEST_MESSAGE || !isActiveRequest()) {
        return;
      }
      setApiErrorDetails((previous: ApiErrorDetails | null) => {
        if (previous) {
          return previous;
        }
        return buildSyntheticApiErrorDetails(
          "/dev/smoke/complete_apply",
          500,
          {
            status: "ERROR",
            message,
          },
          {
            method: "POST",
            requestPayload: smokeErrorFallbackPayload,
            requestDebug: smokeRequestDebug,
            error,
          },
        );
      });
      setRuntimeError(message);
      setCompletionError(message);
      setValidationMessage("Smoke failed");
      setToastMessage("Smoke failed");
      setLastSmokeSucceeded(false);
      setWorkspaceMode("ANALYZE");
      setErrorDetailsOpenSignal((previous: number) => previous + 1);
    } finally {
      if (isActiveRequest()) {
        setRunningDevSmokeCompleteApply(false);
      }
    }
  }

  async function handlePowerTuneTool(): Promise<void> {
    setActiveTool("POWER_TUNE");
    setRunningSmartTune(true);
    setLastTuneSucceeded(false);
    setRuntimeError(null);
    setApiErrorDetails(null);
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
          body: JSON.stringify(payload),
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

    const lastErrorPayload =
      apiErrorDetails === null
        ? null
        : {
            endpoint: apiErrorDetails.endpoint,
            status_code: apiErrorDetails.statusCode,
            method: apiErrorDetails.method || "POST",
            request_id: apiErrorDetails.requestId,
            request_payload_text: apiErrorDetails.requestPayloadText || "",
            request_body_text: apiErrorDetails.requestBodyText || apiErrorDetails.requestPayloadText || "",
            request_debug_text: apiErrorDetails.requestDebugText || "",
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

  function markPreviewImageFailure(imageUrl: string): void {
    setPreviewImageFailures((previous: Record<string, true>) => {
      if (previous[imageUrl]) {
        return previous;
      }
      return {
        ...previous,
        [imageUrl]: true,
      };
    });
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
      <LeftRail />

      <main className="workspace-main-content">
        <div className="workspace-shell">
          <header className="workspace-header">
            <p className="workspace-kicker">MTG Engine Harness · Phase 2</p>
            <h1>Active Deck Workspace</h1>
            <p className="workspace-subtitle">Local-first deck input → build → analysis loop with deterministic rendering.</p>
          </header>

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
                {isEditMode ? (
                  <DeckEditorPanel
                    apiBase={apiBase}
                    snapshotId={snapshotId}
                    cardsInput={deckText}
                    parsedDeckRows={parsedDeckRows}
                    deckLineCount={deckTextLineCount}
                    deckTextRevision={deckTextRevision}
                    cardHintsByName={deckEditorCardHints}
                    onCardsInputChange={(value: string) => {
                      applyDeckText(value, "deck_editor_input");
                    }}
                    onHoverCard={setHoverCard}
                    onResolveNamesMissingChange={setResolveNamesMissingNames}
                    onOpenCard={openCardModal}
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
                        aria-selected={activeTool === "MANA_TUNE"}
                        className={`workspace-tool-action-button ${activeTool === "MANA_TUNE" ? "workspace-tool-action-button-active" : ""}`}
                        onClick={() => {
                          setActiveTool("MANA_TUNE");
                        }}
                      >
                        {runningSmartComplete ? "Mana Tune..." : "Mana Tune"}
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

                    {isDevMode ? (
                      <div className="workspace-tool-dev-row">
                        <button
                          type="button"
                          className="workspace-tool-dev-button"
                          onClick={() => {
                            void handleDevSmokeTestCompleteApply();
                          }}
                          disabled={isAnyToolRunning}
                        >
                          {runningDevSmokeCompleteApply ? "Running Smoke..." : "Smoke: Complete+Apply"}
                        </button>
                      </div>
                    ) : null}

                    <section className="workspace-tool-results" aria-live="polite">
                    {activeTool === "MANA_TUNE" ? (
                      <section className="workspace-tool-panel">
                        <div className="workspace-tool-panel-header">
                          <h3>Mana Tune</h3>
                          <button
                            type="button"
                            className="workspace-tool-run-button"
                            onClick={() => {
                              void handleManaTuneTool();
                            }}
                            disabled={runningSmartComplete || isAnyToolRunning}
                          >
                            {runningSmartComplete ? "Running Mana Tune..." : "Run Mana Tune"}
                          </button>
                        </div>
                        {!completionResult ? (
                          <p className="workspace-muted">Run Mana Tune to view additions and apply the completed deck.</p>
                        ) : (
                          <>
                            <h4>Completion Result</h4>
                            <div className="workspace-chip-row">
                              <span className="workspace-chip">status: {asString(completionResult.status) || "(missing)"}</span>
                              <span className="workspace-chip">cards_added_count: {completionCardsAddedCount}</span>
                              {hasCompletionLandsAddedCount ? (
                                <span className="workspace-chip">lands_added_count: {completionLandsAddedCount}</span>
                              ) : null}
                              <span className="workspace-chip">final_deck_size: {completionDecklistCounts.totalCount}</span>
                            </div>

                            {completeAddedRows.length > 0 ? (
                              <ul className="workspace-compact-list workspace-scroll-list">
                                {completeAddedRows.map((row: DeckCompleteAddedCardV1, index: number) => {
                                  const cardName = asString(row.name) || "(unnamed add)";
                                  const reasons = asStringArray(row.reasons_v1).slice().sort();
                                  const primitives = asStringArray(row.primitives_added_v1).slice().sort();
                                  const cardHint = deckEditorCardHints[normalizeNameToken(cardName)];
                                  return (
                                    <li
                                      key={`tool-complete-add-${index}`}
                                      onMouseEnter={() => {
                                        setHoverCard({
                                          name: cardName,
                                          oracle_id: cardHint?.oracleId || "",
                                          type_line: cardHint?.typeLine || null,
                                          primitive_tags: primitives,
                                          source: "deck",
                                        });
                                      }}
                                      onMouseLeave={() => {
                                        setHoverCard(null);
                                      }}
                                    >
                                      <strong>{cardName}</strong>
                                      {reasons.length > 0 ? <div className="workspace-muted">why: {reasons.join(", ")}</div> : null}
                                      {primitives.length > 0 ? <div className="workspace-muted">primitives: {primitives.join(", ")}</div> : null}
                                    </li>
                                  );
                                })}
                              </ul>
                            ) : (
                              <p className="workspace-muted">No added_cards_v1 rows were returned.</p>
                            )}

                            <details className="workspace-collapsible">
                              <summary>Completed decklist text</summary>
                              <textarea
                                className="workspace-readonly-textarea"
                                value={completedDecklistText}
                                readOnly
                                rows={Math.max(8, Math.min(completedDecklistText.split(/\r?\n/).length + 1, 24))}
                              />
                            </details>

                            <div className="workspace-action-row">
                              <button
                                type="button"
                                className="workspace-tool-run-button"
                                onClick={handleApplyCompletedDecklist}
                                disabled={completedDecklistText.trim() === "" || runningSmartComplete}
                              >
                                Apply Complete
                              </button>

                              <button
                                type="button"
                                onClick={() => {
                                  void handleCopyCompletedDecklist();
                                }}
                                disabled={completedDecklistText.trim() === ""}
                              >
                                Copy Completed Decklist
                              </button>

                              <button type="button" className="workspace-link-button" onClick={handleDismissCompletionResult}>
                                Dismiss
                              </button>
                            </div>
                          </>
                        )}
                      </section>
                    ) : null}

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
                                    {reasons.length > 0 ? <div className="workspace-muted">why: {reasons.join(", ")}</div> : null}
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
                              Complete works (smoke button last run success)
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

            {isEditMode || isToolsMode ? (
            <aside className="workspace-col-right" aria-label="Card art preview dock">
              <div className="workspace-col-right-inner">
                <ArtDock
                  hoverCard={hoverCard}
                  onClear={() => {
                    setHoverCard(null);
                  }}
                  previewImageFailures={previewImageFailures}
                  markPreviewImageFailure={markPreviewImageFailure}
                />
              </div>
            </aside>
            ) : null}
          </div>
        </div>
      </main>

      <CardModal
        isOpen={isCardModalOpen}
        oracleId={cardModalOracleId}
        oracleIds={cardModalList}
        index={cardModalIndex}
        onClose={closeCardModal}
        onPrev={cardModalList.length > 1 ? goPrev : undefined}
        onNext={cardModalList.length > 1 ? goNext : undefined}
      />

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

      {toastMessage ? (
        <div className="workspace-toast" role="status" aria-live="polite">
          {toastMessage}
        </div>
      ) : null}
    </div>
  );
}
