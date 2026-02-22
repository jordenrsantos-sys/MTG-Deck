import { FormEvent, useEffect, useMemo, useState } from "react";

type ValidateRequestPayload = {
  db_snapshot_id: string;
  raw_decklist_text: string;
  format: "commander";
  profile_id: string;
  bracket_id: string;
  commander?: string;
  name_overrides_v1?: NameOverrideV1[];
};

type NameOverrideV1 = {
  name_raw: string;
  resolved_oracle_id?: string;
  resolved_name?: string;
};

type UnknownCandidate = {
  oracle_id?: string;
  name?: string;
};

type UnknownRow = {
  name_raw?: string;
  name_norm?: string;
  count?: number;
  line_no?: number;
  reason_code?: string;
  candidates?: UnknownCandidate[];
};

type DeckValidateViolationV1 = {
  code?: string;
  card_name?: string;
  count?: number;
  line_nos?: number[];
  message?: string;
};

type ValidateResponsePayload = {
  status?: string;
  db_snapshot_id?: string;
  format?: string;
  canonical_deck_input?: Record<string, unknown>;
  unknowns?: UnknownRow[];
  parse_version?: string;
  resolve_version?: string;
  ingest_version?: string;
};

type BuildResponsePayload = {
  engine_version?: string;
  ruleset_version?: string;
  db_snapshot_id?: string;
  profile_id?: string;
  bracket_id?: string;
  status?: string;
  build_hash_v1?: string | null;
  request_hash_v1?: string | null;
  result?: Record<string, unknown>;
};

type DeckTuneRequestV1 = {
  db_snapshot_id: string;
  raw_decklist_text: string;
  format: "commander";
  profile_id: string;
  bracket_id: string;
  mulligan_model_id: string;
  max_swaps: number;
  commander?: string;
  name_overrides_v1?: NameOverrideV1[];
};

type DeckTuneSwapDeltaSummaryV1 = {
  total_score_delta_v1?: number;
  coherence_delta_v1?: number;
  primitive_coverage_delta_v1?: number;
  gc_compliance_preserved_v1?: boolean;
};

type DeckTuneSwapV1 = {
  cut_name?: string;
  add_name?: string;
  reasons_v1?: string[];
  delta_summary_v1?: DeckTuneSwapDeltaSummaryV1;
};

type DeckTuneResponseV1 = {
  status?: string;
  db_snapshot_id?: string;
  format?: string;
  baseline_summary_v1?: Record<string, unknown>;
  recommended_swaps_v1?: DeckTuneSwapV1[];
  request_hash_v1?: string;
  unknowns?: UnknownRow[];
  parse_version?: string;
  resolve_version?: string;
  ingest_version?: string;
  tune_engine_version?: string;
  dev_metrics_v1?: Record<string, unknown>;
};

type DeckCompleteLandMode = "AUTO" | "NONE";

type DeckCompleteRequestV1 = {
  db_snapshot_id: string;
  raw_decklist_text: string;
  format: "commander";
  profile_id: string;
  bracket_id: string;
  mulligan_model_id: string;
  target_deck_size: number;
  max_adds: number;
  allow_basic_lands: boolean;
  land_target_mode: DeckCompleteLandMode;
  commander?: string;
  name_overrides_v1?: NameOverrideV1[];
};

type DeckCompleteAddedCardV1 = {
  name?: string;
  reasons_v1?: string[];
  primitives_added_v1?: string[];
};

type DeckCompleteResponseV1 = {
  status?: string;
  db_snapshot_id?: string;
  format?: string;
  baseline_summary_v1?: Record<string, unknown>;
  added_cards_v1?: DeckCompleteAddedCardV1[];
  completed_decklist_text_v1?: string;
  request_hash_v1?: string;
  unknowns?: UnknownRow[];
  violations_v1?: DeckValidateViolationV1[];
  parse_version?: string;
  resolve_version?: string;
  ingest_version?: string;
  complete_engine_version?: string;
  dev_metrics_v1?: Record<string, unknown>;
};

type CompleteRequestConfig = {
  target_deck_size: number;
  max_adds: number;
  allow_basic_lands: boolean;
  land_target_mode: DeckCompleteLandMode;
};

type InFlightAction = "VALIDATE" | "BUILD" | "TUNE" | "COMPLETE";

type InFlightState = {
  action: InFlightAction;
  startedAtMs: number;
  stageText: string;
};

type RunHistoryEntry = {
  run_key: string;
  build_hash_v1: string;
  request_hash_v1: string;
  db_snapshot_id: string;
  profile_id: string;
  bracket_id: string;
  status: string;
};

const API_BASE =
  (import.meta.env.VITE_API_BASE_URL && import.meta.env.VITE_API_BASE_URL.trim()) ||
  `http://${window.location.hostname}:8000`;

async function debugFetch(url: string, options: RequestInit): Promise<Response> {
  if (import.meta.env.DEV) {
    console.group("API CALL");
    console.log("URL:", url);
    console.log("Method:", options.method ?? "GET");
    console.log("Payload:", options.body ?? null);
  }

  try {
    const res = await fetch(url, options);

    if (import.meta.env.DEV) {
      const debugJson = await res.clone().json().catch(() => null);
      console.log("Status:", res.status);
      console.log("Headers:", Object.fromEntries(res.headers.entries()));
      if (res.ok) {
        console.log("Response JSON:", debugJson);
      }
      console.groupEnd();
    }

    return res;
  } catch (err) {
    if (import.meta.env.DEV) {
      console.error("FETCH ERROR:", err);
      console.groupEnd();
    }
    throw err;
  }
}

const DEFAULT_FORM = {
  db_snapshot_id: "20260217_190902",
  profile_id: "focused",
  bracket_id: "B2",
  commander_override: "",
  raw_decklist_text: "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet\n",
};

const MULLIGAN_MODEL_OPTIONS = ["NORMAL"] as const;
type MulliganModelId = (typeof MULLIGAN_MODEL_OPTIONS)[number];
const COMPLETE_LAND_MODE_OPTIONS = ["AUTO", "NONE"] as const;
const DEFAULT_MAX_SWAPS = 5;
const DEFAULT_COMPLETE_TARGET_DECK_SIZE = 100;
const DEFAULT_COMPLETE_MAX_ADDS = 200;
const DEFAULT_COMPLETE_ALLOW_BASIC_LANDS = true;
const DEFAULT_COMPLETE_LAND_TARGET_MODE: DeckCompleteLandMode = "AUTO";

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function asUnknownRows(value: unknown): UnknownRow[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown) => Boolean(asRecord(entry))) as UnknownRow[];
}

function asDeckValidateViolations(value: unknown): DeckValidateViolationV1[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown) => Boolean(asRecord(entry))) as DeckValidateViolationV1[];
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry: unknown): entry is string => typeof entry === "string");
}

function asNumber(value: unknown, fallback = 0): number {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return fallback;
  }
  return value;
}

function asInteger(value: unknown, fallback = 0): number {
  const numeric = asNumber(value, fallback);
  return Number.isFinite(numeric) ? Math.trunc(numeric) : fallback;
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

function applySwapsDeterministically(cards: string[], swaps: DeckTuneSwapV1[], applyCount: number): string[] {
  const out = cards.slice();
  const boundedApplyCount = clampInteger(applyCount, 0, swaps.length);

  for (let idx = 0; idx < boundedApplyCount; idx += 1) {
    const swap = swaps[idx] ?? {};
    const cutName = asString(swap.cut_name).trim();
    const addName = asString(swap.add_name).trim();
    if (addName === "") {
      continue;
    }

    if (cutName !== "") {
      const cutIndex = out.findIndex((name: string) => name === cutName);
      if (cutIndex >= 0) {
        out[cutIndex] = addName;
        continue;
      }
    }

    out.push(addName);
  }

  return out;
}

function buildCommanderDecklistText(commander: string, cards: string[]): string {
  const lines: string[] = ["Commander", `1 ${commander}`, "Deck"];
  for (const cardName of cards) {
    const token = cardName.trim();
    if (token !== "") {
      lines.push(`1 ${token}`);
    }
  }
  return lines.join("\n");
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
    if (/^\d+\s+.+$/.test(line)) {
      if (section === "COMMANDER") {
        commanderCount += 1;
      } else if (section === "DECK") {
        deckCount += 1;
      }
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

async function copyTextToClipboard(value: string): Promise<void> {
  if (typeof navigator !== "undefined" && navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(textarea);
  if (!copied) {
    throw new Error("Clipboard copy failed.");
  }
}

function normalizeNameToken(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function sanitizeNameOverride(value: NameOverrideV1): NameOverrideV1 | null {
  const nameRaw = value.name_raw.trim().replace(/\s+/g, " ");
  if (nameRaw === "") {
    return null;
  }

  const resolvedOracleId = typeof value.resolved_oracle_id === "string" ? value.resolved_oracle_id.trim() : "";
  const resolvedName = typeof value.resolved_name === "string" ? value.resolved_name.trim().replace(/\s+/g, " ") : "";

  if (resolvedOracleId !== "") {
    return {
      name_raw: nameRaw,
      resolved_oracle_id: resolvedOracleId,
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
  const sanitized = overrides
    .map((row: NameOverrideV1) => sanitizeNameOverride(row))
    .filter((row: NameOverrideV1 | null): row is NameOverrideV1 => row !== null)
    .sort((a: NameOverrideV1, b: NameOverrideV1) => {
      const aNameNorm = normalizeNameToken(a.name_raw);
      const bNameNorm = normalizeNameToken(b.name_raw);
      if (aNameNorm !== bNameNorm) {
        return aNameNorm < bNameNorm ? -1 : 1;
      }

      const aNameRawFolded = a.name_raw.toLowerCase();
      const bNameRawFolded = b.name_raw.toLowerCase();
      if (aNameRawFolded !== bNameRawFolded) {
        return aNameRawFolded < bNameRawFolded ? -1 : 1;
      }
      if (a.name_raw !== b.name_raw) {
        return a.name_raw < b.name_raw ? -1 : 1;
      }

      const aOracle = typeof a.resolved_oracle_id === "string" ? a.resolved_oracle_id : "";
      const bOracle = typeof b.resolved_oracle_id === "string" ? b.resolved_oracle_id : "";
      if (aOracle !== bOracle) {
        return aOracle < bOracle ? -1 : 1;
      }

      const aResolvedName = typeof a.resolved_name === "string" ? a.resolved_name : "";
      const bResolvedName = typeof b.resolved_name === "string" ? b.resolved_name : "";
      const aResolvedNameFolded = aResolvedName.toLowerCase();
      const bResolvedNameFolded = bResolvedName.toLowerCase();
      if (aResolvedNameFolded !== bResolvedNameFolded) {
        return aResolvedNameFolded < bResolvedNameFolded ? -1 : 1;
      }
      if (aResolvedName !== bResolvedName) {
        return aResolvedName < bResolvedName ? -1 : 1;
      }

      return 0;
    });

  const dedup = new Set<string>();
  const normalized: NameOverrideV1[] = [];
  for (const row of sanitized) {
    const key = normalizeNameToken(row.name_raw);
    if (dedup.has(key)) {
      continue;
    }
    dedup.add(key);
    normalized.push(row);
  }

  return normalized;
}

function buildValidateInputKey(payload: ValidateRequestPayload): string {
  const normalizedOverrides = normalizeNameOverrides(payload.name_overrides_v1 ?? []);
  const commander = typeof payload.commander === "string" ? payload.commander.trim() : "";
  return JSON.stringify({
    db_snapshot_id: payload.db_snapshot_id,
    raw_decklist_text: payload.raw_decklist_text,
    format: payload.format,
    profile_id: payload.profile_id,
    bracket_id: payload.bracket_id,
    commander: commander === "" ? null : commander,
    name_overrides_v1: normalizedOverrides,
  });
}

function upsertNameOverride(overrides: NameOverrideV1[], nextOverride: NameOverrideV1): NameOverrideV1[] {
  const key = normalizeNameToken(nextOverride.name_raw);
  const filtered = overrides.filter((row: NameOverrideV1) => normalizeNameToken(row.name_raw) !== key);
  filtered.push(nextOverride);
  return normalizeNameOverrides(filtered);
}

function removeNameOverride(overrides: NameOverrideV1[], nameRaw: string): NameOverrideV1[] {
  const key = normalizeNameToken(nameRaw);
  return normalizeNameOverrides(overrides.filter((row: NameOverrideV1) => normalizeNameToken(row.name_raw) !== key));
}

function sortedJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry: unknown) => sortedJson(entry));
  }

  const rec = asRecord(value);
  if (!rec) {
    return value;
  }

  const out: Record<string, unknown> = {};
  for (const key of Object.keys(rec).sort()) {
    out[key] = sortedJson(rec[key]);
  }
  return out;
}

function JsonViewer({ value }: { value: unknown }) {
  return <pre>{JSON.stringify(sortedJson(value), null, 2)}</pre>;
}

function normalizeRunHistory(entries: RunHistoryEntry[], nextEntry: RunHistoryEntry): RunHistoryEntry[] {
  const dedup = new Map<string, RunHistoryEntry>();
  dedup.set(nextEntry.run_key, nextEntry);
  for (const entry of entries) {
    if (!dedup.has(entry.run_key)) {
      dedup.set(entry.run_key, entry);
    }
  }
  return Array.from(dedup.values()).slice(0, 20);
}

function panelKeyToPayloadKey(panelKey: string): string {
  if (panelKey.startsWith("has_")) {
    return panelKey.slice(4);
  }
  return panelKey;
}

export default function EngineViewerV0() {
  const [dbSnapshotId, setDbSnapshotId] = useState(DEFAULT_FORM.db_snapshot_id);
  const [profileId, setProfileId] = useState(DEFAULT_FORM.profile_id);
  const [bracketId, setBracketId] = useState(DEFAULT_FORM.bracket_id);
  const [commanderOverride, setCommanderOverride] = useState(DEFAULT_FORM.commander_override);
  const [rawDecklistText, setRawDecklistText] = useState(DEFAULT_FORM.raw_decklist_text);
  const [mulliganModelId, setMulliganModelId] = useState<MulliganModelId>(MULLIGAN_MODEL_OPTIONS[0]);
  const [maxSwaps, setMaxSwaps] = useState(DEFAULT_MAX_SWAPS);
  const [completeTargetDeckSize, setCompleteTargetDeckSize] = useState(DEFAULT_COMPLETE_TARGET_DECK_SIZE);
  const [completeMaxAdds, setCompleteMaxAdds] = useState(DEFAULT_COMPLETE_MAX_ADDS);
  const [completeAllowBasicLands, setCompleteAllowBasicLands] = useState(DEFAULT_COMPLETE_ALLOW_BASIC_LANDS);
  const [completeLandTargetMode, setCompleteLandTargetMode] = useState<DeckCompleteLandMode>(
    DEFAULT_COMPLETE_LAND_TARGET_MODE,
  );

  const [validateResponse, setValidateResponse] = useState<ValidateResponsePayload | null>(null);
  const [buildResponse, setBuildResponse] = useState<BuildResponsePayload | null>(null);
  const [deckTuneResponse, setDeckTuneResponse] = useState<DeckTuneResponseV1 | null>(null);
  const [deckCompleteResponse, setDeckCompleteResponse] = useState<DeckCompleteResponseV1 | null>(null);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [loadingValidate, setLoadingValidate] = useState(false);
  const [loadingBuild, setLoadingBuild] = useState(false);
  const [loadingTune, setLoadingTune] = useState(false);
  const [loadingComplete, setLoadingComplete] = useState(false);
  const [selectedPanelKey, setSelectedPanelKey] = useState<string | null>(null);
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [nameOverridesV1, setNameOverridesV1] = useState<NameOverrideV1[]>([]);
  const [lastValidatedInputKey, setLastValidatedInputKey] = useState<string | null>(null);
  const [copySwapCount, setCopySwapCount] = useState(0);
  const [copyNotice, setCopyNotice] = useState<string | null>(null);
  const [copyCompleteNotice, setCopyCompleteNotice] = useState<string | null>(null);
  const [lastCompleteRequestConfig, setLastCompleteRequestConfig] = useState<CompleteRequestConfig | null>(null);
  const [inFlight, setInFlight] = useState<InFlightState | null>(null);
  const [inFlightElapsedMs, setInFlightElapsedMs] = useState(0);
  const [lastActionElapsedMs, setLastActionElapsedMs] = useState(0);

  const validateUnknowns = asUnknownRows(validateResponse?.unknowns);
  const validateStatus = asString(validateResponse?.status);
  const hasUnknowns = validateUnknowns.length > 0;
  const currentValidateInputKey = useMemo(
    () => buildValidateInputKey(buildValidatePayload(nameOverridesV1)),
    [dbSnapshotId, rawDecklistText, profileId, bracketId, commanderOverride, nameOverridesV1],
  );
  const validationIsCurrent = lastValidatedInputKey !== null && lastValidatedInputKey === currentValidateInputKey;
  const canTune = validationIsCurrent && validateStatus === "OK" && !hasUnknowns;
  const tuneBlockedReason =
    !validationIsCurrent
      ? "Validate current decklist before tuning."
      : hasUnknowns
      ? "Resolve unknown cards before tuning."
      : validateStatus !== "OK"
      ? `Validation status must be OK (current: ${validateStatus || "UNKNOWN"}).`
      : "";

  const canonicalDeckInput = asRecord(validateResponse?.canonical_deck_input) ?? {};
  const canonicalCommander = asString(canonicalDeckInput.commander);
  const canonicalCards = asStringArray(canonicalDeckInput.cards);

  const tuneSwapRows = asDeckTuneSwapRows(deckTuneResponse?.recommended_swaps_v1);
  const tuneSummary = asRecord(deckTuneResponse?.baseline_summary_v1) ?? {};
  const tuneStatus = asString(deckTuneResponse?.status);
  const tuneDevMetrics = asRecord(deckTuneResponse?.dev_metrics_v1);
  const tuneProtectedCutCount = asInteger(tuneDevMetrics?.protected_cut_count, 0);
  const tuneProtectedCutNamesTop10 = asStringArray(tuneDevMetrics?.protected_cut_names_top10);
  const effectiveCopySwapCount = clampInteger(copySwapCount, 0, tuneSwapRows.length);
  const completeAddedRows = asDeckCompleteAddedRows(deckCompleteResponse?.added_cards_v1);
  const completeStatus = asString(deckCompleteResponse?.status);
  const completedDecklistText = asString(deckCompleteResponse?.completed_decklist_text_v1);
  const completeUnknowns = asUnknownRows(deckCompleteResponse?.unknowns);
  const completeViolations = asDeckValidateViolations(deckCompleteResponse?.violations_v1);
  const completeDevMetrics = asRecord(deckCompleteResponse?.dev_metrics_v1);
  const completeDecklistCounts = useMemo(
    () => parseCompletedDecklistCounts(completedDecklistText),
    [completedDecklistText],
  );
  const completeLandsAdded = useMemo(
    () => completeAddedRows.filter((row: DeckCompleteAddedCardV1) => isLikelyLandAddition(row)).length,
    [completeAddedRows],
  );
  const requestedCompleteTargetDeckSize =
    lastCompleteRequestConfig?.target_deck_size ??
    clampInteger(asInteger(completeTargetDeckSize, DEFAULT_COMPLETE_TARGET_DECK_SIZE), 1, 300);
  const completeNeedsAttention =
    deckCompleteResponse !== null &&
    (completeStatus !== "OK" || completeDecklistCounts.totalCount < requestedCompleteTargetDeckSize);

  useEffect(() => {
    setCopySwapCount(tuneSwapRows.length);
    setCopyNotice(null);
  }, [deckTuneResponse, tuneSwapRows.length]);

  useEffect(() => {
    if (!inFlight) {
      return;
    }

    const updateElapsed = () => {
      setInFlightElapsedMs(Math.max(performance.now() - inFlight.startedAtMs, 0));
    };

    updateElapsed();
    const timerId = window.setInterval(updateElapsed, 120);
    return () => {
      window.clearInterval(timerId);
    };
  }, [inFlight]);

  function startInFlight(action: InFlightAction, stageText: string) {
    const startedAtMs = performance.now();
    setInFlight({ action, startedAtMs, stageText });
    setInFlightElapsedMs(0);
  }

  function updateInFlightStage(stageText: string) {
    setInFlight((previous: InFlightState | null) =>
      previous
        ? {
            ...previous,
            stageText,
          }
        : previous,
    );
  }

  function finishInFlight() {
    setInFlight((previous: InFlightState | null) => {
      if (previous) {
        const elapsed = Math.max(performance.now() - previous.startedAtMs, 0);
        setLastActionElapsedMs(elapsed);
      }
      return null;
    });
    setInFlightElapsedMs(0);
  }

  const availablePanels = useMemo(() => {
    const result = asRecord(buildResponse?.result);
    const panelRecord = asRecord(result?.available_panels_v1);
    if (!panelRecord) {
      return [] as Array<{ key: string; enabled: boolean }>;
    }

    return Object.keys(panelRecord)
      .sort()
      .map((key: string) => ({
        key,
        enabled: panelRecord[key] === true,
      }));
  }, [buildResponse]);

  const selectedPanelPayload = useMemo(() => {
    if (!selectedPanelKey) {
      return null;
    }
    const result = asRecord(buildResponse?.result);
    if (!result) {
      return null;
    }
    const payloadKey = panelKeyToPayloadKey(selectedPanelKey);
    if (!(payloadKey in result)) {
      return null;
    }
    return {
      payloadKey,
      payload: result[payloadKey],
    };
  }, [buildResponse, selectedPanelKey]);

  async function postJson(path: string, payload: unknown): Promise<unknown> {
    const requestUrl = `${API_BASE}${path}`;
    const requestBody = JSON.stringify(payload);

    try {
      const res = await debugFetch(requestUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: requestBody,
      });

      const text = await res.text();
      let parsed: unknown = null;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = null;
      }

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${text}`);
      }

      return parsed;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown runtime error";
      throw new Error(`${errorMessage} (API_BASE=${API_BASE}, path=${path})`);
    }
  }

  function buildValidatePayload(nameOverrides: NameOverrideV1[]): ValidateRequestPayload {
    const payload: ValidateRequestPayload = {
      db_snapshot_id: dbSnapshotId,
      raw_decklist_text: rawDecklistText,
      format: "commander",
      profile_id: profileId,
      bracket_id: bracketId,
    };

    const commander = commanderOverride.trim();
    if (commander !== "") {
      payload.commander = commander;
    }

    const normalizedOverrides = normalizeNameOverrides(nameOverrides);
    if (normalizedOverrides.length > 0) {
      payload.name_overrides_v1 = normalizedOverrides;
    }

    return payload;
  }

  function buildCompletePayload(nameOverrides: NameOverrideV1[]): DeckCompleteRequestV1 {
    const targetDeckSize = clampInteger(asInteger(completeTargetDeckSize, DEFAULT_COMPLETE_TARGET_DECK_SIZE), 1, 300);
    const maxAdds = clampInteger(asInteger(completeMaxAdds, DEFAULT_COMPLETE_MAX_ADDS), 1, 500);
    const landTargetMode: DeckCompleteLandMode = completeLandTargetMode === "NONE" ? "NONE" : "AUTO";
    const payload: DeckCompleteRequestV1 = {
      db_snapshot_id: dbSnapshotId,
      raw_decklist_text: rawDecklistText,
      format: "commander",
      profile_id: profileId,
      bracket_id: bracketId,
      mulligan_model_id: mulliganModelId,
      target_deck_size: targetDeckSize,
      max_adds: maxAdds,
      allow_basic_lands: Boolean(completeAllowBasicLands),
      land_target_mode: landTargetMode,
    };

    const commander = commanderOverride.trim();
    if (commander !== "") {
      payload.commander = commander;
    }

    const normalizedOverrides = normalizeNameOverrides(nameOverrides);
    if (normalizedOverrides.length > 0) {
      payload.name_overrides_v1 = normalizedOverrides;
    }

    return payload;
  }

  function buildTunePayload(nameOverrides: NameOverrideV1[]): DeckTuneRequestV1 {
    const payload: DeckTuneRequestV1 = {
      db_snapshot_id: dbSnapshotId,
      raw_decklist_text: rawDecklistText,
      format: "commander",
      profile_id: profileId,
      bracket_id: bracketId,
      mulligan_model_id: mulliganModelId,
      max_swaps: clampInteger(asInteger(maxSwaps, DEFAULT_MAX_SWAPS), 1, 50),
    };

    const commander = commanderOverride.trim();
    if (commander !== "") {
      payload.commander = commander;
    }

    const normalizedOverrides = normalizeNameOverrides(nameOverrides);
    if (normalizedOverrides.length > 0) {
      payload.name_overrides_v1 = normalizedOverrides;
    }

    return payload;
  }

  async function executeValidate(
    nameOverrides: NameOverrideV1[] = nameOverridesV1,
  ): Promise<{ response: ValidateResponsePayload; inputKey: string }> {
    const payload = buildValidatePayload(nameOverrides);
    const inputKey = buildValidateInputKey(payload);
    const parsed = await postJson("/deck/validate", payload);
    return {
      response: (asRecord(parsed) ?? {}) as ValidateResponsePayload,
      inputKey,
    };
  }

  async function revalidateWithOverrides(nextOverrides: NameOverrideV1[]) {
    setLoadingValidate(true);
    startInFlight("VALIDATE", "Validating decklist...");
    setRuntimeError(null);
    setBuildResponse(null);
    setDeckTuneResponse(null);
    setDeckCompleteResponse(null);
    setCopyNotice(null);
    setCopyCompleteNotice(null);

    try {
      const normalizedOverrides = normalizeNameOverrides(nextOverrides);
      setNameOverridesV1(normalizedOverrides);
      const { response, inputKey } = await executeValidate(normalizedOverrides);
      setValidateResponse(response);
      setLastValidatedInputKey(inputKey);
      setSelectedPanelKey(null);
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unknown runtime error");
      setValidateResponse(null);
      setLastValidatedInputKey(null);
    } finally {
      finishInFlight();
      setLoadingValidate(false);
    }
  }

  async function handleValidate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await revalidateWithOverrides(nameOverridesV1);
  }

  async function handleApplyOverride(row: UnknownRow, candidate: UnknownCandidate) {
    const nameRaw = asString(row.name_raw);
    if (nameRaw === "") {
      return;
    }

    const oracleId = asString(candidate.oracle_id);
    const resolvedName = asString(candidate.name);
    if (oracleId === "" && resolvedName === "") {
      return;
    }

    const nextOverride: NameOverrideV1 =
      oracleId !== ""
        ? {
            name_raw: nameRaw,
            resolved_oracle_id: oracleId,
          }
        : {
            name_raw: nameRaw,
            resolved_name: resolvedName,
          };

    const nextOverrides = upsertNameOverride(nameOverridesV1, nextOverride);
    await revalidateWithOverrides(nextOverrides);
  }

  async function handleRemoveOverride(nameRaw: string) {
    const nextOverrides = removeNameOverride(nameOverridesV1, nameRaw);
    await revalidateWithOverrides(nextOverrides);
  }

  async function handleBuild() {
    setLoadingBuild(true);
    startInFlight("BUILD", "Running pipeline...");
    setRuntimeError(null);
    setDeckTuneResponse(null);
    setDeckCompleteResponse(null);
    setCopyNotice(null);
    setCopyCompleteNotice(null);

    try {
      const { response: nextValidate, inputKey } = await executeValidate(nameOverridesV1);
      setValidateResponse(nextValidate);
      setLastValidatedInputKey(inputKey);

      const nextUnknowns = asUnknownRows(nextValidate.unknowns);
      if (nextUnknowns.length > 0 || nextValidate.status !== "OK") {
        setBuildResponse(null);
        setSelectedPanelKey(null);
        return;
      }

      updateInFlightStage("Running pipeline...");
      const canonicalDeckInput = asRecord(nextValidate.canonical_deck_input) ?? {};
      const buildParsed = await postJson("/build", canonicalDeckInput);
      const buildPayload = (asRecord(buildParsed) ?? {}) as BuildResponsePayload;
      setBuildResponse(buildPayload);

      const available = asRecord(buildPayload.result)?.available_panels_v1;
      const firstPanel = asRecord(available);
      if (firstPanel) {
        const panelKeys = Object.keys(firstPanel).sort();
        setSelectedPanelKey(panelKeys.length > 0 ? panelKeys[0] : null);
      } else {
        setSelectedPanelKey(null);
      }

      const buildHash = asString(buildPayload.build_hash_v1);
      const requestHash = asString(buildPayload.request_hash_v1);
      const runKey = buildHash !== "" ? buildHash : requestHash;
      if (runKey !== "") {
        const nextEntry: RunHistoryEntry = {
          run_key: runKey,
          build_hash_v1: buildHash,
          request_hash_v1: requestHash,
          db_snapshot_id: asString(buildPayload.db_snapshot_id),
          profile_id: asString(buildPayload.profile_id),
          bracket_id: asString(buildPayload.bracket_id),
          status: asString(buildPayload.status),
        };
        setRunHistory((prev: RunHistoryEntry[]) => normalizeRunHistory(prev, nextEntry));
      }
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unknown runtime error");
      setBuildResponse(null);
      setSelectedPanelKey(null);
      setLastValidatedInputKey(null);
    } finally {
      finishInFlight();
      setLoadingBuild(false);
    }
  }

  async function handleTune() {
    setLoadingTune(true);
    startInFlight("TUNE", "Evaluating swaps...");
    setRuntimeError(null);
    setCopyNotice(null);

    try {
      const { response: nextValidate, inputKey } = await executeValidate(nameOverridesV1);
      setValidateResponse(nextValidate);
      setLastValidatedInputKey(inputKey);

      const nextUnknowns = asUnknownRows(nextValidate.unknowns);
      if (nextUnknowns.length > 0) {
        setRuntimeError("Cannot tune deck while unknown cards are unresolved.");
        setDeckTuneResponse(null);
        return;
      }

      const nextStatus = asString(nextValidate.status);
      if (nextStatus !== "OK") {
        setRuntimeError(`Cannot tune deck: validation status must be OK (current: ${nextStatus || "UNKNOWN"}).`);
        setDeckTuneResponse(null);
        return;
      }

      updateInFlightStage("Evaluating swaps...");
      const tunePayload = buildTunePayload(nameOverridesV1);
      const tuneParsed = await postJson("/deck/tune_v1", tunePayload);
      const tunePayloadParsed = (asRecord(tuneParsed) ?? {}) as DeckTuneResponseV1;
      setDeckTuneResponse(tunePayloadParsed);
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unknown runtime error");
      setDeckTuneResponse(null);
    } finally {
      finishInFlight();
      setLoadingTune(false);
    }
  }

  async function handleComplete() {
    setLoadingComplete(true);
    startInFlight("COMPLETE", "Filling missing slots...");
    setRuntimeError(null);
    setCopyCompleteNotice(null);

    try {
      const { response: nextValidate, inputKey } = await executeValidate(nameOverridesV1);
      setValidateResponse(nextValidate);
      setLastValidatedInputKey(inputKey);

      const nextUnknowns = asUnknownRows(nextValidate.unknowns);
      if (nextUnknowns.length > 0) {
        setRuntimeError("Cannot complete deck while unknown cards are unresolved.");
        setDeckCompleteResponse(null);
        return;
      }

      const nextStatus = asString(nextValidate.status);
      if (nextStatus !== "OK") {
        setRuntimeError(`Cannot complete deck: validation status must be OK (current: ${nextStatus || "UNKNOWN"}).`);
        setDeckCompleteResponse(null);
        return;
      }

      updateInFlightStage("Filling missing slots...");
      const completePayload = buildCompletePayload(nameOverridesV1);
      setLastCompleteRequestConfig({
        target_deck_size: completePayload.target_deck_size,
        max_adds: completePayload.max_adds,
        allow_basic_lands: completePayload.allow_basic_lands,
        land_target_mode: completePayload.land_target_mode,
      });
      const completeParsed = await postJson("/deck/complete_v1", completePayload);
      const completePayloadParsed = (asRecord(completeParsed) ?? {}) as DeckCompleteResponseV1;
      setDeckCompleteResponse(completePayloadParsed);
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unknown runtime error");
      setDeckCompleteResponse(null);
    } finally {
      finishInFlight();
      setLoadingComplete(false);
    }
  }

  async function handleCopyTunedDecklist() {
    if (canonicalCommander === "") {
      setRuntimeError("Canonical commander is missing. Validate deck first.");
      return;
    }
    if (tuneSwapRows.length === 0) {
      setRuntimeError("No tune swaps available to copy.");
      return;
    }

    const tunedCards = applySwapsDeterministically(canonicalCards, tuneSwapRows, effectiveCopySwapCount);
    const decklistText = buildCommanderDecklistText(canonicalCommander, tunedCards);

    try {
      await copyTextToClipboard(decklistText);
      setCopyNotice(`Copied tuned decklist with ${effectiveCopySwapCount} swap(s).`);
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unable to copy tuned decklist.");
    }
  }

  async function handleCopyCompletedDecklist() {
    const completedDecklist = asString(deckCompleteResponse?.completed_decklist_text_v1);
    if (completedDecklist === "") {
      setRuntimeError("No completed decklist available to copy.");
      return;
    }

    try {
      await copyTextToClipboard(completedDecklist);
      setCopyCompleteNotice("Copied completed decklist.");
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unable to copy completed decklist.");
    }
  }

  const resultRecord = asRecord(buildResponse?.result);
  const buildPipelineStage = asString(resultRecord?.build_pipeline_stage);

  return (
    <div className="app-shell engine-viewer-v0">
      <section className="panel viewer-control-panel">
        <h1>Engine Viewer v0</h1>
        <form onSubmit={(event: FormEvent<HTMLFormElement>) => void handleValidate(event)}>
          <div className="form-grid">
            <label>
              DB Snapshot ID
              <input value={dbSnapshotId} onChange={(event) => setDbSnapshotId(event.target.value)} />
            </label>
            <label>
              Profile ID
              <input value={profileId} onChange={(event) => setProfileId(event.target.value)} />
            </label>
            <label>
              Bracket ID
              <input value={bracketId} onChange={(event) => setBracketId(event.target.value)} />
            </label>
            <label>
              Commander Override (optional)
              <input value={commanderOverride} onChange={(event) => setCommanderOverride(event.target.value)} />
            </label>
            <label className="full-width">
              Raw Decklist Text
              <textarea
                value={rawDecklistText}
                rows={10}
                onChange={(event) => setRawDecklistText(event.target.value)}
              />
            </label>
          </div>
          <div className="form-actions">
            <button type="submit" disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}>
              {loadingValidate ? "Validating..." : "Validate"}
            </button>
            <button
              type="button"
              onClick={() => void handleBuild()}
              disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete || hasUnknowns}
            >
              {loadingBuild ? "Building..." : "Build"}
            </button>
            <label className="inline-control">
              Mulligan Model
              <select
                value={mulliganModelId}
                onChange={(event) => setMulliganModelId(event.target.value as MulliganModelId)}
                disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
              >
                {MULLIGAN_MODEL_OPTIONS.map((option: MulliganModelId) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>
            <label className="inline-control">
              Max Swaps
              <input
                type="number"
                min={1}
                max={50}
                value={maxSwaps}
                onChange={(event) => setMaxSwaps(clampInteger(Number(event.target.value), 1, 50))}
                disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
              />
            </label>
            <label className="inline-control">
              Complete Target Size
              <input
                type="number"
                min={1}
                max={300}
                value={completeTargetDeckSize}
                onChange={(event) =>
                  setCompleteTargetDeckSize(clampInteger(Number(event.target.value), 1, 300))
                }
                disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
              />
            </label>
            <label className="inline-control">
              Complete Max Adds
              <input
                type="number"
                min={1}
                max={500}
                value={completeMaxAdds}
                onChange={(event) => setCompleteMaxAdds(clampInteger(Number(event.target.value), 1, 500))}
                disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
              />
            </label>
            <label className="inline-control">
              Land Fill Mode
              <select
                value={completeLandTargetMode}
                onChange={(event) => setCompleteLandTargetMode(event.target.value as DeckCompleteLandMode)}
                disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
              >
                {COMPLETE_LAND_MODE_OPTIONS.map((option: DeckCompleteLandMode) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>
            <label className="inline-control complete-checkbox-control">
              Allow Basic Lands
              <input
                type="checkbox"
                checked={completeAllowBasicLands}
                onChange={(event) => setCompleteAllowBasicLands(event.target.checked)}
                disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
              />
            </label>
            <button
              type="button"
              onClick={() => void handleTune()}
              disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete || !canTune}
            >
              {loadingTune ? "Tuning..." : "Tune Deck"}
            </button>
            <button
              type="button"
              onClick={() => void handleComplete()}
              disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete || !canTune}
            >
              {loadingComplete ? "Completing..." : "Complete to Target"}
            </button>
            {!canTune && tuneBlockedReason !== "" ? <span className="muted">{tuneBlockedReason}</span> : null}
          </div>
          {inFlight ? (
            <div className="inflight-progress-wrap" aria-live="polite">
              <div
                className="inflight-progress-bar"
                role="progressbar"
                aria-label={`${inFlight.action} progress`}
                aria-valuetext={inFlight.stageText}
              >
                <span className="inflight-progress-indeterminate" />
              </div>
              <div className="muted inflight-progress-meta">
                <span>{inFlight.stageText}</span>
                <span>Elapsed: {(inFlightElapsedMs / 1000).toFixed(1)}s</span>
              </div>
            </div>
          ) : lastActionElapsedMs > 0 ? (
            <p className="muted inflight-last-elapsed">Last action elapsed: {(lastActionElapsedMs / 1000).toFixed(1)}s</p>
          ) : null}
        </form>
        {runtimeError ? <p className="error">{runtimeError}</p> : null}
      </section>

      <div className="viewer-three-column">
        <aside className="panel viewer-column">
          <h2>Run History</h2>
          <p className="muted">Keyed by build_hash_v1.</p>
          {runHistory.length === 0 ? (
            <p className="muted">No successful builds yet.</p>
          ) : (
            <ul className="run-history-list">
              {runHistory.map((entry: RunHistoryEntry) => (
                <li key={entry.run_key}>
                  <div><strong>{entry.build_hash_v1}</strong></div>
                  <div>request_hash_v1: {entry.request_hash_v1}</div>
                  <div>{entry.db_snapshot_id}</div>
                  <div>{entry.profile_id} / {entry.bracket_id}</div>
                  <div>Status: {entry.status}</div>
                </li>
              ))}
            </ul>
          )}
        </aside>

        <section className="panel viewer-column">
          <h2>Build Header</h2>
          <div className="summary-grid">
            <div>engine_version: {asString(buildResponse?.engine_version) || "(missing)"}</div>
            <div>ruleset_version: {asString(buildResponse?.ruleset_version) || "(missing)"}</div>
            <div>db_snapshot_id: {asString(buildResponse?.db_snapshot_id) || "(missing)"}</div>
            <div>profile_id: {asString(buildResponse?.profile_id) || "(missing)"}</div>
            <div>bracket_id: {asString(buildResponse?.bracket_id) || "(missing)"}</div>
            <div>build_hash_v1: {asString(buildResponse?.build_hash_v1) || "(missing)"}</div>
            <div>request_hash_v1: {asString(buildResponse?.request_hash_v1) || "(missing)"}</div>
            <div>build_pipeline_stage: {buildPipelineStage || "(missing)"}</div>
          </div>

          <h3>Validate Response</h3>
          <JsonViewer value={validateResponse} />

          <h3>Unknown Queue</h3>
          <h4>Active Overrides</h4>
          {nameOverridesV1.length === 0 ? (
            <p className="muted">No active overrides.</p>
          ) : (
            <ul className="active-overrides-list">
              {nameOverridesV1.map((row: NameOverrideV1, index: number) => {
                const target = asString(row.resolved_oracle_id) || asString(row.resolved_name);
                return (
                  <li key={`override-${index}`}>
                    <div>
                      <strong>{row.name_raw}</strong> → {target || "(invalid)"}
                    </div>
                    <button
                      type="button"
                      onClick={() => void handleRemoveOverride(row.name_raw)}
                      disabled={loadingValidate || loadingBuild}
                    >
                      Remove
                    </button>
                  </li>
                );
              })}
            </ul>
          )}

          {validateUnknowns.length === 0 ? (
            <p className="muted">No unknowns. Build enabled.</p>
          ) : (
            <ul className="unknown-queue-list">
              {validateUnknowns.map((row: UnknownRow, index: number) => (
                <li key={`unknown-${index}`}>
                  <div><strong>{asString(row.reason_code) || "UNKNOWN"}</strong></div>
                  <div>name_raw: {asString(row.name_raw) || "(missing)"}</div>
                  <div>name_norm: {asString(row.name_norm) || "(missing)"}</div>
                  <div>line_no: {typeof row.line_no === "number" ? row.line_no : 0}</div>
                  <div>count: {typeof row.count === "number" ? row.count : 0}</div>
                  {Array.isArray(row.candidates) && row.candidates.length > 0 ? (
                    <div>
                      candidates:
                      <ul>
                        {row.candidates
                          .slice()
                          .sort((a: UnknownCandidate, b: UnknownCandidate) => {
                            const aOracle = asString(a.oracle_id);
                            const bOracle = asString(b.oracle_id);
                            if (aOracle !== bOracle) {
                              return aOracle.localeCompare(bOracle);
                            }
                            return asString(a.name).localeCompare(asString(b.name));
                          })
                          .map((candidate: UnknownCandidate, candidateIndex: number) => (
                            <li key={`candidate-${index}-${candidateIndex}`}>
                              <div className="candidate-row">
                                <span>{asString(candidate.oracle_id)} :: {asString(candidate.name)}</span>
                                <button
                                  type="button"
                                  onClick={() => void handleApplyOverride(row, candidate)}
                                  disabled={loadingValidate || loadingBuild}
                                >
                                  Use this
                                </button>
                              </div>
                            </li>
                          ))}
                      </ul>
                    </div>
                  ) : null}
                </li>
              ))}
            </ul>
          )}

          <h3>Deck Tune v1</h3>
          {deckTuneResponse ? (
            <div className="tune-results-block">
              <div className="tune-status-row">
                <strong>status: {tuneStatus || "(missing)"}</strong>
                <span>request_hash_v1: {asString(deckTuneResponse.request_hash_v1) || "(missing)"}</span>
                <span>tune_engine_version: {asString(deckTuneResponse.tune_engine_version) || "(missing)"}</span>
              </div>

              <div className="tune-chip-grid">
                <span className="tune-chip">dead_slots: {asInteger(tuneSummary.dead_slot_count_v1, 0)}</span>
                <span className="tune-chip">missing_required: {asInteger(tuneSummary.missing_required_count_v1, 0)}</span>
                <span className="tune-chip">
                  primitive_concentration: {asNumber(tuneSummary.primitive_concentration_index_v1, 0).toFixed(3)}
                </span>
                <span className="tune-chip">gc_count: {asInteger(tuneSummary.game_changers_in_deck, 0)}</span>
                <span className="tune-chip">profile: {asString(tuneSummary.profile_id) || "(missing)"}</span>
                <span className="tune-chip">bracket: {asString(tuneSummary.bracket_id) || "(missing)"}</span>
                <span className="tune-chip">mulligan: {asString(tuneSummary.mulligan_model_id) || "(missing)"}</span>
              </div>

              {tuneDevMetrics ? (
                <div className="tune-keep-block">
                  <div className="tune-chip-grid">
                    <span className="tune-chip">protected_cut_count: {tuneProtectedCutCount}</span>
                  </div>
                  <div>
                    <strong>KEEP (protected)</strong>
                    {tuneProtectedCutNamesTop10.length === 0 ? (
                      <p className="muted">No protected cards reported.</p>
                    ) : (
                      <ul className="tune-keep-list">
                        {tuneProtectedCutNamesTop10.map((cardName: string, idx: number) => (
                          <li key={`keep-${idx}-${cardName}`}>{cardName}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                </div>
              ) : null}

              {tuneSwapRows.length === 0 ? (
                <p className="muted">No recommended swaps returned.</p>
              ) : (
                <>
                  <div className="copy-row tune-copy-row">
                    <label className="inline-control">
                      Apply Top N Swaps
                      <input
                        type="number"
                        min={0}
                        max={tuneSwapRows.length}
                        value={effectiveCopySwapCount}
                        onChange={(event) =>
                          setCopySwapCount(clampInteger(Number(event.target.value), 0, tuneSwapRows.length))
                        }
                        disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
                      />
                    </label>
                    <button
                      type="button"
                      onClick={() => void handleCopyTunedDecklist()}
                      disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
                    >
                      Copy tuned decklist
                    </button>
                  </div>
                  {copyNotice ? <p className="copy-notice">{copyNotice}</p> : null}

                  <ul className="tune-swaps-list">
                    {tuneSwapRows.map((swap: DeckTuneSwapV1, idx: number) => {
                      const cutName = asString(swap.cut_name) || "(missing cut)";
                      const addName = asString(swap.add_name) || "(missing add)";
                      const reasons = asStringArray(swap.reasons_v1).slice().sort();
                      const delta = asRecord(swap.delta_summary_v1) ?? {};

                      return (
                        <li key={`swap-${idx}-${cutName}-${addName}`} className="tune-swap-row">
                          <div>
                            <strong>{cutName}</strong> <span aria-hidden="true">→</span> <strong>{addName}</strong>
                          </div>
                          <div className="reason-tags">
                            {reasons.length === 0 ? (
                              <span className="reason-tag">NO_REASON</span>
                            ) : (
                              reasons.map((reason: string) => (
                                <span key={`reason-${idx}-${reason}`} className="reason-tag">
                                  {reason}
                                </span>
                              ))
                            )}
                          </div>
                          <div className="delta-grid">
                            <span>total_score_delta: {asNumber(delta.total_score_delta_v1, 0).toFixed(3)}</span>
                            <span>coherence_delta: {asNumber(delta.coherence_delta_v1, 0).toFixed(3)}</span>
                            <span>primitive_coverage_delta: {asInteger(delta.primitive_coverage_delta_v1, 0)}</span>
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                </>
              )}
            </div>
          ) : (
            <p className="muted">No tune results yet. Validate a deck, then click Tune Deck.</p>
          )}

          <h3>Deck Complete v1</h3>
          {deckCompleteResponse ? (
            <div className="complete-results-block">
              <div className="tune-status-row">
                <strong>status: {completeStatus || "(missing)"}</strong>
                <span>request_hash_v1: {asString(deckCompleteResponse.request_hash_v1) || "(missing)"}</span>
                <span>complete_engine_version: {asString(deckCompleteResponse.complete_engine_version) || "(missing)"}</span>
              </div>

              <div className="tune-chip-grid">
                <span className="tune-chip">cards_added: {completeAddedRows.length}</span>
                <span className="tune-chip">lands_added: {completeLandsAdded}</span>
                <span className="tune-chip">target_deck_size: {requestedCompleteTargetDeckSize}</span>
                <span className="tune-chip">
                  max_adds: {lastCompleteRequestConfig?.max_adds ?? clampInteger(asInteger(completeMaxAdds, DEFAULT_COMPLETE_MAX_ADDS), 1, 500)}
                </span>
                <span className="tune-chip">
                  allow_basic_lands: {(lastCompleteRequestConfig?.allow_basic_lands ?? completeAllowBasicLands) ? "true" : "false"}
                </span>
                <span className="tune-chip">
                  land_target_mode: {lastCompleteRequestConfig?.land_target_mode ?? completeLandTargetMode}
                </span>
                <span className="tune-chip">commander_cards: {completeDecklistCounts.commanderCount}</span>
                <span className="tune-chip">deck_cards: {completeDecklistCounts.deckCount}</span>
                <span className="tune-chip">final_deck_size: {completeDecklistCounts.totalCount}</span>
              </div>

              {completeNeedsAttention ? (
                <div className="complete-warning-box" role="alert">
                  <strong>Completion warning</strong>
                  <ul>
                    <li>status: {completeStatus || "(missing)"}</li>
                    <li>final_deck_size: {completeDecklistCounts.totalCount}</li>
                    <li>target_deck_size: {requestedCompleteTargetDeckSize}</li>
                    <li>cards_added: {completeAddedRows.length}</li>
                    <li>lands_added: {completeLandsAdded}</li>
                  </ul>

                  {completeUnknowns.length > 0 ? (
                    <div>
                      <strong>unknowns</strong>
                      <ul>
                        {completeUnknowns.map((row: UnknownRow, idx: number) => (
                          <li key={`complete-unknown-${idx}`}>
                            {(asString(row.reason_code) || "UNKNOWN") +
                              (asString(row.name_raw) ? ` :: ${asString(row.name_raw)}` : "")}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ) : null}

                  {completeViolations.length > 0 ? (
                    <div>
                      <strong>violations</strong>
                      <ul>
                        {completeViolations.map((row: DeckValidateViolationV1, idx: number) => {
                          const code = asString(row.code) || "UNKNOWN";
                          const message = asString(row.message);
                          const lineNos = Array.isArray(row.line_nos)
                            ? row.line_nos
                                .filter((value: unknown) => typeof value === "number")
                                .map((value: number) => value)
                                .join(", ")
                            : "";
                          return (
                            <li key={`complete-violation-${idx}`}>
                              {code}
                              {message ? ` :: ${message}` : ""}
                              {lineNos ? ` (lines: ${lineNos})` : ""}
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  ) : null}
                </div>
              ) : null}

              {completeDevMetrics ? (
                <details className="complete-details-block">
                  <summary>Details (DEV metrics)</summary>
                  <JsonViewer value={completeDevMetrics} />
                </details>
              ) : null}

              <label className="complete-decklist-label">
                Completed decklist text
                <textarea
                  className="complete-decklist-textarea"
                  value={completedDecklistText}
                  readOnly
                  rows={Math.max(8, Math.min(completedDecklistText.split(/\r?\n/).length + 1, 22))}
                />
              </label>

              <div className="copy-row tune-copy-row">
                <button
                  type="button"
                  onClick={() => void handleCopyCompletedDecklist()}
                  disabled={loadingValidate || loadingBuild || loadingTune || loadingComplete}
                >
                  Copy completed decklist
                </button>
              </div>
              {copyCompleteNotice ? <p className="copy-notice">{copyCompleteNotice}</p> : null}

              {completeAddedRows.length === 0 ? (
                <p className="muted">No added cards returned.</p>
              ) : (
                <ul className="complete-add-list">
                  {completeAddedRows.map((row: DeckCompleteAddedCardV1, idx: number) => {
                    const cardName = asString(row.name) || "(missing card)";
                    const reasons = asStringArray(row.reasons_v1).slice().sort();
                    const primitives = asStringArray(row.primitives_added_v1).slice().sort();
                    return (
                      <li key={`complete-${idx}-${cardName}`} className="complete-add-row">
                        <div>
                          <strong>{cardName}</strong>
                        </div>
                        <div className="reason-tags">
                          {reasons.length === 0 ? (
                            <span className="reason-tag">NO_REASON</span>
                          ) : (
                            reasons.map((reason: string) => (
                              <span key={`complete-reason-${idx}-${reason}`} className="reason-tag">
                                {reason}
                              </span>
                            ))
                          )}
                        </div>
                        {primitives.length > 0 ? (
                          <div className="delta-grid">
                            <span>primitives_added: {primitives.join(", ")}</span>
                          </div>
                        ) : null}
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          ) : (
            <p className="muted">No completion results yet. Validate a deck, then click Complete Deck.</p>
          )}
        </section>

        <aside className="panel viewer-column">
          <h2>Panels</h2>
          {availablePanels.length === 0 ? (
            <p className="muted">No available_panels_v1 in build response.</p>
          ) : (
            <>
              <ul className="panel-key-list">
                {availablePanels.map((entry: { key: string; enabled: boolean }) => (
                  <li key={entry.key}>
                    <button
                      type="button"
                      className={selectedPanelKey === entry.key ? "active" : ""}
                      onClick={() => setSelectedPanelKey(entry.key)}
                    >
                      {entry.key} ({entry.enabled ? "true" : "false"})
                    </button>
                  </li>
                ))}
              </ul>
              <h3>Selected Panel Payload</h3>
              {selectedPanelKey ? (
                selectedPanelPayload ? (
                  <>
                    <div>payload_key: {selectedPanelPayload.payloadKey}</div>
                    <JsonViewer value={selectedPanelPayload.payload} />
                  </>
                ) : (
                  <p className="muted">Panel payload is not present in result.</p>
                )
              ) : (
                <p className="muted">Select a panel key.</p>
              )}
            </>
          )}
        </aside>
      </div>
    </div>
  );
}
