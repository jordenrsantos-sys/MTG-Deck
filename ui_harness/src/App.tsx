import { FormEvent, useEffect, useMemo, useState } from "react";

type BuildResult = {
  ui_contract_version?: string;
  available_panels_v1?: Record<string, boolean>;
  structural_snapshot_v1?: Record<string, unknown>;
  commander_canonical_slot?: unknown;
  deck_cards_canonical_input_order?: unknown[];
  deck_cards_canonical_input_order_total?: number;
  deck_cards_slot_ids_playable?: unknown[];
  deck_cards_slot_ids_nonplayable?: unknown[];
  deck_cards_canonical_playable_slots_total?: number;
  deck_cards_canonical_nonplayable_slots_total?: number;
  deck_cards_unknowns_by_slot?: unknown;
  canonical_slots_all?: unknown[];
  canonical_slots_all_total?: number;
  graph_v1?: {
    candidate_edges_total?: number;
    bounded?: boolean;
    candidate_edges?: unknown[];
    stats?: Record<string, unknown>;
  };
  graph_nodes_total?: number;
  graph_edges_total?: number;
  graph_node_degrees?: unknown;
  graph_components?: unknown;
  [key: string]: unknown;
};

type BuildResponse = {
  engine_version?: string;
  ruleset_version?: string;
  bracket_definition_version?: string;
  game_changers_version?: string;
  db_snapshot_id?: string;
  profile_id?: string;
  bracket_id?: string;
  status?: string;
  deck_size_total?: number | null;
  deck_status?: string | null;
  cards_needed?: number | null;
  cards_to_cut?: number | null;
  build_hash_v1?: string | null;
  graph_hash_v2?: string | null;
  unknowns?: unknown[];
  result?: BuildResult;
  [key: string]: unknown;
};

type BuildRequestPayload = {
  db_snapshot_id: string;
  profile_id: string;
  bracket_id: string;
  format: "commander";
  commander: string;
  cards: string[];
  engine_patches_v0: unknown[];
};

type RunHistoryEntry = {
  id: string;
  timestamp: string;
  commanderInput: string;
  dbSnapshotId: string;
  profileId: string;
  bracketId: string;
  status: string;
  buildHashV1: string | null;
  graphHashV2: string | null;
  unknownCodes: string[];
  requestPayload: BuildRequestPayload;
  responseBody: BuildResponse;
};

type DeterminismDrillRunSummary = {
  status: string;
  buildHashV1: string | null;
  graphHashV2: string | null;
  unknownCodes: string[];
  contractMatched: boolean;
};

type DeterminismDrillResult = {
  run1: DeterminismDrillRunSummary;
  run2: DeterminismDrillRunSummary;
  determinismPass: boolean;
  unknownCodesAdded: string[];
  unknownCodesRemoved: string[];
};

type BuildExecution = {
  statusCode: number;
  raw: string;
  parsed: BuildResponse | null;
  contractMatch: boolean;
  unknownCodes: string[];
};

type ValueChangeEvent = {
  target: {
    value: string;
  };
};

type TabId = "summary" | "canonical" | "structural" | "graph" | "raw";
const UI_CONTRACT_VERSION = "ui_contract_v1";
const RUN_HISTORY_STORAGE_KEY = "ui_harness_runs_v1";
const RUN_HISTORY_MAX = 10;
const LOCAL_PATH_REDACTED = "<LOCAL_PATH_REDACTED>";

const WINDOWS_ABSOLUTE_PATH_RE = /^[A-Za-z]:\\/;
const UNC_ABSOLUTE_PATH_RE = /^\\\\[^\\]+\\[^\\]+\\/;
const POSIX_ABSOLUTE_PATH_RE = /^\//;
const HTTP_URL_RE = /^https?:\/\//i;

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

const DEFAULT_FORM = {
  db_snapshot_id: "20260217_190902",
  profile_id: "focused",
  bracket_id: "B2",
  commander: "Krenko, Mob Boss",
  cardsText: "",
};

function parseCardsInput(cardsText: string): string[] {
  const out: string[] = [];
  const lines = cardsText.split(/\r?\n/);
  for (const line of lines) {
    const pieces = line.split(",");
    for (const piece of pieces) {
      const card = piece.trim();
      if (card !== "") {
        out.push(card);
      }
    }
  }
  return out;
}

function isBooleanMap(value: unknown): value is Record<string, boolean> {
  if (!value || typeof value !== "object") {
    return false;
  }
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (typeof key !== "string" || typeof item !== "boolean") {
      return false;
    }
  }
  return true;
}

function extractUnknownMessage(item: unknown): string {
  if (item && typeof item === "object" && "message" in item) {
    const message = (item as { message?: unknown }).message;
    if (typeof message === "string") {
      return message;
    }
  }
  if (typeof item === "string") {
    return item;
  }
  try {
    return JSON.stringify(item);
  } catch {
    return String(item);
  }
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

function extractUnknownCodes(items: unknown[] | undefined): string[] {
  if (!Array.isArray(items)) {
    return [];
  }
  const codes: string[] = [];
  for (const item of items) {
    const rec = asRecord(item);
    if (rec && typeof rec.code === "string") {
      codes.push(rec.code);
    }
  }
  return codes;
}

function isLocalAbsolutePath(value: string): boolean {
  if (HTTP_URL_RE.test(value)) {
    return false;
  }
  return WINDOWS_ABSOLUTE_PATH_RE.test(value) || UNC_ABSOLUTE_PATH_RE.test(value) || POSIX_ABSOLUTE_PATH_RE.test(value);
}

function redactLocalPathsForExport(value: unknown): unknown {
  if (typeof value === "string") {
    return isLocalAbsolutePath(value) ? LOCAL_PATH_REDACTED : value;
  }
  if (Array.isArray(value)) {
    return value.map((item: unknown) => redactLocalPathsForExport(item));
  }
  const rec = asRecord(value);
  if (!rec) {
    return value;
  }
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(rec)) {
    out[key] = redactLocalPathsForExport(item);
  }
  return out;
}

function normalizeRunHistoryEntry(value: unknown): RunHistoryEntry | null {
  const rec = asRecord(value);
  if (!rec) {
    return null;
  }
  if (typeof rec.id !== "string" || typeof rec.timestamp !== "string") {
    return null;
  }
  if (
    typeof rec.commanderInput !== "string" ||
    typeof rec.dbSnapshotId !== "string" ||
    typeof rec.profileId !== "string" ||
    typeof rec.bracketId !== "string" ||
    typeof rec.status !== "string"
  ) {
    return null;
  }

  const requestPayloadRaw = rec.requestPayload;
  const responseBodyRaw = rec.responseBody;
  const requestPayload = asRecord(requestPayloadRaw);
  const responseBody = asRecord(responseBodyRaw);
  if (!requestPayload || !responseBody) {
    return null;
  }
  if (
    typeof requestPayload.db_snapshot_id !== "string" ||
    typeof requestPayload.profile_id !== "string" ||
    typeof requestPayload.bracket_id !== "string" ||
    requestPayload.format !== "commander" ||
    typeof requestPayload.commander !== "string" ||
    !Array.isArray(requestPayload.cards) ||
    !Array.isArray(requestPayload.engine_patches_v0)
  ) {
    return null;
  }

  const unknownCodes = Array.isArray(rec.unknownCodes)
    ? rec.unknownCodes.filter((item: unknown): item is string => typeof item === "string")
    : extractUnknownCodes((responseBody as BuildResponse).unknowns);

  return {
    id: rec.id,
    timestamp: rec.timestamp,
    commanderInput: rec.commanderInput,
    dbSnapshotId: rec.dbSnapshotId,
    profileId: rec.profileId,
    bracketId: rec.bracketId,
    status: rec.status,
    buildHashV1: typeof rec.buildHashV1 === "string" ? rec.buildHashV1 : null,
    graphHashV2: typeof rec.graphHashV2 === "string" ? rec.graphHashV2 : null,
    unknownCodes,
    requestPayload: requestPayloadRaw as BuildRequestPayload,
    responseBody: responseBodyRaw as BuildResponse,
  };
}

function hasOwnField(record: Record<string, unknown> | undefined, fieldName: string): boolean {
  return !!record && Object.prototype.hasOwnProperty.call(record, fieldName);
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (value === null || value === undefined) {
    return "-";
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function safeJsonParse(raw: string): BuildResponse | null {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object") {
      return parsed as BuildResponse;
    }
  } catch {
    return null;
  }
  return null;
}

function App() {
  const [dbSnapshotId, setDbSnapshotId] = useState(DEFAULT_FORM.db_snapshot_id);
  const [profileId, setProfileId] = useState(DEFAULT_FORM.profile_id);
  const [bracketId, setBracketId] = useState(DEFAULT_FORM.bracket_id);
  const [commander, setCommander] = useState(DEFAULT_FORM.commander);
  const [cardsText, setCardsText] = useState(DEFAULT_FORM.cardsText);

  const [lastRequestPayload, setLastRequestPayload] = useState<BuildRequestPayload | null>(null);
  const [rawResponse, setRawResponse] = useState<string>("");
  const [responseBody, setResponseBody] = useState<BuildResponse | null>(null);
  const [httpStatus, setHttpStatus] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [contractMatched, setContractMatched] = useState<boolean | null>(null);
  const [copyNotice, setCopyNotice] = useState<string | null>(null);
  const [canonicalFilter, setCanonicalFilter] = useState("");
  const [activeTab, setActiveTab] = useState<TabId>("summary");
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [compareViewOpen, setCompareViewOpen] = useState(false);
  const [compareRunAId, setCompareRunAId] = useState("");
  const [compareRunBId, setCompareRunBId] = useState("");
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [determinismLoading, setDeterminismLoading] = useState(false);
  const [determinismDrillResult, setDeterminismDrillResult] = useState<DeterminismDrillResult | null>(null);
  const [redactLocalPathsInExport, setRedactLocalPathsInExport] = useState(true);

  const resultRecord = responseBody?.result;
  const structuralSnapshot = responseBody?.result?.structural_snapshot_v1;
  const commanderCanonicalSlot = responseBody?.result?.commander_canonical_slot;
  const deckCardsCanonicalInputOrderRaw = responseBody?.result?.deck_cards_canonical_input_order;
  const deckCardsCanonicalInputOrder = Array.isArray(deckCardsCanonicalInputOrderRaw)
    ? deckCardsCanonicalInputOrderRaw
    : null;
  const deckCardsCanonicalInputOrderTotalRaw = responseBody?.result?.deck_cards_canonical_input_order_total;
  const deckCardsSlotIdsPlayableRaw = responseBody?.result?.deck_cards_slot_ids_playable;
  const deckCardsSlotIdsPlayable = Array.isArray(deckCardsSlotIdsPlayableRaw) ? deckCardsSlotIdsPlayableRaw : null;
  const deckCardsSlotIdsNonplayableRaw = responseBody?.result?.deck_cards_slot_ids_nonplayable;
  const deckCardsSlotIdsNonplayable = Array.isArray(deckCardsSlotIdsNonplayableRaw)
    ? deckCardsSlotIdsNonplayableRaw
    : null;
  const deckCardsCanonicalPlayableSlotsTotalRaw = responseBody?.result?.deck_cards_canonical_playable_slots_total;
  const deckCardsCanonicalNonplayableSlotsTotalRaw = responseBody?.result?.deck_cards_canonical_nonplayable_slots_total;
  const deckCardsUnknownsBySlot = responseBody?.result?.deck_cards_unknowns_by_slot;
  const canonicalSlotsAllRaw = responseBody?.result?.canonical_slots_all;
  const canonicalSlotsAll = Array.isArray(canonicalSlotsAllRaw)
    ? canonicalSlotsAllRaw
    : null;
  const hasCommanderCanonicalSlot = hasOwnField(resultRecord, "commander_canonical_slot");
  const hasDeckCardsCanonicalInputOrder = hasOwnField(resultRecord, "deck_cards_canonical_input_order");
  const hasDeckCardsCanonicalInputOrderTotal = hasOwnField(resultRecord, "deck_cards_canonical_input_order_total");
  const hasDeckCardsSlotIdsPlayable = hasOwnField(resultRecord, "deck_cards_slot_ids_playable");
  const hasDeckCardsSlotIdsNonplayable = hasOwnField(resultRecord, "deck_cards_slot_ids_nonplayable");
  const hasDeckCardsCanonicalPlayableSlotsTotal = hasOwnField(resultRecord, "deck_cards_canonical_playable_slots_total");
  const hasDeckCardsCanonicalNonplayableSlotsTotal = hasOwnField(
    resultRecord,
    "deck_cards_canonical_nonplayable_slots_total",
  );
  const hasDeckCardsUnknownsBySlot = hasOwnField(resultRecord, "deck_cards_unknowns_by_slot");
  const hasCanonicalSlotsAll = hasOwnField(resultRecord, "canonical_slots_all");
  const graphV1 = responseBody?.result?.graph_v1;
  const graphNodeDegrees = responseBody?.result?.graph_node_degrees;
  const graphComponents = responseBody?.result?.graph_components;
  const unknowns = Array.isArray(responseBody?.unknowns) ? responseBody.unknowns : [];
  const unknownCodes = extractUnknownCodes(unknowns);
  const unknownMessages = unknowns.slice(0, 3).map(extractUnknownMessage);

  const availablePanels = useMemo(() => {
    const rawMap = responseBody?.result?.available_panels_v1;
    return isBooleanMap(rawMap) ? rawMap : {};
  }, [responseBody]);

  const oracleTextLayerSkipped = unknownCodes.includes("LAYER_SKIPPED_ORACLE_TEXT_REQUIRED");
  const oracleTextBadge =
    availablePanels.has_proof_attempts === false && oracleTextLayerSkipped
      ? "Oracle Text: FORBIDDEN"
      : responseBody
        ? "Oracle Text: (unknown)"
        : null;

  const commanderCanonicalSlotJson = hasCommanderCanonicalSlot ? JSON.stringify(commanderCanonicalSlot ?? null, null, 2) : null;
  const canonicalSlotsAllJson = hasCanonicalSlotsAll ? JSON.stringify(canonicalSlotsAllRaw ?? null, null, 2) : null;

  const summaryTabEnabled = contractMatched !== false;
  const canonicalFieldsPresent =
    hasCommanderCanonicalSlot ||
    hasDeckCardsCanonicalInputOrder ||
    hasCanonicalSlotsAll ||
    hasDeckCardsUnknownsBySlot;
  const canonicalTabVisible = canonicalFieldsPresent || availablePanels.has_unknowns_canonical === true;
  const canonicalTabEnabled = summaryTabEnabled && canonicalTabVisible;
  const structuralTabVisible =
    availablePanels.has_structural_reporting === true && structuralSnapshot !== undefined && structuralSnapshot !== null;
  const graphTabVisible = availablePanels.has_graph === true;
  const structuralTabEnabled = summaryTabEnabled && structuralTabVisible;
  const graphTabEnabled = summaryTabEnabled && graphTabVisible;

  const canonicalSlotsFiltered = useMemo(() => {
    const rows = Array.isArray(canonicalSlotsAll) ? canonicalSlotsAll : [];
    const needle = canonicalFilter.trim().toLowerCase();
    if (needle === "") {
      return rows;
    }
    return rows.filter((row) => {
      const rec = asRecord(row);
      const slotId = rec ? asString(rec.slot_id).toLowerCase() : "";
      const input = rec ? asString(rec.input).toLowerCase() : "";
      const resolvedName = rec ? asString(rec.resolved_name).toLowerCase() : "";
      const resolvedOracleId = rec ? asString(rec.resolved_oracle_id).toLowerCase() : "";
      const status = rec ? asString(rec.status).toLowerCase() : "";
      const codesRaw = rec ? rec.codes : undefined;
      const codes = Array.isArray(codesRaw)
        ? codesRaw.map((entry: unknown) => asString(entry)).join(", ").toLowerCase()
        : rec
          ? asString(codesRaw).toLowerCase()
          : "";
      return (
        slotId.includes(needle) ||
        input.includes(needle) ||
        resolvedName.includes(needle) ||
        resolvedOracleId.includes(needle) ||
        status.includes(needle) ||
        codes.includes(needle)
      );
    });
  }, [canonicalSlotsAll, canonicalFilter]);

  const structuralRequiredCount = Array.isArray(structuralSnapshot?.required_primitives_v1)
    ? structuralSnapshot.required_primitives_v1.length
    : null;
  const structuralPresentCount = Array.isArray(structuralSnapshot?.present_primitives_v1)
    ? structuralSnapshot.present_primitives_v1.length
    : null;
  const structuralMissingCount = Array.isArray(structuralSnapshot?.missing_primitives_v1)
    ? structuralSnapshot.missing_primitives_v1.length
    : null;
  const structuralDeadSlotsCount = Array.isArray(structuralSnapshot?.dead_slot_ids_v1)
    ? structuralSnapshot.dead_slot_ids_v1.length
    : null;
  const structuralCommanderDependencySignal = structuralSnapshot?.commander_dependency_signal_v1;

  const graphStatsRecord = asRecord(graphV1?.stats);
  const graphCapsHit = graphStatsRecord ? graphStatsRecord.caps_hit : undefined;

  const graphNodeDegreeRows = useMemo(() => {
    if (Array.isArray(graphNodeDegrees)) {
      return graphNodeDegrees.map((entry) => {
        if (Array.isArray(entry) && entry.length >= 2) {
          return { nodeId: asString(entry[0]), degree: asString(entry[1]) };
        }
        const rec = asRecord(entry);
        if (rec) {
          return {
            nodeId: asString(rec.node_id ?? rec.id ?? "-"),
            degree: asString(rec.degree ?? rec.value ?? "-"),
          };
        }
        return { nodeId: "-", degree: asString(entry) };
      });
    }
    const rec = asRecord(graphNodeDegrees);
    if (rec) {
      return Object.entries(rec).map(([nodeId, degree]) => ({
        nodeId,
        degree: asString(degree),
      }));
    }
    return null;
  }, [graphNodeDegrees]);

  const graphComponentRows = useMemo(() => {
    if (!Array.isArray(graphComponents)) {
      return null;
    }
    const rows: Array<{ componentId: string; nodeTotal: string }> = [];
    for (let idx = 0; idx < graphComponents.length; idx += 1) {
      const rec = asRecord(graphComponents[idx]);
      if (!rec) {
        return null;
      }
      const componentId = rec.component_id ?? rec.id ?? "-";
      const nodeTotal: unknown = rec.node_total ?? rec.nodes_total ?? rec.node_count;
      rows.push({
        componentId: asString(componentId),
        nodeTotal: nodeTotal === undefined ? "-" : asString(nodeTotal),
      });
    }
    return rows;
  }, [graphComponents]);

  const graphSummary = useMemo(() => {
    return {
      candidate_edges_total: typeof graphV1?.candidate_edges_total === "number" ? graphV1.candidate_edges_total : null,
      bounded: typeof graphV1?.bounded === "boolean" ? graphV1.bounded : null,
      stats: graphV1?.stats ?? null,
      graph_nodes_total:
        typeof responseBody?.result?.graph_nodes_total === "number" ? responseBody.result.graph_nodes_total : null,
      graph_edges_total:
        typeof responseBody?.result?.graph_edges_total === "number" ? responseBody.result.graph_edges_total : null,
    };
  }, [graphV1, responseBody]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    try {
      const stored = window.localStorage.getItem(RUN_HISTORY_STORAGE_KEY);
      if (!stored) {
        return;
      }
      const parsed = JSON.parse(stored) as unknown;
      if (!Array.isArray(parsed)) {
        return;
      }
      const normalized: RunHistoryEntry[] = [];
      for (const item of parsed) {
        const entry = normalizeRunHistoryEntry(item);
        if (entry) {
          normalized.push(entry);
        }
      }
      setRunHistory(normalized.slice(0, RUN_HISTORY_MAX));
    } catch {
      // ignore localStorage parsing errors
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    try {
      window.localStorage.setItem(RUN_HISTORY_STORAGE_KEY, JSON.stringify(runHistory));
    } catch {
      // ignore localStorage write errors
    }
  }, [runHistory]);

  useEffect(() => {
    if (runHistory.length === 0) {
      setCompareRunAId("");
      setCompareRunBId("");
      return;
    }
    setCompareRunAId((prev) => {
      if (runHistory.some((entry) => entry.id === prev)) {
        return prev;
      }
      return runHistory[0].id;
    });
    setCompareRunBId((prev) => {
      if (runHistory.some((entry) => entry.id === prev)) {
        return prev;
      }
      return runHistory[1]?.id ?? runHistory[0].id;
    });
  }, [runHistory]);

  const compareRunA = useMemo(
    () => runHistory.find((entry: RunHistoryEntry) => entry.id === compareRunAId) ?? null,
    [runHistory, compareRunAId],
  );
  const compareRunB = useMemo(
    () => runHistory.find((entry: RunHistoryEntry) => entry.id === compareRunBId) ?? null,
    [runHistory, compareRunBId],
  );

  const compareData = useMemo(() => {
    if (!compareRunA || !compareRunB) {
      return null;
    }

    const resultA = asRecord(compareRunA.responseBody.result);
    const resultB = asRecord(compareRunB.responseBody.result);

    const unknownCodesAdded = compareRunB.unknownCodes.filter((code: string) => !compareRunA.unknownCodes.includes(code));
    const unknownCodesRemoved = compareRunA.unknownCodes.filter((code: string) => !compareRunB.unknownCodes.includes(code));

    const structuralA = asRecord(resultA ? resultA.structural_snapshot_v1 : null);
    const structuralB = asRecord(resultB ? resultB.structural_snapshot_v1 : null);
    const structuralKeysA = structuralA ? Object.keys(structuralA) : [];
    const structuralKeysB = structuralB ? Object.keys(structuralB) : [];
    const structuralKeysAdded = structuralKeysB.filter((key: string) => !structuralKeysA.includes(key));
    const structuralKeysRemoved = structuralKeysA.filter((key: string) => !structuralKeysB.includes(key));

    const graphA = asRecord(resultA ? resultA.graph_v1 : null);
    const graphB = asRecord(resultB ? resultB.graph_v1 : null);
    const graphStatsA = asRecord(graphA ? graphA.stats : null);
    const graphStatsB = asRecord(graphB ? graphB.stats : null);
    const graphStatsKeysA = graphStatsA ? Object.keys(graphStatsA) : [];
    const graphStatsKeysB = graphStatsB ? Object.keys(graphStatsB) : [];
    const graphStatsKeysAdded = graphStatsKeysB.filter((key: string) => !graphStatsKeysA.includes(key));
    const graphStatsKeysRemoved = graphStatsKeysA.filter((key: string) => !graphStatsKeysB.includes(key));

    const canonicalTotalA = resultA && typeof resultA.canonical_slots_all_total === "number" ? resultA.canonical_slots_all_total : null;
    const canonicalTotalB = resultB && typeof resultB.canonical_slots_all_total === "number" ? resultB.canonical_slots_all_total : null;

    return {
      buildHashMatch: compareRunA.buildHashV1 === compareRunB.buildHashV1,
      graphHashMatch: compareRunA.graphHashV2 === compareRunB.graphHashV2,
      statusA: compareRunA.status,
      statusB: compareRunB.status,
      buildHashA: compareRunA.buildHashV1,
      buildHashB: compareRunB.buildHashV1,
      graphHashA: compareRunA.graphHashV2,
      graphHashB: compareRunB.graphHashV2,
      unknownCodesAdded,
      unknownCodesRemoved,
      canonicalTotalA,
      canonicalTotalB,
      structuralKeysA,
      structuralKeysB,
      structuralKeysAdded,
      structuralKeysRemoved,
      graphStatsKeysA,
      graphStatsKeysB,
      graphStatsKeysAdded,
      graphStatsKeysRemoved,
    };
  }, [compareRunA, compareRunB]);

  function appendRunHistoryEntry(requestPayload: BuildRequestPayload, payload: BuildResponse) {
    const timestamp = new Date().toISOString();
    const entry: RunHistoryEntry = {
      id: `${timestamp}-${requestPayload.commander}-${requestPayload.db_snapshot_id}`,
      timestamp,
      commanderInput: requestPayload.commander,
      dbSnapshotId: requestPayload.db_snapshot_id,
      profileId: requestPayload.profile_id,
      bracketId: requestPayload.bracket_id,
      status: typeof payload.status === "string" ? payload.status : "-",
      buildHashV1: typeof payload.build_hash_v1 === "string" ? payload.build_hash_v1 : null,
      graphHashV2: typeof payload.graph_hash_v2 === "string" ? payload.graph_hash_v2 : null,
      unknownCodes: extractUnknownCodes(payload.unknowns),
      requestPayload,
      responseBody: payload,
    };
    setRunHistory((prev: RunHistoryEntry[]) => [entry, ...prev].slice(0, RUN_HISTORY_MAX));
    setSelectedRunId(entry.id);
  }

  function loadRunFromHistory(entry: RunHistoryEntry) {
    setSelectedRunId(entry.id);
    setLastRequestPayload(entry.requestPayload);
    setRuntimeError(null);
    setCopyNotice(null);
    setCanonicalFilter("");
    setHttpStatus(200);
    setResponseBody(entry.responseBody);
    setRawResponse(JSON.stringify(entry.responseBody, null, 2));
    const isContractMatch = entry.responseBody.result?.ui_contract_version === UI_CONTRACT_VERSION;
    setContractMatched(isContractMatch);
    setActiveTab(isContractMatch ? "summary" : "raw");
  }

  function summarizeDrillRun(execution: BuildExecution): DeterminismDrillRunSummary {
    return {
      status: execution.parsed?.status ?? "-",
      buildHashV1: typeof execution.parsed?.build_hash_v1 === "string" ? execution.parsed.build_hash_v1 : null,
      graphHashV2: typeof execution.parsed?.graph_hash_v2 === "string" ? execution.parsed.graph_hash_v2 : null,
      unknownCodes: execution.unknownCodes,
      contractMatched: execution.contractMatch,
    };
  }

  function applyExecutionToInspector(requestPayload: BuildRequestPayload, execution: BuildExecution) {
    setSelectedRunId(null);
    setLastRequestPayload(requestPayload);
    setRawResponse(execution.raw);
    setHttpStatus(execution.statusCode);
    setResponseBody(execution.parsed);
    setContractMatched(execution.contractMatch);
    setActiveTab(execution.contractMatch ? "summary" : "raw");
  }

  async function executeBuildRequest(requestPayload: BuildRequestPayload): Promise<BuildExecution> {
    const response = await fetch(`${API_BASE_URL}/build`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestPayload),
    });
    const raw = await response.text();
    const parsed = safeJsonParse(raw);
    const contractMatch = parsed?.result?.ui_contract_version === UI_CONTRACT_VERSION;
    const runUnknownCodes = extractUnknownCodes(parsed?.unknowns);
    if (response.status === 200 && parsed) {
      appendRunHistoryEntry(requestPayload, parsed);
    }
    return {
      statusCode: response.status,
      raw,
      parsed,
      contractMatch,
      unknownCodes: runUnknownCodes,
    };
  }

  function buildUiHarnessMetadata() {
    return {
      app: "mtg-ui-harness",
      ui_contract_expected: UI_CONTRACT_VERSION,
      api_base_url: API_BASE_URL,
    };
  }

  function sanitizeFilenameToken(value: string | null | undefined, fallback: string): string {
    const source = typeof value === "string" && value.trim() !== "" ? value.trim().toLowerCase() : fallback;
    const cleaned = source.replace(/[^a-z0-9_-]+/gi, "_").replace(/^_+|_+$/g, "");
    return cleaned === "" ? fallback : cleaned;
  }

  function formatTimestampToken(value: string): string {
    const parsed = new Date(value);
    const stamp = Number.isNaN(parsed.getTime()) ? new Date() : parsed;
    const year = stamp.getUTCFullYear().toString().padStart(4, "0");
    const month = String(stamp.getUTCMonth() + 1).padStart(2, "0");
    const day = String(stamp.getUTCDate()).padStart(2, "0");
    const hour = String(stamp.getUTCHours()).padStart(2, "0");
    const minute = String(stamp.getUTCMinutes()).padStart(2, "0");
    const second = String(stamp.getUTCSeconds()).padStart(2, "0");
    return `${year}${month}${day}_${hour}${minute}${second}`;
  }

  function triggerJsonDownload(filename: string, payload: unknown) {
    if (typeof document === "undefined") {
      return;
    }
    const json = JSON.stringify(payload, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    URL.revokeObjectURL(url);
  }

  function buildRunBundleRun(entry: RunHistoryEntry, redactLocalPathsInExport: boolean) {
    const detectedContract =
      typeof entry.responseBody.result?.ui_contract_version === "string" ? entry.responseBody.result.ui_contract_version : null;
    const responseForExport = redactLocalPathsInExport ? redactLocalPathsForExport(entry.responseBody) : entry.responseBody;
    return {
      run_id: entry.id,
      timestamp_iso: entry.timestamp,
      request: entry.requestPayload,
      response: responseForExport,
      status: typeof entry.responseBody.status === "string" ? entry.responseBody.status : entry.status,
      hashes: {
        build_hash_v1: entry.buildHashV1,
        graph_hash_v2: entry.graphHashV2,
      },
      unknown_codes: entry.unknownCodes,
      contract: {
        matched: detectedContract === UI_CONTRACT_VERSION,
        detected: detectedContract,
      },
    };
  }

  function exportRunBundle(entry: RunHistoryEntry) {
    const runStatus = typeof entry.responseBody.status === "string" ? entry.responseBody.status : entry.status;
    const statusToken = sanitizeFilenameToken(runStatus, "unknown_status");
    const commanderToken = sanitizeFilenameToken(entry.requestPayload.commander, "unknown");
    const timestampToken = formatTimestampToken(entry.timestamp);
    const filename = `mtg_repro_bundle_${statusToken}_${commanderToken}_${timestampToken}.json`;
    const payload = {
      bundle_version: "repro_bundle_v1",
      created_at_iso: new Date().toISOString(),
      ui_harness: buildUiHarnessMetadata(),
      run: buildRunBundleRun(entry, redactLocalPathsInExport),
    };
    triggerJsonDownload(filename, payload);
    setCopyNotice(`Exported ${filename}`);
  }

  function exportCompareBundle() {
    if (!compareRunA || !compareRunB || !compareData) {
      return;
    }
    const createdAt = new Date().toISOString();
    const filename = `mtg_repro_compare_bundle_${formatTimestampToken(createdAt)}.json`;
    const payload = {
      bundle_version: "repro_compare_bundle_v1",
      created_at_iso: createdAt,
      ui_harness: buildUiHarnessMetadata(),
      compare: {
        run_a: buildRunBundleRun(compareRunA, redactLocalPathsInExport),
        run_b: buildRunBundleRun(compareRunB, redactLocalPathsInExport),
        diff_summary: {
          build_hash_match:
            compareData.buildHashA !== null && compareData.buildHashB !== null ? compareData.buildHashMatch : null,
          graph_hash_match:
            compareData.graphHashA !== null && compareData.graphHashB !== null ? compareData.graphHashMatch : null,
          status_a: compareData.statusA,
          status_b: compareData.statusB,
          unknown_codes_added: compareData.unknownCodesAdded,
          unknown_codes_removed: compareData.unknownCodesRemoved,
          canonical_slots_all_total_a: compareData.canonicalTotalA,
          canonical_slots_all_total_b: compareData.canonicalTotalB,
        },
      },
    };
    triggerJsonDownload(filename, payload);
    setCopyNotice(`Exported ${filename}`);
  }

  async function copyToClipboard(label: string, value: string | null | undefined) {
    if (!value) {
      setCopyNotice(`${label} unavailable`);
      return;
    }
    if (!("clipboard" in navigator)) {
      setCopyNotice("Clipboard API unavailable");
      return;
    }
    try {
      await navigator.clipboard.writeText(value);
      setCopyNotice(`${label} copied`);
    } catch {
      setCopyNotice(`Failed to copy ${label}`);
    }
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const requestPayload: BuildRequestPayload = {
      db_snapshot_id: dbSnapshotId,
      profile_id: profileId,
      bracket_id: bracketId,
      format: "commander",
      commander,
      cards: parseCardsInput(cardsText),
      engine_patches_v0: [],
    };

    setLastRequestPayload(requestPayload);
    setLoading(true);
    setRuntimeError(null);
    setContractMatched(null);
    setCopyNotice(null);
    setCanonicalFilter("");
    setSelectedRunId(null);
    setDeterminismDrillResult(null);

    try {
      const execution = await executeBuildRequest(requestPayload);
      applyExecutionToInspector(requestPayload, execution);
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unknown runtime error");
      setHttpStatus(null);
      setRawResponse("");
      setResponseBody(null);
      setContractMatched(null);
    } finally {
      setLoading(false);
    }
  }

  async function handleRunTwiceDeterminism() {
    const requestPayload: BuildRequestPayload = {
      db_snapshot_id: dbSnapshotId,
      profile_id: profileId,
      bracket_id: bracketId,
      format: "commander",
      commander,
      cards: parseCardsInput(cardsText),
      engine_patches_v0: [],
    };

    setLastRequestPayload(requestPayload);
    setDeterminismLoading(true);
    setRuntimeError(null);
    setContractMatched(null);
    setCopyNotice(null);
    setCanonicalFilter("");
    setSelectedRunId(null);
    setDeterminismDrillResult(null);

    try {
      const run1Execution = await executeBuildRequest(requestPayload);
      const run2Execution = await executeBuildRequest(requestPayload);

      const run1 = summarizeDrillRun(run1Execution);
      const run2 = summarizeDrillRun(run2Execution);
      const unknownCodesAdded = run2.unknownCodes.filter((code: string) => !run1.unknownCodes.includes(code));
      const unknownCodesRemoved = run1.unknownCodes.filter((code: string) => !run2.unknownCodes.includes(code));
      const determinismPass =
        run1.buildHashV1 !== null &&
        run2.buildHashV1 !== null &&
        run1.graphHashV2 !== null &&
        run2.graphHashV2 !== null &&
        run1.buildHashV1 === run2.buildHashV1 &&
        run1.graphHashV2 === run2.graphHashV2;

      setDeterminismDrillResult({
        run1,
        run2,
        determinismPass,
        unknownCodesAdded,
        unknownCodesRemoved,
      });

      if (!run1Execution.contractMatch) {
        applyExecutionToInspector(requestPayload, run1Execution);
      } else if (!run2Execution.contractMatch) {
        applyExecutionToInspector(requestPayload, run2Execution);
      } else {
        applyExecutionToInspector(requestPayload, run2Execution);
      }
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unknown runtime error");
      setHttpStatus(null);
      setRawResponse("");
      setResponseBody(null);
      setContractMatched(null);
    } finally {
      setDeterminismLoading(false);
    }
  }

  return (
    <div className="app-shell">
      {contractMatched === false ? <div className="banner banner-danger">UI contract mismatch</div> : null}

      {unknowns.length > 0 ? (
        <div className="banner banner-warning">
          <div>unknowns_count: {unknowns.length}</div>
          <ul>
            {unknownMessages.map((message: string, idx: number) => (
              <li key={`${idx}-${message}`}>{message}</li>
            ))}
          </ul>
          <button type="button" onClick={() => setActiveTab("summary")} disabled={!summaryTabEnabled}>
            Jump to Summary
          </button>
        </div>
      ) : null}

      <header className="panel">
        <h1>MTG UI Harness (read-only)</h1>
        {oracleTextBadge ? <div className="runtime-mode-badge">{oracleTextBadge}</div> : null}
        <div className="grid">
          <div>engine_version: {responseBody?.engine_version ?? "-"}</div>
          <div>ruleset_version: {responseBody?.ruleset_version ?? "-"}</div>
          <div>db_snapshot_id: {responseBody?.db_snapshot_id ?? "-"}</div>
          <div>profile_id: {responseBody?.profile_id ?? "-"}</div>
          <div>bracket_id: {responseBody?.bracket_id ?? "-"}</div>
          <div>status: {responseBody?.status ?? "-"}</div>
          <div className="copy-row">
            <span>build_hash_v1: {responseBody?.build_hash_v1 ?? "-"}</span>
            <button
              type="button"
              onClick={() => void copyToClipboard("build_hash_v1", responseBody?.build_hash_v1 ?? null)}
              disabled={!responseBody?.build_hash_v1}
            >
              Copy
            </button>
          </div>
          <div className="copy-row">
            <span>graph_hash_v2: {responseBody?.graph_hash_v2 ?? "-"}</span>
            <button
              type="button"
              onClick={() => void copyToClipboard("graph_hash_v2", responseBody?.graph_hash_v2 ?? null)}
              disabled={!responseBody?.graph_hash_v2}
            >
              Copy
            </button>
          </div>
          <div>http_status: {httpStatus ?? "-"}</div>
          <div>api_base_url: {API_BASE_URL}</div>
        </div>
        {copyNotice ? <div className="copy-notice">{copyNotice}</div> : null}
      </header>

      <section className="panel">
        <h2>Build Request Form</h2>
        <form onSubmit={handleSubmit} className="form-grid">
          <label>
            Snapshot ID
            <input value={dbSnapshotId} onChange={(e: ValueChangeEvent) => setDbSnapshotId(e.target.value)} />
          </label>
          <label>
            Profile ID
            <input value={profileId} onChange={(e: ValueChangeEvent) => setProfileId(e.target.value)} />
          </label>
          <label>
            Bracket ID
            <input value={bracketId} onChange={(e: ValueChangeEvent) => setBracketId(e.target.value)} />
          </label>
          <label>
            Commander
            <input value={commander} onChange={(e: ValueChangeEvent) => setCommander(e.target.value)} />
          </label>
          <label className="full-width">
            Cards (one per line, or comma-separated)
            <textarea
              rows={7}
              value={cardsText}
              onChange={(e: ValueChangeEvent) => setCardsText(e.target.value)}
              placeholder="Optional seed cards"
            />
          </label>
          <div className="form-actions full-width">
            <button type="submit" disabled={loading || determinismLoading}>
              {loading ? "Running..." : "Run /build"}
            </button>
            <button type="button" onClick={() => void handleRunTwiceDeterminism()} disabled={loading || determinismLoading}>
              {determinismLoading ? "Running Twice..." : "Run Twice (Determinism)"}
            </button>
          </div>
        </form>
        {determinismDrillResult ? (
          <div className="determinism-result">
            <div className="section-title">Determinism Drill</div>
            <div>
              run1 hashes: build_hash_v1={determinismDrillResult.run1.buildHashV1 ?? "-"}, graph_hash_v2={
                determinismDrillResult.run1.graphHashV2 ?? "-"
              }
            </div>
            <div>
              run2 hashes: build_hash_v1={determinismDrillResult.run2.buildHashV1 ?? "-"}, graph_hash_v2={
                determinismDrillResult.run2.graphHashV2 ?? "-"
              }
            </div>
            <div>determinism: {determinismDrillResult.determinismPass ? "PASS" : "FAIL"}</div>
            <div>
              unknown_codes added: {determinismDrillResult.unknownCodesAdded.length > 0 ? determinismDrillResult.unknownCodesAdded.join(", ") : "-"}
            </div>
            <div>
              unknown_codes removed: {determinismDrillResult.unknownCodesRemoved.length > 0 ? determinismDrillResult.unknownCodesRemoved.join(", ") : "-"}
            </div>
          </div>
        ) : null}
        {runtimeError ? <div className="error">runtime_error: {runtimeError}</div> : null}
      </section>

      <div className="inspector-layout">
        <aside className="panel runs-sidebar">
          <h2>Runs</h2>
          <div>stored_runs: {runHistory.length}</div>
          <div className="export-toggle">
            <label className="export-toggle-label">
              <input
                type="checkbox"
                checked={redactLocalPathsInExport}
                onChange={(event) => setRedactLocalPathsInExport(event.target.checked)}
              />
              <span>Redact local paths in export</span>
            </label>
          </div>
          {runHistory.length === 0 ? (
            <div>No runs yet.</div>
          ) : (
            <ul className="runs-list">
              {runHistory.map((entry: RunHistoryEntry) => (
                <li key={entry.id} className="run-entry">
                  <button
                    type="button"
                    className={selectedRunId === entry.id ? "run-item active" : "run-item"}
                    onClick={() => loadRunFromHistory(entry)}
                  >
                    <div>{entry.timestamp}</div>
                    <div>{entry.commanderInput}</div>
                    <div>
                      {entry.dbSnapshotId} / {entry.profileId} / {entry.bracketId}
                    </div>
                    <div>status: {entry.status}</div>
                    <div>build_hash_v1: {entry.buildHashV1 ?? "-"}</div>
                    <div>graph_hash_v2: {entry.graphHashV2 ?? "-"}</div>
                    <div>unknown_codes: {entry.unknownCodes.length > 0 ? entry.unknownCodes.join(", ") : "-"}</div>
                  </button>
                  <button type="button" className="run-export-button" onClick={() => exportRunBundle(entry)}>
                    Export
                  </button>
                </li>
              ))}
            </ul>
          )}
        </aside>

        <section className="panel inspector-panel">
          <div className="section-row">
            <h2>Inspector Tabs</h2>
            <button
              type="button"
              onClick={() => setCompareViewOpen((prev: boolean) => !prev)}
              disabled={runHistory.length < 2}
            >
              Compare
            </button>
          </div>

          {compareViewOpen ? (
            <div className="compare-view">
              <div className="section-title">Compare Runs (diff view)</div>
              <div className="compare-controls">
                <label>
                  Run A
                  <select value={compareRunAId} onChange={(e: ValueChangeEvent) => setCompareRunAId(e.target.value)}>
                    {runHistory.map((entry: RunHistoryEntry) => (
                      <option key={`run-a-${entry.id}`} value={entry.id}>
                        {entry.timestamp} | {entry.commanderInput}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Run B
                  <select value={compareRunBId} onChange={(e: ValueChangeEvent) => setCompareRunBId(e.target.value)}>
                    {runHistory.map((entry: RunHistoryEntry) => (
                      <option key={`run-b-${entry.id}`} value={entry.id}>
                        {entry.timestamp} | {entry.commanderInput}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="tab-actions compare-actions">
                <button type="button" onClick={() => exportCompareBundle()} disabled={!compareData}>
                  Export Compare Bundle
                </button>
              </div>

              {compareData ? (
                <div className="compare-grid">
                  <div>
                    build_hash_v1: {compareData.buildHashA ?? "-"} vs {compareData.buildHashB ?? "-"} (
                    {compareData.buildHashMatch ? "match" : "mismatch"})
                  </div>
                  <div>
                    graph_hash_v2: {compareData.graphHashA ?? "-"} vs {compareData.graphHashB ?? "-"} (
                    {compareData.graphHashMatch ? "match" : "mismatch"})
                  </div>
                  <div>
                    status: {compareData.statusA} vs {compareData.statusB}
                  </div>
                  <div>
                    unknown_codes added: {compareData.unknownCodesAdded.length > 0 ? compareData.unknownCodesAdded.join(", ") : "-"}
                  </div>
                  <div>
                    unknown_codes removed: {compareData.unknownCodesRemoved.length > 0 ? compareData.unknownCodesRemoved.join(", ") : "-"}
                  </div>
                  <div>
                    canonical_slots_all_total: {compareData.canonicalTotalA ?? "-"} vs {compareData.canonicalTotalB ?? "-"}
                  </div>
                  <div>
                    structural_snapshot_v1 keys A ({compareData.structuralKeysA.length}): {compareData.structuralKeysA.length > 0 ? compareData.structuralKeysA.join(", ") : "-"}
                  </div>
                  <div>
                    structural_snapshot_v1 keys B ({compareData.structuralKeysB.length}): {compareData.structuralKeysB.length > 0 ? compareData.structuralKeysB.join(", ") : "-"}
                  </div>
                  <div>
                    structural keys added: {compareData.structuralKeysAdded.length > 0 ? compareData.structuralKeysAdded.join(", ") : "-"}
                  </div>
                  <div>
                    structural keys removed: {compareData.structuralKeysRemoved.length > 0 ? compareData.structuralKeysRemoved.join(", ") : "-"}
                  </div>
                  <div>
                    graph_v1.stats keys A ({compareData.graphStatsKeysA.length}): {compareData.graphStatsKeysA.length > 0 ? compareData.graphStatsKeysA.join(", ") : "-"}
                  </div>
                  <div>
                    graph_v1.stats keys B ({compareData.graphStatsKeysB.length}): {compareData.graphStatsKeysB.length > 0 ? compareData.graphStatsKeysB.join(", ") : "-"}
                  </div>
                  <div>
                    graph_v1.stats keys added: {compareData.graphStatsKeysAdded.length > 0 ? compareData.graphStatsKeysAdded.join(", ") : "-"}
                  </div>
                  <div>
                    graph_v1.stats keys removed: {compareData.graphStatsKeysRemoved.length > 0 ? compareData.graphStatsKeysRemoved.join(", ") : "-"}
                  </div>
                </div>
              ) : (
                <div>Select runs A and B to compare.</div>
              )}
            </div>
          ) : null}

        <div className="tabs">
          {summaryTabEnabled ? (
            <button
              onClick={() => setActiveTab("summary")}
              className={activeTab === "summary" ? "active" : ""}
              disabled={!summaryTabEnabled}
            >
              Summary
            </button>
          ) : null}
          {summaryTabEnabled && canonicalTabVisible ? (
            <button
              onClick={() => setActiveTab("canonical")}
              className={activeTab === "canonical" ? "active" : ""}
              disabled={!canonicalTabEnabled}
            >
              Canonical
            </button>
          ) : null}
          {summaryTabEnabled && structuralTabVisible ? (
            <button
              onClick={() => setActiveTab("structural")}
              className={activeTab === "structural" ? "active" : ""}
              disabled={!structuralTabEnabled}
            >
              Structural
            </button>
          ) : null}
          {summaryTabEnabled && graphTabVisible ? (
            <button
              onClick={() => setActiveTab("graph")}
              className={activeTab === "graph" ? "active" : ""}
              disabled={!graphTabEnabled}
            >
              Graph
            </button>
          ) : null}
          <button onClick={() => setActiveTab("raw")} className={activeTab === "raw" ? "active" : ""}>
            Raw JSON
          </button>
        </div>

        {activeTab === "summary" && summaryTabEnabled ? (
          <div className="tab-content">
            <div>status: {responseBody?.status ?? "-"}</div>
            <div>unknowns_count: {Array.isArray(responseBody?.unknowns) ? responseBody?.unknowns.length : 0}</div>
            <pre>{JSON.stringify(responseBody?.unknowns ?? [], null, 2)}</pre>
          </div>
        ) : null}

        {activeTab === "canonical" && canonicalTabEnabled ? (
          <div className="tab-content">
            <div className="tab-actions canonical-actions">
              <button
                type="button"
                onClick={() => void copyToClipboard("canonical_slots_all", canonicalSlotsAllJson)}
                disabled={!hasCanonicalSlotsAll}
              >
                Copy canonical table JSON
              </button>
              {hasCommanderCanonicalSlot ? (
                <button
                  type="button"
                  onClick={() => void copyToClipboard("commander_canonical_slot", commanderCanonicalSlotJson)}
                  disabled={!commanderCanonicalSlotJson}
                >
                  Copy commander slot JSON
                </button>
              ) : null}
            </div>

            {hasCommanderCanonicalSlot ? (
              <div>
                <div className="section-title">Commander Canonical Slot</div>
                <pre>{commanderCanonicalSlotJson}</pre>
              </div>
            ) : null}

            {hasDeckCardsCanonicalInputOrder ? (
              <div>
                <div className="section-title">Deck Cards Canonical Input Order</div>
                {hasDeckCardsCanonicalInputOrderTotal ? (
                  <div>deck_cards_canonical_input_order_total: {asString(deckCardsCanonicalInputOrderTotalRaw)}</div>
                ) : null}
                {deckCardsCanonicalInputOrder ? (
                  <ol className="value-list">
                    {deckCardsCanonicalInputOrder.map((entry: unknown, idx: number) => (
                      <li key={`deck-order-${idx}`}>{asString(entry)}</li>
                    ))}
                  </ol>
                ) : (
                  <pre>{JSON.stringify(deckCardsCanonicalInputOrderRaw ?? null, null, 2)}</pre>
                )}
              </div>
            ) : null}

            {hasDeckCardsSlotIdsPlayable ||
            hasDeckCardsSlotIdsNonplayable ||
            hasDeckCardsCanonicalPlayableSlotsTotal ||
            hasDeckCardsCanonicalNonplayableSlotsTotal ? (
              <div>
                <div className="section-title">Slot IDs Playable / Nonplayable</div>
                {hasDeckCardsCanonicalPlayableSlotsTotal ? (
                  <div>deck_cards_canonical_playable_slots_total: {asString(deckCardsCanonicalPlayableSlotsTotalRaw)}</div>
                ) : null}
                {hasDeckCardsCanonicalNonplayableSlotsTotal ? (
                  <div>
                    deck_cards_canonical_nonplayable_slots_total: {asString(deckCardsCanonicalNonplayableSlotsTotalRaw)}
                  </div>
                ) : null}
                {hasDeckCardsSlotIdsPlayable ? (
                  <div>
                    <div className="section-title">deck_cards_slot_ids_playable</div>
                    {deckCardsSlotIdsPlayable ? (
                      <ol className="value-list">
                        {deckCardsSlotIdsPlayable.map((entry: unknown, idx: number) => (
                          <li key={`playable-slot-${idx}`}>{asString(entry)}</li>
                        ))}
                      </ol>
                    ) : (
                      <pre>{JSON.stringify(deckCardsSlotIdsPlayableRaw ?? null, null, 2)}</pre>
                    )}
                  </div>
                ) : null}
                {hasDeckCardsSlotIdsNonplayable ? (
                  <div>
                    <div className="section-title">deck_cards_slot_ids_nonplayable</div>
                    {deckCardsSlotIdsNonplayable ? (
                      <ol className="value-list">
                        {deckCardsSlotIdsNonplayable.map((entry: unknown, idx: number) => (
                          <li key={`nonplayable-slot-${idx}`}>{asString(entry)}</li>
                        ))}
                      </ol>
                    ) : (
                      <pre>{JSON.stringify(deckCardsSlotIdsNonplayableRaw ?? null, null, 2)}</pre>
                    )}
                  </div>
                ) : null}
              </div>
            ) : null}

            {hasDeckCardsUnknownsBySlot ? (
              <div>
                <div className="section-title">Unknowns by Slot</div>
                {Array.isArray(deckCardsUnknownsBySlot) ? (
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {deckCardsUnknownsBySlot.map((entry: unknown, idx: number) => (
                        <tr key={`unknown-slot-${idx}`}>
                          <td>{idx}</td>
                          <td>{asString(entry)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <pre>{JSON.stringify(deckCardsUnknownsBySlot ?? null, null, 2)}</pre>
                )}
              </div>
            ) : null}

            {hasCanonicalSlotsAll ? (
              <div>
                <div className="section-title">Canonical Slots Table (truth table)</div>
                {canonicalSlotsAll ? (
                  <>
                    <label className="filter-label">
                      Filter (slot_id / input / resolved_name / resolved_oracle_id / status / codes)
                      <input
                        value={canonicalFilter}
                        onChange={(e: ValueChangeEvent) => setCanonicalFilter(e.target.value)}
                        placeholder="Type to filter"
                      />
                    </label>
                    <div>canonical_slots_all_total: {canonicalSlotsFiltered.length}</div>
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>slot_id</th>
                          <th>input</th>
                          <th>resolved_name</th>
                          <th>resolved_oracle_id</th>
                          <th>status</th>
                          <th>codes</th>
                        </tr>
                      </thead>
                      <tbody>
                        {canonicalSlotsFiltered.map((row: unknown, idx: number) => {
                          const rec = asRecord(row);
                          const codesRaw = rec ? rec.codes : undefined;
                          const codesValue = Array.isArray(codesRaw)
                            ? codesRaw.map((entry: unknown) => asString(entry)).join(", ")
                            : asString(codesRaw);
                          return (
                            <tr key={`canonical-slot-${idx}`}>
                              <td>{rec ? asString(rec.slot_id) : "-"}</td>
                              <td>{rec ? asString(rec.input) : "-"}</td>
                              <td>{rec ? asString(rec.resolved_name) : "-"}</td>
                              <td>{rec ? asString(rec.resolved_oracle_id) : "-"}</td>
                              <td>{rec ? asString(rec.status) : "-"}</td>
                              <td>{codesValue}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </>
                ) : (
                  <pre>{JSON.stringify(canonicalSlotsAllRaw ?? null, null, 2)}</pre>
                )}
              </div>
            ) : null}
          </div>
        ) : null}

        {activeTab === "structural" && structuralTabEnabled ? (
          <div className="tab-content">
            <div className="summary-grid">
              <div>required_primitives_v1 total: {structuralRequiredCount ?? "-"}</div>
              <div>present_primitives_v1 total: {structuralPresentCount ?? "-"}</div>
              <div>missing_primitives_v1 total: {structuralMissingCount ?? "-"}</div>
              <div>dead_slot_ids_v1 count: {structuralDeadSlotsCount ?? "-"}</div>
              <div>commander_dependency_signal_v1: {asString(structuralCommanderDependencySignal)}</div>
            </div>
            <div>
              structural_snapshot_v1 keys: {structuralSnapshot ? Object.keys(structuralSnapshot).join(", ") : "-"}
            </div>
            <pre>{JSON.stringify(structuralSnapshot ?? null, null, 2)}</pre>
          </div>
        ) : null}

        {activeTab === "graph" && graphTabEnabled ? (
          <div className="tab-content">
            <div className="summary-grid">
              <div>graph_nodes_total: {responseBody?.result?.graph_nodes_total ?? "-"}</div>
              <div>graph_edges_total: {responseBody?.result?.graph_edges_total ?? "-"}</div>
              <div>caps_hit: {graphCapsHit === undefined ? "-" : asString(graphCapsHit)}</div>
            </div>

            {graphNodeDegreeRows ? (
              <div>
                <div className="section-title">graph_node_degrees</div>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>node_id</th>
                      <th>degree</th>
                    </tr>
                  </thead>
                  <tbody>
                    {graphNodeDegreeRows.map((row: { nodeId: string; degree: string }, idx: number) => (
                      <tr key={`graph-degree-${idx}`}>
                        <td>{row.nodeId}</td>
                        <td>{row.degree}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}

            {graphComponents !== undefined ? (
              <div>
                <div className="section-title">graph_components</div>
                {graphComponentRows ? (
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>component_id</th>
                        <th>node_total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {graphComponentRows.map((row: { componentId: string; nodeTotal: string }, idx: number) => (
                        <tr key={`graph-component-${idx}`}>
                          <td>{row.componentId}</td>
                          <td>{row.nodeTotal}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <pre>{JSON.stringify(graphComponents, null, 2)}</pre>
                )}
              </div>
            ) : null}

            <pre>{JSON.stringify(graphSummary, null, 2)}</pre>
            <pre>{JSON.stringify(graphV1 ?? null, null, 2)}</pre>
          </div>
        ) : null}

        {activeTab === "raw" ? (
          <div className="tab-content">
            <div className="tab-actions">
              <button type="button" onClick={() => void copyToClipboard("raw_json_response", rawResponse)}>
                Copy Raw JSON
              </button>
            </div>
            <pre>{rawResponse || "No response yet."}</pre>
          </div>
        ) : null}
        </section>
      </div>

      <section className="panel">
        <h2>Last Request Payload</h2>
        <pre>{JSON.stringify(lastRequestPayload ?? null, null, 2)}</pre>
      </section>
    </div>
  );
}

export default App;
