import { useEffect, useMemo, useRef, useState } from "react";
import type { ChangeEvent, KeyboardEvent, MouseEvent } from "react";

import fixtureBuildResult from "../fixtures/build_result.json";

type DataMode = "file" | "api";
type JsonRecord = Record<string, unknown>;

type CardSuggestRow = {
  oracle_id: string;
  name: string;
  mana_cost: string | null;
  type_line: string | null;
  image_uri: string | null;
};

type BuildResponsePayload = JsonRecord & {
  result?: JsonRecord;
  unknowns?: unknown[];
};

type PrimitiveExplorerCardRow = {
  slot_id: string;
  name: string;
  oracle_id: string;
  type_line: string | null;
  image_uri: string | null;
  primitive_tags: string[];
};

type PrimitiveExplorerGroup = {
  primitive_id: string;
  count: number;
  slot_ids: string[];
  cards: PrimitiveExplorerCardRow[];
};

type ParsedDecklistRow = {
  name: string;
  count: number;
  source_order: number;
};

const DEFAULT_API_BASE = String(import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000").trim();
const SUGGEST_LIMIT_MAX = 20;
const FIXTURE_PATH_LABEL = "./ui_harness/fixtures/build_result.json";
const COMMANDER_SUGGEST_LIMIT = 12;
const MAX_DECK_COUNT_PER_LINE = 250;

const FIXTURE_ROOT = asRecord(fixtureBuildResult);
const FIXTURE_DEFAULT_SNAPSHOT_ID = firstNonEmptyString(FIXTURE_ROOT?.db_snapshot_id) || "";
const DEFAULT_PROFILE_ID = firstNonEmptyString(FIXTURE_ROOT?.profile_id) === "focused" ? "focused" : "default";
const DEFAULT_BRACKET_ID = firstNonEmptyString(FIXTURE_ROOT?.bracket_id) || "B3";

function asRecord(value: unknown): JsonRecord | null {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as JsonRecord;
  }
  return null;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asOptionalString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const token = value.trim();
  return token === "" ? null : token;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const rows: string[] = [];
  for (const rawEntry of value) {
    const token = asOptionalString(rawEntry);
    if (token !== null) {
      rows.push(token);
    }
  }
  return rows;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function firstNonEmptyString(...values: unknown[]): string | null {
  for (const value of values) {
    const token = asOptionalString(value);
    if (token !== null) {
      return token;
    }
  }
  return null;
}

function firstNumber(...values: unknown[]): number | null {
  for (const value of values) {
    const token = asNumber(value);
    if (token !== null) {
      return token;
    }
  }
  return null;
}

function getPath(root: unknown, path: string[]): unknown {
  let cursor: unknown = root;
  for (const key of path) {
    const row = asRecord(cursor);
    if (!row || !(key in row)) {
      return null;
    }
    cursor = row[key];
  }
  return cursor;
}

function toPrettyJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "{}";
  }
}

function safeParseJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
}

function normalizeApiBase(raw: string): string {
  const token = raw.trim();
  if (token === "") {
    return "http://127.0.0.1:8000";
  }
  return token.endsWith("/") ? token.slice(0, -1) : token;
}

function buildLocalCardImageUrl(apiBaseUrl: string, oracleId: string, size: "normal" | "small" | "large" | "png" | "art_crop" | "border_crop" = "normal"): string {
  const base = normalizeApiBase(apiBaseUrl);
  return `${base}/cards/image/${encodeURIComponent(oracleId)}?size=${encodeURIComponent(size)}`;
}

function clampSuggestLimit(value: number): number {
  if (!Number.isFinite(value)) {
    return SUGGEST_LIMIT_MAX;
  }
  const rounded = Math.trunc(value);
  return Math.min(Math.max(rounded, 1), SUGGEST_LIMIT_MAX);
}

function toSingleLineSnippet(raw: string, maxLength = 180): string {
  const normalized = raw.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength)}...`;
}

function uniqueSortedStrings(values: string[]): string[] {
  const deduped = new Set<string>();
  for (const value of values) {
    const token = value.trim();
    if (token !== "") {
      deduped.add(token);
    }
  }
  return Array.from(deduped).sort((a: string, b: string) => a.localeCompare(b));
}

function extractPrimitiveIds(value: unknown): string[] {
  const rows = asArray(value);
  const primitiveIds: string[] = [];

  for (const rawRow of rows) {
    const asText = asOptionalString(rawRow);
    if (asText !== null) {
      primitiveIds.push(asText);
      continue;
    }

    const row = asRecord(rawRow);
    if (!row) {
      continue;
    }

    const primitiveId = firstNonEmptyString(row.primitive_id, row.primitive, row.id, row.tag_id, row.code);
    if (primitiveId !== null) {
      primitiveIds.push(primitiveId);
    }
  }

  return uniqueSortedStrings(primitiveIds);
}

function normalizeSlotIds(value: unknown): string[] {
  const rows = asArray(value);
  const slotIds: string[] = [];

  for (const rawRow of rows) {
    const asText = asOptionalString(rawRow);
    if (asText !== null) {
      slotIds.push(asText);
      continue;
    }

    const row = asRecord(rawRow);
    if (!row) {
      continue;
    }

    const slotId = firstNonEmptyString(row.slot_id, row.id);
    if (slotId !== null) {
      slotIds.push(slotId);
    }
  }

  return uniqueSortedStrings(slotIds);
}

function hasOwnKey(record: JsonRecord | null, key: string): boolean {
  return record !== null && Object.prototype.hasOwnProperty.call(record, key);
}

function cardNameSortKey(value: string): string {
  return value.trim().toLowerCase();
}

function mapPrimitiveCardToSuggestRow(row: PrimitiveExplorerCardRow): CardSuggestRow {
  return {
    oracle_id: row.oracle_id,
    name: row.name,
    mana_cost: null,
    type_line: row.type_line,
    image_uri: row.image_uri,
  };
}

function parseDecklistInput(rawDecklist: string): ParsedDecklistRow[] {
  const lines = rawDecklist.split(/\r?\n/);
  const rows: ParsedDecklistRow[] = [];

  for (const rawLine of lines) {
    const trimmed = rawLine.trim();
    if (trimmed === "" || trimmed.startsWith("#") || trimmed.startsWith("//")) {
      continue;
    }

    let count = 1;
    let name = trimmed;

    const countMatch = trimmed.match(/^(\d+)(?:\s*[xX])?\s+(.+)$/);
    if (countMatch) {
      const parsedCount = Number(countMatch[1]);
      const parsedName = countMatch[2].trim();
      if (Number.isFinite(parsedCount) && parsedCount >= 1 && parsedCount <= MAX_DECK_COUNT_PER_LINE && parsedName !== "") {
        count = Math.trunc(parsedCount);
        name = parsedName;
      }
    }

    if (name === "") {
      continue;
    }

    rows.push({
      name,
      count,
      source_order: rows.length,
    });
  }

  return rows;
}

function expandDecklistRowsInInputOrder(rows: ParsedDecklistRow[]): string[] {
  const cards: string[] = [];
  for (const row of rows) {
    const safeCount = Number.isFinite(row.count) ? Math.max(1, Math.trunc(row.count)) : 1;
    for (let i = 0; i < safeCount; i += 1) {
      cards.push(row.name);
    }
  }
  return cards;
}

function buildNormalizedDeckPreviewLines(cardsInPayloadOrder: string[]): string[] {
  return cardsInPayloadOrder
    .map((name: string, index: number) => ({
      name,
      source_order: index,
    }))
    .sort((a: { name: string; source_order: number }, b: { name: string; source_order: number }) => {
      const byName = cardNameSortKey(a.name).localeCompare(cardNameSortKey(b.name));
      if (byName !== 0) {
        return byName;
      }
      const byLiteral = a.name.localeCompare(b.name);
      if (byLiteral !== 0) {
        return byLiteral;
      }
      return a.source_order - b.source_order;
    })
    .map((row: { name: string }) => `1 ${row.name}`);
}

function extractLatestSnapshotId(payload: unknown): string {
  const root = asRecord(payload);
  const rows = asArray(root?.snapshots);
  for (const rawRow of rows) {
    const asText = asOptionalString(rawRow);
    if (asText !== null) {
      return asText;
    }

    const row = asRecord(rawRow);
    if (!row) {
      continue;
    }

    const snapshotId = firstNonEmptyString(row.snapshot_id, row.id, row.db_snapshot_id);
    if (snapshotId !== null) {
      return snapshotId;
    }
  }

  return "";
}

async function fetchLatestSnapshotIdFromApi(base: string): Promise<string> {
  const response = await fetch(`${base}/snapshots?limit=1`, {
    method: "GET",
  });
  const text = await response.text();
  const parsed = safeParseJson(text);

  if (!response.ok) {
    const responseSnippet = toSingleLineSnippet(text);
    const debugResponse = responseSnippet === "" ? "(empty response body)" : responseSnippet;
    throw new Error(`HTTP ${response.status} from /snapshots | response=${debugResponse}`);
  }

  const latestSnapshotId = extractLatestSnapshotId(parsed);
  if (latestSnapshotId === "") {
    throw new Error("No snapshots returned from /snapshots.");
  }

  return latestSnapshotId;
}

function parseCardSuggestRows(payload: unknown): CardSuggestRow[] {
  const root = asRecord(payload);
  const rawRows = asArray(root?.results);
  const rows: CardSuggestRow[] = [];

  for (const rawRow of rawRows) {
    const row = asRecord(rawRow);
    if (!row) {
      continue;
    }
    const name = asOptionalString(row.name);
    if (name === null) {
      continue;
    }

    rows.push({
      oracle_id: asOptionalString(row.oracle_id) || "",
      name,
      mana_cost: asOptionalString(row.mana_cost),
      type_line: asOptionalString(row.type_line),
      image_uri: asOptionalString(row.image_uri),
    });

    if (rows.length >= 20) {
      break;
    }
  }

  return rows;
}

function yesNoUnknown(value: unknown): string {
  if (value === true) {
    return "YES";
  }
  if (value === false) {
    return "NO";
  }
  return "-";
}

export default function Phase1Harness() {
  const [mode, setMode] = useState<DataMode>("file");
  const [apiBase, setApiBase] = useState(DEFAULT_API_BASE);
  const [dbSnapshotId, setDbSnapshotId] = useState(FIXTURE_DEFAULT_SNAPSHOT_ID);
  const [profileId, setProfileId] = useState(DEFAULT_PROFILE_ID);
  const [bracketId, setBracketId] = useState(DEFAULT_BRACKET_ID);
  const [commander, setCommander] = useState("Krenko, Mob Boss");
  const [cardsInput, setCardsInput] = useState("");

  const [buildPayload, setBuildPayload] = useState<BuildResponsePayload | null>(null);
  const [rawPayload, setRawPayload] = useState("");
  const [payloadSource, setPayloadSource] = useState("Not loaded");
  const [loadingBuild, setLoadingBuild] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);

  const [searchQuery, setSearchQuery] = useState("");
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchResults, setSearchResults] = useState<CardSuggestRow[]>([]);
  const [activeSuggestionIndex, setActiveSuggestionIndex] = useState(-1);
  const [selectedSuggestion, setSelectedSuggestion] = useState<CardSuggestRow | null>(null);
  const [commanderSuggestLoading, setCommanderSuggestLoading] = useState(false);
  const [commanderSuggestError, setCommanderSuggestError] = useState<string | null>(null);
  const [commanderSuggestRows, setCommanderSuggestRows] = useState<CardSuggestRow[]>([]);
  const [selectedPrimitiveId, setSelectedPrimitiveId] = useState<string | null>(null);
  const [hoverSuggestion, setHoverSuggestion] = useState<CardSuggestRow | null>(null);
  const [hoverPreviewSource, setHoverPreviewSource] = useState<"search" | "primitive" | null>(null);
  const [hoverPrimitiveTags, setHoverPrimitiveTags] = useState<string[]>([]);
  const [hoverVisible, setHoverVisible] = useState(false);
  const [previewImageFailures, setPreviewImageFailures] = useState<Record<string, true>>({});

  const hoverDelayTimerRef = useRef<number | null>(null);
  const suggestRequestIdRef = useRef(0);
  const commanderSuggestRequestIdRef = useRef(0);

  const parsedDecklistRows = useMemo(() => parseDecklistInput(cardsInput), [cardsInput]);
  const deckCardsInPayloadOrder = useMemo(
    () => expandDecklistRowsInInputOrder(parsedDecklistRows),
    [parsedDecklistRows],
  );
  const normalizedDeckPreviewLines = useMemo(
    () => buildNormalizedDeckPreviewLines(deckCardsInPayloadOrder),
    [deckCardsInPayloadOrder],
  );

  function clearHoverTimer() {
    if (hoverDelayTimerRef.current !== null) {
      window.clearTimeout(hoverDelayTimerRef.current);
      hoverDelayTimerRef.current = null;
    }
  }

  function hideHoverPreview() {
    clearHoverTimer();
    setHoverVisible(false);
    setHoverPreviewSource(null);
    setHoverPrimitiveTags([]);
  }

  function showHoverPreview(row: CardSuggestRow, source: "search" | "primitive", primitiveTags: string[] = []) {
    clearHoverTimer();
    hoverDelayTimerRef.current = window.setTimeout(() => {
      setHoverSuggestion(row);
      setHoverPreviewSource(source);
      setHoverPrimitiveTags(uniqueSortedStrings(primitiveTags));
      setHoverVisible(true);
      hoverDelayTimerRef.current = null;
    }, 150);
  }

  function loadFixturePayload() {
    const payload = fixtureBuildResult as BuildResponsePayload;
    setBuildPayload(payload);
    setRawPayload(toPrettyJson(payload));
    setPayloadSource(`File mode (${FIXTURE_PATH_LABEL})`);
    setRuntimeError(null);
  }

  async function runBuildApi() {
    setLoadingBuild(true);
    setRuntimeError(null);

    try {
      const base = normalizeApiBase(apiBase);
      let snapshotId = dbSnapshotId.trim();
      if (snapshotId === "") {
        snapshotId = await fetchLatestSnapshotIdFromApi(base);
        setDbSnapshotId(snapshotId);
      }

      const requestBody = {
        db_snapshot_id: snapshotId,
        profile_id: profileId.trim(),
        bracket_id: bracketId.trim(),
        format: "commander",
        commander: commander.trim(),
        cards: deckCardsInPayloadOrder,
        engine_patches_v0: [],
      };

      const response = await fetch(`${base}/build`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      const text = await response.text();
      const parsed = safeParseJson(text);
      if (!response.ok) {
        const responseSnippet = toSingleLineSnippet(text);
        const debugResponse = responseSnippet === "" ? "(empty response body)" : responseSnippet;
        throw new Error(`HTTP ${response.status} from /build | response=${debugResponse}`);
      }

      const root = asRecord(parsed);
      if (!root) {
        throw new Error("Build response was not a JSON object.");
      }

      const payload = root as BuildResponsePayload;
      setBuildPayload(payload);
      setRawPayload(toPrettyJson(payload));
      setPayloadSource(`API mode (${base}/build)`);
    } catch (error) {
      let message = "Unknown /build runtime error";
      if (error instanceof TypeError) {
        message = [
          "Failed to fetch /build",
          `apiBaseUrl=${normalizeApiBase(apiBase)}`,
          "API not reachable (backend unavailable or CORS blocked).",
        ].join(" | ");
      } else if (error instanceof Error) {
        message = error.message;
      }
      setRuntimeError(message);
    } finally {
      setLoadingBuild(false);
    }
  }

  function selectSuggestion(row: CardSuggestRow) {
    setSearchQuery(row.name);
    setSelectedSuggestion(row);
    setSearchOpen(false);
    setActiveSuggestionIndex(-1);
  }

  function handleSearchKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Escape") {
      setSearchOpen(false);
      setActiveSuggestionIndex(-1);
      return;
    }

    if (event.key === "Tab" && searchResults.length > 0) {
      event.preventDefault();
      selectSuggestion(searchResults[0]);
      return;
    }

    if (!searchOpen || searchResults.length === 0) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveSuggestionIndex((prev: number) => {
        if (prev < 0) {
          return 0;
        }
        return (prev + 1) % searchResults.length;
      });
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveSuggestionIndex((prev: number) => {
        if (prev < 0) {
          return searchResults.length - 1;
        }
        return (prev - 1 + searchResults.length) % searchResults.length;
      });
      return;
    }

    if (event.key === "Enter") {
      const index = activeSuggestionIndex >= 0 ? activeSuggestionIndex : 0;
      const row = searchResults[index];
      if (!row) {
        return;
      }
      event.preventDefault();
      selectSuggestion(row);
    }
  }

  function handleSuggestionMouseEnter(row: CardSuggestRow) {
    showHoverPreview(row, "search", []);
  }

  function handleSuggestionMouseLeave() {
    clearHoverTimer();
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

  useEffect(() => {
    if (mode === "file") {
      loadFixturePayload();
      if (FIXTURE_DEFAULT_SNAPSHOT_ID !== "") {
        setDbSnapshotId(FIXTURE_DEFAULT_SNAPSHOT_ID);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode]);

  useEffect(() => {
    if (mode !== "api") {
      return;
    }
    if (dbSnapshotId.trim() !== "") {
      return;
    }

    const base = normalizeApiBase(apiBase);
    let cancelled = false;

    void (async () => {
      try {
        const latestSnapshotId = await fetchLatestSnapshotIdFromApi(base);
        if (!cancelled && latestSnapshotId !== "") {
          setDbSnapshotId(latestSnapshotId);
        }
      } catch {
        // Leave blank; runBuildApi will report a detailed error if snapshot resolution fails.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [apiBase, dbSnapshotId, mode]);

  useEffect(() => {
    return () => {
      clearHoverTimer();
    };
  }, []);

  useEffect(() => {
    const query = searchQuery.trim();
    if (query.length < 2) {
      setSearchLoading(false);
      setSearchError(null);
      setSearchResults([]);
      setSearchOpen(false);
      setActiveSuggestionIndex(-1);
      return;
    }

    const requestId = suggestRequestIdRef.current + 1;
    suggestRequestIdRef.current = requestId;
    const controller = new AbortController();
    const base = normalizeApiBase(apiBase);
    const safeLimit = clampSuggestLimit(SUGGEST_LIMIT_MAX);
    const snapshotPart = dbSnapshotId.trim() !== "" ? `&snapshot_id=${encodeURIComponent(dbSnapshotId.trim())}` : "";
    const url = `${base}/cards/suggest?q=${encodeURIComponent(query)}${snapshotPart}&limit=${safeLimit}`;

    setSearchLoading(true);
    setSearchError(null);

    const timerId = window.setTimeout(async () => {
      try {
        const response = await fetch(url, {
          method: "GET",
          signal: controller.signal,
        });

        const text = await response.text();
        const parsed = safeParseJson(text);
        if (!response.ok) {
          const responseSnippet = toSingleLineSnippet(text);
          const debugResponse = responseSnippet === "" ? "(empty response body)" : responseSnippet;
          throw new Error(
            `Failed to fetch /cards/suggest (HTTP ${response.status}) | apiBaseUrl=${base} | requestUrl=${url} | response=${debugResponse}`,
          );
        }

        if (requestId !== suggestRequestIdRef.current) {
          return;
        }

        const rows = parseCardSuggestRows(parsed);
        setSearchResults(rows);
        setSearchOpen(true);
        setActiveSuggestionIndex(rows.length > 0 ? 0 : -1);
      } catch (error) {
        if (controller.signal.aborted) {
          return;
        }
        if (requestId !== suggestRequestIdRef.current) {
          return;
        }
        let message = "Unknown /cards/suggest runtime error";
        if (error instanceof TypeError) {
          message = [
            "Failed to fetch /cards/suggest",
            `apiBaseUrl=${base}`,
            `requestUrl=${url}`,
            "API not reachable (backend unavailable or CORS blocked).",
          ].join(" | ");
        } else if (error instanceof Error) {
          message = error.message;
        }
        setSearchResults([]);
        setSearchOpen(false);
        setActiveSuggestionIndex(-1);
        setSearchError(message);
      } finally {
        if (requestId === suggestRequestIdRef.current) {
          setSearchLoading(false);
        }
      }
    }, 60);

    return () => {
      controller.abort();
      window.clearTimeout(timerId);
    };
  }, [apiBase, dbSnapshotId, searchQuery]);

  useEffect(() => {
    if (mode !== "api") {
      setCommanderSuggestLoading(false);
      setCommanderSuggestError(null);
      setCommanderSuggestRows([]);
      return;
    }

    const query = commander.trim();
    if (query.length < 2) {
      setCommanderSuggestLoading(false);
      setCommanderSuggestError(null);
      setCommanderSuggestRows([]);
      return;
    }

    const requestId = commanderSuggestRequestIdRef.current + 1;
    commanderSuggestRequestIdRef.current = requestId;
    const controller = new AbortController();
    const base = normalizeApiBase(apiBase);
    const safeLimit = clampSuggestLimit(COMMANDER_SUGGEST_LIMIT);
    const snapshotPart = dbSnapshotId.trim() !== "" ? `&snapshot_id=${encodeURIComponent(dbSnapshotId.trim())}` : "";
    const url = `${base}/cards/suggest?q=${encodeURIComponent(query)}${snapshotPart}&limit=${safeLimit}`;

    setCommanderSuggestLoading(true);
    setCommanderSuggestError(null);

    const timerId = window.setTimeout(async () => {
      try {
        const response = await fetch(url, {
          method: "GET",
          signal: controller.signal,
        });
        const text = await response.text();
        const parsed = safeParseJson(text);
        if (!response.ok) {
          const responseSnippet = toSingleLineSnippet(text);
          const debugResponse = responseSnippet === "" ? "(empty response body)" : responseSnippet;
          throw new Error(`Failed to fetch /cards/suggest (HTTP ${response.status}) | response=${debugResponse}`);
        }

        if (requestId !== commanderSuggestRequestIdRef.current) {
          return;
        }

        setCommanderSuggestRows(parseCardSuggestRows(parsed));
      } catch (error) {
        if (controller.signal.aborted) {
          return;
        }
        if (requestId !== commanderSuggestRequestIdRef.current) {
          return;
        }

        const message = error instanceof Error ? error.message : "Unknown commander autocomplete error";
        setCommanderSuggestRows([]);
        setCommanderSuggestError(message);
      } finally {
        if (requestId === commanderSuggestRequestIdRef.current) {
          setCommanderSuggestLoading(false);
        }
      }
    }, 60);

    return () => {
      controller.abort();
      window.clearTimeout(timerId);
    };
  }, [apiBase, commander, dbSnapshotId, mode]);

  const result = asRecord(buildPayload?.result);
  const unknowns = asArray(buildPayload?.unknowns);
  const availablePanels = asRecord(result?.available_panels_v1);

  const structuralSnapshot = asRecord(result?.structural_snapshot_v1);
  const profileBracketEnforcement = asRecord(result?.profile_bracket_enforcement_v1);
  const profileBracketCounts = asRecord(profileBracketEnforcement?.counts);
  const bracketComplianceSummary = asRecord(result?.bracket_compliance_summary_v1);
  const engineRequirementDetectionV1 = asRecord(result?.engine_requirement_detection_v1);
  const engineCoherenceV1 = asRecord(result?.engine_coherence_v1);
  const engineCoherenceV1Summary = asRecord(engineCoherenceV1?.summary);

  const commanderDependencyV2 = asRecord(result?.commander_dependency_v2);
  const engineCoherenceV2 = asRecord(result?.engine_coherence_v2);
  const stressTransformV2 = asRecord(result?.stress_transform_engine_v2);

  const pipelineVersions = asRecord(result?.pipeline_versions);
  const graphV1 = asRecord(result?.graph_v1);
  const graphBounds = asRecord(graphV1?.bounds);
  const graphStats = asRecord(graphV1?.stats);
  const graphCapsHit = asRecord(graphStats?.caps_hit);
  const primitiveIndexBySlot = asRecord(result?.primitive_index_by_slot);
  const slotIdsByPrimitive = asRecord(result?.slot_ids_by_primitive);
  const primitiveIndexTotals = asRecord(result?.primitive_index_totals);
  const hasPrimitiveIndexBySlotField = hasOwnKey(result, "primitive_index_by_slot");
  const hasSlotIdsByPrimitiveField = hasOwnKey(result, "slot_ids_by_primitive");

  const structuralStatus =
    firstNonEmptyString(structuralSnapshot?.status, structuralSnapshot?.state, structuralSnapshot ? "PRESENT" : "MISSING") ||
    "-";

  const gameChangerCount = firstNumber(
    profileBracketCounts?.game_changers_in_deck,
    bracketComplianceSummary?.game_changers_in_deck,
    getPath(bracketComplianceSummary, ["counts", "game_changers_in_deck"]),
  );

  const commanderDependencyV1 =
    firstNonEmptyString(
      engineRequirementDetectionV1?.commander_dependency,
      engineRequirementDetectionV1?.commander_dependent,
      getPath(engineRequirementDetectionV1, ["summary", "commander_dependency"]),
    ) || "-";

  const coherenceV1Status = firstNonEmptyString(engineCoherenceV1?.status) || "-";
  const coherenceV1OverlapScore = firstNumber(engineCoherenceV1Summary?.overlap_score, engineCoherenceV1Summary?.coherence_score);

  const v2CommanderAccessRequired = firstNonEmptyString(
    commanderDependencyV2?.access_required,
    getPath(commanderDependencyV2, ["summary", "access_required"]),
  );
  const v2CommanderAmplifier = firstNumber(
    commanderDependencyV2?.amplifier,
    getPath(commanderDependencyV2, ["summary", "amplifier"]),
  );
  const v2CommanderLineShare = firstNumber(
    commanderDependencyV2?.line_share_percent,
    getPath(commanderDependencyV2, ["summary", "line_share_percent"]),
  );

  const v2CoherenceScore = firstNumber(
    engineCoherenceV2?.score,
    engineCoherenceV2?.coherence_score,
    engineCoherenceV2?.score_v2,
  );
  const v2CoherenceComponents = asRecord(engineCoherenceV2?.components);

  const stressPolicyVersion = firstNonEmptyString(stressTransformV2?.policy_version) || "-";
  const stressOperators = asArray(stressTransformV2?.operators_applied)
    .map((entry: unknown) => asRecord(entry))
    .filter((entry: JsonRecord | null): entry is JsonRecord => entry !== null);

  const commanderCanonicalSlot = result?.commander_canonical_slot;
  const unknownsBySlot = result?.deck_cards_unknowns_by_slot ?? result?.unknowns_by_slot;

  const slotPrimitiveTagsById = useMemo(() => {
    const rowsBySlotId = new Map<string, string[]>();
    if (!primitiveIndexBySlot) {
      return rowsBySlotId;
    }

    for (const slotId of Object.keys(primitiveIndexBySlot).sort((a: string, b: string) => a.localeCompare(b))) {
      const primitiveTags = extractPrimitiveIds(primitiveIndexBySlot[slotId]);
      if (primitiveTags.length > 0) {
        rowsBySlotId.set(slotId, primitiveTags);
      }
    }

    return rowsBySlotId;
  }, [primitiveIndexBySlot]);

  const slotCardRowsById = useMemo(() => {
    const rowsBySlotId = new Map<string, PrimitiveExplorerCardRow>();

    function upsertFromRaw(raw: unknown): void {
      const row = asRecord(raw);
      if (!row) {
        return;
      }

      const slotId = firstNonEmptyString(row.slot_id, row.id);
      if (slotId === null) {
        return;
      }

      const nextName = firstNonEmptyString(row.resolved_name, row.name, row.input, row.card_name, row.slot_name, slotId) || slotId;
      const nextOracleId = firstNonEmptyString(row.resolved_oracle_id, row.oracle_id) || "";
      const nextTypeLine = firstNonEmptyString(row.type_line, row.resolved_type_line, row.card_type_line);
      const nextImageUri = firstNonEmptyString(row.image_uri, row.image_url, row.art_uri, row.art_url);
      const nextPrimitiveTags = uniqueSortedStrings([
        ...extractPrimitiveIds(row.primitives),
        ...extractPrimitiveIds(row.primitive_ids),
        ...extractPrimitiveIds(row.primitive_tags),
        ...asStringArray(row.tags),
      ]);

      const previous = rowsBySlotId.get(slotId);
      if (!previous) {
        rowsBySlotId.set(slotId, {
          slot_id: slotId,
          name: nextName,
          oracle_id: nextOracleId,
          type_line: nextTypeLine,
          image_uri: nextImageUri,
          primitive_tags: nextPrimitiveTags,
        });
        return;
      }

      const preferredName = cardNameSortKey(previous.name) !== cardNameSortKey(slotId) ? previous.name : nextName;
      rowsBySlotId.set(slotId, {
        slot_id: slotId,
        name: preferredName,
        oracle_id: previous.oracle_id || nextOracleId,
        type_line: previous.type_line || nextTypeLine,
        image_uri: previous.image_uri || nextImageUri,
        primitive_tags: uniqueSortedStrings([...previous.primitive_tags, ...nextPrimitiveTags]),
      });
    }

    upsertFromRaw(result?.commander_canonical_slot);
    for (const row of asArray(result?.canonical_slots_all)) {
      upsertFromRaw(row);
    }
    for (const row of asArray(result?.graph_nodes)) {
      upsertFromRaw(row);
    }
    for (const row of asArray(result?.deck_cards_playable)) {
      upsertFromRaw(row);
    }
    for (const row of asArray(result?.deck_cards_nonplayable)) {
      upsertFromRaw(row);
    }

    return rowsBySlotId;
  }, [result]);

  const primitiveExplorer = useMemo(() => {
    const primitiveToSlotIds = new Map<string, Set<string>>();
    let source: "primitive_index_by_slot" | "slot_ids_by_primitive" | "missing" = "missing";

    function addPrimitiveSlotPair(primitiveIdRaw: string, slotIdRaw: string): void {
      const primitiveId = primitiveIdRaw.trim();
      const slotId = slotIdRaw.trim();
      if (primitiveId === "" || slotId === "") {
        return;
      }
      const slotIds = primitiveToSlotIds.get(primitiveId) || new Set<string>();
      slotIds.add(slotId);
      primitiveToSlotIds.set(primitiveId, slotIds);
    }

    if (hasPrimitiveIndexBySlotField) {
      source = "primitive_index_by_slot";

      if (primitiveIndexBySlot) {
        for (const slotId of Object.keys(primitiveIndexBySlot).sort((a: string, b: string) => a.localeCompare(b))) {
          const primitiveIds = extractPrimitiveIds(primitiveIndexBySlot[slotId]);
          for (const primitiveId of primitiveIds) {
            addPrimitiveSlotPair(primitiveId, slotId);
          }
        }
      }

      if (primitiveToSlotIds.size === 0 && slotIdsByPrimitive) {
        for (const primitiveId of Object.keys(slotIdsByPrimitive).sort((a: string, b: string) => a.localeCompare(b))) {
          const slotIds = normalizeSlotIds(slotIdsByPrimitive[primitiveId]);
          for (const slotId of slotIds) {
            addPrimitiveSlotPair(primitiveId, slotId);
          }
        }
      }
    } else if (hasSlotIdsByPrimitiveField) {
      source = "slot_ids_by_primitive";
      if (slotIdsByPrimitive) {
        for (const primitiveId of Object.keys(slotIdsByPrimitive).sort((a: string, b: string) => a.localeCompare(b))) {
          const slotIds = normalizeSlotIds(slotIdsByPrimitive[primitiveId]);
          for (const slotId of slotIds) {
            addPrimitiveSlotPair(primitiveId, slotId);
          }
        }
      }
    }

    const groups: PrimitiveExplorerGroup[] = Array.from(primitiveToSlotIds.entries()).map(([primitiveId, slotIdSet]) => {
      const slotIds = Array.from(slotIdSet).sort((a: string, b: string) => a.localeCompare(b));
      const cards: PrimitiveExplorerCardRow[] = slotIds
        .map((slotId: string) => {
          const cardRow = slotCardRowsById.get(slotId);
          const mergedTags = uniqueSortedStrings([
            ...(slotPrimitiveTagsById.get(slotId) || []),
            ...(cardRow ? cardRow.primitive_tags : []),
          ]);

          return {
            slot_id: slotId,
            name: cardRow?.name || slotId,
            oracle_id: cardRow?.oracle_id || "",
            type_line: cardRow?.type_line || null,
            image_uri: cardRow?.image_uri || null,
            primitive_tags: mergedTags,
          };
        })
        .sort((a: PrimitiveExplorerCardRow, b: PrimitiveExplorerCardRow) => {
          const byName = cardNameSortKey(a.name).localeCompare(cardNameSortKey(b.name));
          if (byName !== 0) {
            return byName;
          }
          return a.slot_id.localeCompare(b.slot_id);
        });

      return {
        primitive_id: primitiveId,
        count: slotIds.length,
        slot_ids: slotIds,
        cards,
      };
    });

    groups.sort((a: PrimitiveExplorerGroup, b: PrimitiveExplorerGroup) => {
      if (a.count !== b.count) {
        return b.count - a.count;
      }
      return a.primitive_id.localeCompare(b.primitive_id);
    });

    return {
      source,
      groups,
    };
  }, [
    hasPrimitiveIndexBySlotField,
    hasSlotIdsByPrimitiveField,
    primitiveIndexBySlot,
    slotIdsByPrimitive,
    slotCardRowsById,
    slotPrimitiveTagsById,
  ]);

  const primitiveExplorerAvailable = primitiveExplorer.source !== "missing";
  const primitiveExplorerSourceLabel =
    primitiveExplorer.source === "primitive_index_by_slot"
      ? "result.primitive_index_by_slot"
      : primitiveExplorer.source === "slot_ids_by_primitive"
      ? "result.slot_ids_by_primitive"
      : "none";

  const selectedPrimitiveGroup = useMemo(() => {
    if (primitiveExplorer.groups.length === 0) {
      return null;
    }
    if (selectedPrimitiveId === null) {
      return primitiveExplorer.groups[0];
    }
    const match = primitiveExplorer.groups.find((row: PrimitiveExplorerGroup) => row.primitive_id === selectedPrimitiveId);
    return match || primitiveExplorer.groups[0];
  }, [primitiveExplorer.groups, selectedPrimitiveId]);

  const deckCardsDebugRows = useMemo(() => {
    if (!result) {
      return [] as Array<{ key: string; value: unknown }>;
    }

    return Object.keys(result)
      .filter((key: string) => key.startsWith("deck_cards_"))
      .sort((a: string, b: string) => a.localeCompare(b))
      .map((key: string) => ({
        key,
        value: result[key],
      }));
  }, [result]);

  const unknownCodes = useMemo(() => {
    return unknowns
      .map((entry: unknown) => {
        const row = asRecord(entry);
        return firstNonEmptyString(row?.code) || "";
      })
      .filter((code: string) => code !== "");
  }, [unknowns]);

  useEffect(() => {
    if (primitiveExplorer.groups.length === 0) {
      if (selectedPrimitiveId !== null) {
        setSelectedPrimitiveId(null);
      }
      return;
    }

    const hasSelected =
      selectedPrimitiveId !== null &&
      primitiveExplorer.groups.some((row: PrimitiveExplorerGroup) => row.primitive_id === selectedPrimitiveId);

    if (!hasSelected) {
      setSelectedPrimitiveId(primitiveExplorer.groups[0].primitive_id);
    }
  }, [primitiveExplorer.groups, selectedPrimitiveId]);

  const selectedPreviewRow = hoverVisible ? hoverSuggestion : null;
  const selectedPreviewOracleId = firstNonEmptyString(selectedPreviewRow?.oracle_id) || "";
  const selectedPreviewImageUrl =
    selectedPreviewOracleId !== "" ? buildLocalCardImageUrl(apiBase, selectedPreviewOracleId, "normal") : "";
  const selectedPreviewImageFailed = selectedPreviewImageUrl !== "" && Boolean(previewImageFailures[selectedPreviewImageUrl]);
  const selectedPreviewCanRenderImage = selectedPreviewImageUrl !== "" && !selectedPreviewImageFailed;
  const selectedPreviewPrimitiveTags =
    hoverVisible && hoverPreviewSource === "primitive" ? hoverPrimitiveTags : [];

  return (
    <div className="phase1-shell">
      <aside className="phase1-rail" aria-label="Harness navigation">
        <a className="phase1-rail-link" href="#phase1-control">
          <span className="phase1-rail-icon">C</span>
          <span className="phase1-rail-text">Control</span>
        </a>
        <a className="phase1-rail-link" href="#phase1-build-runner">
          <span className="phase1-rail-icon">B</span>
          <span className="phase1-rail-text">Build Runner</span>
        </a>
        <a className="phase1-rail-link" href="#phase1-search">
          <span className="phase1-rail-icon">S</span>
          <span className="phase1-rail-text">Card Search</span>
        </a>
        <a className="phase1-rail-link" href="#phase1-primitive">
          <span className="phase1-rail-icon">P</span>
          <span className="phase1-rail-text">Primitive Explorer</span>
        </a>
        <a className="phase1-rail-link" href="#phase1-v2">
          <span className="phase1-rail-icon">V2</span>
          <span className="phase1-rail-text">V2 Panels</span>
        </a>
        <a className="phase1-rail-link" href="#phase1-canonical">
          <span className="phase1-rail-icon">D</span>
          <span className="phase1-rail-text">Debug Slots</span>
        </a>
        <a className="phase1-rail-link" href="#phase1-raw">
          <span className="phase1-rail-icon">JSON</span>
          <span className="phase1-rail-text">Raw</span>
        </a>
      </aside>

      <main className="phase1-main">
        <header className="phase1-header">
          <p className="phase1-kicker">MTG Engine UI Harness - Phase 1</p>
          <h1>Desktop Harness / UI_CONTRACT_v1</h1>
          <p className="phase1-subtitle">
            Deterministic panel renderer with file/API mode and local card suggest lookup.
          </p>
        </header>

        <section className="phase1-panel" id="phase1-control">
          <h2>Load mode + build source</h2>

          <div className="phase1-control-grid">
            <label>
              Data mode
              <select
                value={mode}
                onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                  setMode(event.target.value === "api" ? "api" : "file");
                }}
              >
                <option value="file">File mode</option>
                <option value="api">API mode</option>
              </select>
            </label>

            <label>
              API base URL
              <input
                value={apiBase}
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  setApiBase(event.target.value);
                }}
                placeholder="http://127.0.0.1:8000"
              />
            </label>

            {mode === "file" ? (
              <div className="phase1-file-mode-box">
                <p>
                  Fixture source: <code>{FIXTURE_PATH_LABEL}</code>
                </p>
                <button type="button" onClick={loadFixturePayload}>
                  Reload fixture JSON
                </button>
              </div>
            ) : (
              <div className="phase1-file-mode-box">
                <p>API mode enabled. Use the Deck input + build runner panel below.</p>
              </div>
            )}
          </div>

          <div className="phase1-source-row">
            <span className="phase1-chip">Source: {payloadSource}</span>
            {runtimeError ? <span className="phase1-chip phase1-chip-error">{runtimeError}</span> : null}
          </div>
        </section>

        <section className="phase1-panel" id="phase1-build-runner">
          <h2>Deck input + build runner</h2>

          <div className="phase1-build-form-grid">
            <label>
              Commander
              <input
                value={commander}
                list="phase1-commander-suggest"
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  setCommander(event.target.value);
                }}
                placeholder="Krenko, Mob Boss"
              />
              <datalist id="phase1-commander-suggest">
                {commanderSuggestRows.map((row: CardSuggestRow, index: number) => (
                  <option key={`${row.oracle_id}-${row.name}-${index}`} value={row.name} />
                ))}
              </datalist>
            </label>

            <label>
              Snapshot ID
              <input
                value={dbSnapshotId}
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  setDbSnapshotId(event.target.value);
                }}
                placeholder={mode === "file" ? FIXTURE_DEFAULT_SNAPSHOT_ID : "latest snapshot (if blank)"}
              />
            </label>

            <label>
              Profile ID
              <input
                value={profileId}
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  setProfileId(event.target.value);
                }}
                placeholder={DEFAULT_PROFILE_ID}
              />
            </label>

            <label>
              Bracket ID
              <input
                value={bracketId}
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  setBracketId(event.target.value);
                }}
                placeholder={DEFAULT_BRACKET_ID}
              />
            </label>

            <label className="phase1-span-2">
              Decklist
              <textarea
                rows={9}
                value={cardsInput}
                onChange={(event: ChangeEvent<HTMLTextAreaElement>) => {
                  setCardsInput(event.target.value);
                }}
                placeholder={[
                  "1 Sol Ring",
                  "1x Arcane Signet",
                  "Mountain",
                  "# comments are ignored",
                  "// comments are ignored",
                ].join("\n")}
              />
            </label>

            <div className="phase1-build-actions phase1-span-2">
              <button type="button" onClick={runBuildApi} disabled={loadingBuild || mode !== "api"}>
                {loadingBuild ? (
                  <span className="phase1-button-inline">
                    <span className="phase1-spinner" aria-hidden="true" />
                    Running /build...
                  </span>
                ) : (
                  "Run build"
                )}
              </button>
            </div>
          </div>

          <div className="phase1-source-row">
            <span className="phase1-chip">Parsed cards (payload order): {deckCardsInPayloadOrder.length}</span>
            <span className="phase1-chip">Normalized preview lines: {normalizedDeckPreviewLines.length}</span>
            {mode !== "api" ? <span className="phase1-chip">Switch to API mode to run /build.</span> : null}
            <span className="phase1-chip">
              commander autocomplete: {commanderSuggestLoading ? "loading..." : `${commanderSuggestRows.length} result(s)`}
            </span>
          </div>

          {commanderSuggestError ? (
            <div className="phase1-search-error-banner" role="status">
              <strong>Commander autocomplete error</strong>
              <code>{commanderSuggestError}</code>
            </div>
          ) : null}

          {runtimeError ? (
            <div className="phase1-search-error-banner" role="status">
              <strong>Build request failed</strong>
              <code>{runtimeError}</code>
            </div>
          ) : null}

          <div className="phase1-deck-preview-block">
            <h3>Normalized preview (deterministic sort)</h3>
            {normalizedDeckPreviewLines.length > 0 ? (
              <pre>{normalizedDeckPreviewLines.join("\n")}</pre>
            ) : (
              <div className="phase1-preview-placeholder">No deck cards parsed yet.</div>
            )}
          </div>
        </section>

        <section className="phase1-panel" id="phase1-search">
          <h2>Card search (local, deterministic, offline-safe)</h2>

          <div className="phase1-search-grid" onMouseLeave={hideHoverPreview}>
            <div className="phase1-search-box">
              <label>
                Card lookup
                <input
                  value={searchQuery}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    setSearchQuery(event.target.value);
                    setSearchOpen(true);
                  }}
                  onFocus={() => {
                    if (searchResults.length > 0) {
                      setSearchOpen(true);
                    }
                  }}
                  onKeyDown={handleSearchKeyDown}
                  placeholder="Type 2+ chars (debounce 60ms, max 20)"
                />
              </label>

              <div className="phase1-search-meta">
                <span>{searchLoading ? "Searching..." : `Results: ${searchResults.length}`}</span>
                {searchError ? <span className="phase1-search-error">{searchError}</span> : null}
              </div>

              {searchError ? (
                <div className="phase1-search-error-banner" role="status">
                  <strong>Failed to fetch /cards/suggest</strong>
                  <code>{searchError}</code>
                </div>
              ) : null}

              {searchOpen && searchResults.length > 0 ? (
                <ul className="phase1-suggest-list" role="listbox" aria-label="Card suggestions">
                  {searchResults.map((row: CardSuggestRow, index: number) => (
                    <li
                      key={`${row.oracle_id}-${row.name}-${index}`}
                      className={index === activeSuggestionIndex ? "is-active" : ""}
                      onMouseEnter={() => {
                        setActiveSuggestionIndex(index);
                        handleSuggestionMouseEnter(row);
                      }}
                      onMouseLeave={handleSuggestionMouseLeave}
                      onMouseDown={(event: MouseEvent<HTMLLIElement>) => {
                        event.preventDefault();
                        selectSuggestion(row);
                      }}
                    >
                      <div className="phase1-suggest-name">{row.name}</div>
                      <div className="phase1-suggest-meta-row">
                        <span>{row.mana_cost || "-"}</span>
                        <span>{row.type_line || "-"}</span>
                      </div>
                    </li>
                  ))}
                </ul>
              ) : null}

              <div className="phase1-search-selected">
                Selected:{" "}
                <strong>
                  {selectedSuggestion ? `${selectedSuggestion.name}${selectedSuggestion.mana_cost ? ` ${selectedSuggestion.mana_cost}` : ""}` : "-"}
                </strong>
              </div>
            </div>

            <div className={`phase1-card-preview ${selectedPreviewRow ? "is-visible" : ""}`}>
              <h3>Hover card art preview</h3>
              {selectedPreviewRow ? (
                <>
                  <p className="phase1-preview-title">{selectedPreviewRow.name}</p>
                  <p className="phase1-preview-subtitle">{selectedPreviewRow.type_line || "Type unavailable"}</p>
                  {selectedPreviewCanRenderImage ? (
                    <img
                      src={selectedPreviewImageUrl}
                      alt={selectedPreviewRow.name}
                      onError={() => {
                        markPreviewImageFailure(selectedPreviewImageUrl);
                      }}
                    />
                  ) : (
                    <div className="phase1-preview-placeholder">
                      {selectedPreviewOracleId === ""
                        ? "No oracle_id available for this card in current payload."
                        : "Not cached in local image cache."}
                    </div>
                  )}
                </>
              ) : (
                <div className="phase1-preview-placeholder">Hover a suggestion for 150ms to preview.</div>
              )}
            </div>
          </div>
        </section>

        {buildPayload ? (
          <>
            <section className="phase1-panel" id="phase1-primitive">
              <h2>Primitive Explorer (scaffold)</h2>

              <div className="phase1-source-row">
                <span className="phase1-chip">source: {primitiveExplorerSourceLabel}</span>
                <span className="phase1-chip">
                  unique_primitives_total: {firstNumber(primitiveIndexTotals?.unique_primitives_total) ?? primitiveExplorer.groups.length}
                </span>
                <span className="phase1-chip">
                  slots_with_primitives: {firstNumber(primitiveIndexTotals?.slots_with_primitives) ?? "-"}
                </span>
              </div>

              {!primitiveExplorerAvailable ? (
                <div className="phase1-empty-state">
                  Primitive index not present in this build result (requires runtime tag index + primitive index layer).
                </div>
              ) : primitiveExplorer.groups.length === 0 ? (
                <div className="phase1-empty-state">Primitive index is present, but no primitives were mapped in this payload.</div>
              ) : (
                <div className="phase1-primitive-grid" onMouseLeave={hideHoverPreview}>
                  <div className="phase1-primitive-column">
                    <h3>Primitive categories ({primitiveExplorer.groups.length})</h3>

                    <ul className="phase1-primitive-list">
                      {primitiveExplorer.groups.map((row: PrimitiveExplorerGroup) => {
                        const isActive = selectedPrimitiveGroup?.primitive_id === row.primitive_id;
                        return (
                          <li key={row.primitive_id}>
                            <button
                              type="button"
                              className={`phase1-primitive-button ${isActive ? "is-active" : ""}`}
                              onClick={() => setSelectedPrimitiveId(row.primitive_id)}
                            >
                              <span className="phase1-primitive-label">{row.primitive_id}</span>
                              <span className="phase1-primitive-count">{row.count}</span>
                            </button>
                          </li>
                        );
                      })}
                    </ul>
                  </div>

                  <div className="phase1-primitive-column">
                    <h3>
                      Cards mapped to {selectedPrimitiveGroup?.primitive_id || "-"} ({selectedPrimitiveGroup?.cards.length || 0})
                    </h3>

                    {selectedPrimitiveGroup && selectedPrimitiveGroup.cards.length > 0 ? (
                      <ul className="phase1-primitive-card-list">
                        {selectedPrimitiveGroup.cards.map((row: PrimitiveExplorerCardRow) => (
                          <li
                            key={`${selectedPrimitiveGroup.primitive_id}-${row.slot_id}`}
                            onMouseEnter={() => {
                              showHoverPreview(mapPrimitiveCardToSuggestRow(row), "primitive", row.primitive_tags);
                            }}
                            onMouseLeave={handleSuggestionMouseLeave}
                          >
                            <div className="phase1-suggest-name">{row.name}</div>
                            <div className="phase1-suggest-meta-row">
                              <span>{row.slot_id}</span>
                              <span>{row.type_line || "-"}</span>
                            </div>
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <div className="phase1-empty-state phase1-empty-state-compact">No cards mapped to selected primitive.</div>
                    )}

                    <div className={`phase1-card-preview ${selectedPreviewRow ? "is-visible" : ""}`}>
                      <h3>Primitive hover preview</h3>
                      {selectedPreviewRow ? (
                        <>
                          <p className="phase1-preview-title">{selectedPreviewRow.name}</p>
                          <p className="phase1-preview-subtitle">{selectedPreviewRow.type_line || "Type unavailable"}</p>
                          {selectedPreviewCanRenderImage ? (
                            <img
                              src={selectedPreviewImageUrl}
                              alt={selectedPreviewRow.name}
                              onError={() => {
                                markPreviewImageFailure(selectedPreviewImageUrl);
                              }}
                            />
                          ) : (
                            <div className="phase1-preview-placeholder">
                              {selectedPreviewOracleId === ""
                                ? "No oracle_id available for this card in current payload."
                                : "Not cached in local image cache."}
                            </div>
                          )}

                          {selectedPreviewPrimitiveTags.length > 0 ? (
                            <div className="phase1-primitive-tag-wrap">
                              {selectedPreviewPrimitiveTags.map((tag: string) => (
                                <span className="phase1-chip" key={tag}>
                                  {tag}
                                </span>
                              ))}
                            </div>
                          ) : (
                            <div className="phase1-footnote">Primitive tags not available for this card in current build payload.</div>
                          )}
                        </>
                      ) : (
                        <div className="phase1-preview-placeholder">
                          Hover a mapped card row for 150ms to preview and inspect primitive tags.
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </section>

            <section className="phase1-panel" id="phase1-overview">
              <h2>Header chips + analysis status bar</h2>

              <div className="phase1-chip-wrap">
                <span className="phase1-chip">engine_version: {firstNonEmptyString(buildPayload.engine_version) || "-"}</span>
                <span className="phase1-chip">ruleset_version: {firstNonEmptyString(buildPayload.ruleset_version) || "-"}</span>
                <span className="phase1-chip">db_snapshot_id: {firstNonEmptyString(buildPayload.db_snapshot_id) || "-"}</span>
                <span className="phase1-chip">profile_id: {firstNonEmptyString(buildPayload.profile_id) || "-"}</span>
                <span className="phase1-chip">bracket_id: {firstNonEmptyString(buildPayload.bracket_id) || "-"}</span>
                <span className="phase1-chip">build_hash_v1: {firstNonEmptyString(buildPayload.build_hash_v1) || "-"}</span>
              </div>

              <div className="phase1-status-grid">
                <div className="phase1-glass-card">
                  <h3>Structural</h3>
                  <p>{structuralStatus}</p>
                </div>
                <div className="phase1-glass-card">
                  <h3>Game Changers</h3>
                  <p>{gameChangerCount !== null ? gameChangerCount : "-"}</p>
                </div>
                <div className="phase1-glass-card">
                  <h3>Commander dependency (v1)</h3>
                  <p>{commanderDependencyV1}</p>
                </div>
                <div className="phase1-glass-card">
                  <h3>Engine coherence (v1)</h3>
                  <p>
                    {coherenceV1Status}
                    {coherenceV1OverlapScore !== null ? ` / overlap=${coherenceV1OverlapScore}` : ""}
                  </p>
                </div>
              </div>

              <div className="phase1-footnote">
                available_panels_v1 keys: {availablePanels ? Object.keys(availablePanels).sort().join(", ") : "-"}
              </div>

              <div className="phase1-footnote">
                unknown codes: {unknownCodes.length > 0 ? unknownCodes.join(", ") : "none"}
              </div>
            </section>

            <section className="phase1-panel" id="phase1-v2">
              <h2>V2 optional panels + graph bounds caps</h2>

              <div className="phase1-status-grid">
                <div className="phase1-glass-card">
                  <h3>commander_dependency_v2</h3>
                  <p>access_required: {v2CommanderAccessRequired || "-"}</p>
                  <p>amplifier: {v2CommanderAmplifier !== null ? v2CommanderAmplifier : "-"}</p>
                  <p>line_share_percent: {v2CommanderLineShare !== null ? v2CommanderLineShare : "-"}</p>
                </div>

                <div className="phase1-glass-card">
                  <h3>engine_coherence_v2</h3>
                  <p>score: {v2CoherenceScore !== null ? v2CoherenceScore : "-"}</p>
                  <pre>{toPrettyJson(v2CoherenceComponents || {})}</pre>
                </div>

                <div className="phase1-glass-card">
                  <h3>stress_transform_engine_v2</h3>
                  <p>policy_version: {stressPolicyVersion}</p>
                  <ol className="phase1-operator-list">
                    {stressOperators.map((row: JsonRecord, index: number) => {
                      const opName = firstNonEmptyString(row.op) || `op_${index + 1}`;
                      const byTurn = firstNumber(row.by_turn);
                      return <li key={`${opName}-${index}`}>{byTurn !== null ? `${opName} @ t${byTurn}` : opName}</li>;
                    })}
                    {stressOperators.length === 0 ? <li>-</li> : null}
                  </ol>
                </div>

                <div className="phase1-glass-card">
                  <h3>graph bounds + caps hit</h3>
                  <p>graph_bounds_policy_version: {firstNonEmptyString(pipelineVersions?.graph_bounds_policy_version) || "-"}</p>
                  <p>MAX_PRIMS_PER_SLOT: {firstNumber(graphBounds?.MAX_PRIMS_PER_SLOT) ?? "-"}</p>
                  <p>MAX_SLOTS_PER_PRIM: {firstNumber(graphBounds?.MAX_SLOTS_PER_PRIM) ?? "-"}</p>
                  <p>MAX_CARD_CARD_EDGES_TOTAL: {firstNumber(graphBounds?.MAX_CARD_CARD_EDGES_TOTAL) ?? "-"}</p>
                  <p>caps_hit.max_prims_per_slot: {yesNoUnknown(graphCapsHit?.max_prims_per_slot)}</p>
                  <p>caps_hit.max_slots_per_prim: {yesNoUnknown(graphCapsHit?.max_slots_per_prim)}</p>
                  <p>caps_hit.max_edges_total: {yesNoUnknown(graphCapsHit?.max_edges_total)}</p>
                </div>
              </div>
            </section>

            <section className="phase1-panel" id="phase1-canonical">
              <h2>Canonical/debug slots</h2>

              <div className="phase1-debug-grid">
                <article>
                  <h3>commander_canonical_slot</h3>
                  <pre>{toPrettyJson(commanderCanonicalSlot || {})}</pre>
                </article>

                <article>
                  <h3>deck_cards_* payloads</h3>
                  {deckCardsDebugRows.length === 0 ? <p>-</p> : null}
                  {deckCardsDebugRows.map((row: { key: string; value: unknown }) => (
                    <details key={row.key}>
                      <summary>{row.key}</summary>
                      <pre>{toPrettyJson(row.value)}</pre>
                    </details>
                  ))}
                </article>

                <article>
                  <h3>unknowns_by_slot</h3>
                  <pre>{toPrettyJson(unknownsBySlot || {})}</pre>
                </article>
              </div>
            </section>

            <section className="phase1-panel" id="phase1-raw">
              <h2>Raw payload</h2>
              <pre>{rawPayload}</pre>
            </section>
          </>
        ) : (
          <section className="phase1-panel">
            <p>No build payload loaded yet.</p>
          </section>
        )}
      </main>
    </div>
  );
}
