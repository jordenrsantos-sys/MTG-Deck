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

const DEFAULT_API_BASE = String(import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000").trim();
const SUGGEST_LIMIT_MAX = 20;
const FIXTURE_PATH_LABEL = "./ui_harness/fixtures/build_result.json";

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

function normalizeCardsInput(rawCards: string): string[] {
  return rawCards
    .split(/\r?\n|,/)
    .map((value: string) => value.trim())
    .filter((value: string) => value !== "");
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

function isLocalImageUri(value: string | null): boolean {
  if (value === null) {
    return false;
  }
  const token = value.trim();
  if (token === "") {
    return false;
  }

  const lower = token.toLowerCase();
  if (lower.startsWith("http://") || lower.startsWith("https://")) {
    return false;
  }

  return (
    lower.startsWith("/") ||
    lower.startsWith("./") ||
    lower.startsWith("../") ||
    lower.startsWith("file://") ||
    lower.startsWith("data:image/") ||
    /^[a-z]:\\/.test(lower)
  );
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
  const [dbSnapshotId, setDbSnapshotId] = useState("20260217_190902");
  const [profileId, setProfileId] = useState("focused");
  const [bracketId, setBracketId] = useState("B2");
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
  const [hoverSuggestion, setHoverSuggestion] = useState<CardSuggestRow | null>(null);
  const [hoverVisible, setHoverVisible] = useState(false);

  const hoverDelayTimerRef = useRef<number | null>(null);
  const suggestRequestIdRef = useRef(0);

  function clearHoverTimer() {
    if (hoverDelayTimerRef.current !== null) {
      window.clearTimeout(hoverDelayTimerRef.current);
      hoverDelayTimerRef.current = null;
    }
  }

  function loadFixturePayload() {
    const payload = fixtureBuildResult as BuildResponsePayload;
    setBuildPayload(payload);
    setRawPayload(toPrettyJson(payload));
    setPayloadSource(`File mode (${FIXTURE_PATH_LABEL})`);
    setRuntimeError(null);
  }

  async function runBuildApi() {
    const requestBody = {
      db_snapshot_id: dbSnapshotId.trim(),
      profile_id: profileId.trim(),
      bracket_id: bracketId.trim(),
      format: "commander",
      commander: commander.trim(),
      cards: normalizeCardsInput(cardsInput),
      engine_patches_v0: [],
    };

    setLoadingBuild(true);
    setRuntimeError(null);

    try {
      const base = normalizeApiBase(apiBase);
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
        throw new Error(`HTTP ${response.status} from /build`);
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
      const message = error instanceof Error ? error.message : "Unknown /build runtime error";
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
    clearHoverTimer();
    hoverDelayTimerRef.current = window.setTimeout(() => {
      setHoverSuggestion(row);
      setHoverVisible(true);
      hoverDelayTimerRef.current = null;
    }, 150);
  }

  function handleSuggestionMouseLeave() {
    clearHoverTimer();
  }

  useEffect(() => {
    if (mode === "file") {
      loadFixturePayload();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode]);

  useEffect(() => {
    return () => {
      clearHoverTimer();
    };
  }, []);

  useEffect(() => {
    for (const row of searchResults) {
      if (!isLocalImageUri(row.image_uri)) {
        continue;
      }
      const image = new Image();
      image.src = row.image_uri as string;
    }
  }, [searchResults]);

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

  const selectedPreviewRow = hoverVisible ? hoverSuggestion : null;
  const selectedPreviewHasLocalArt = isLocalImageUri(selectedPreviewRow?.image_uri || null);

  return (
    <div className="phase1-shell">
      <aside className="phase1-rail" aria-label="Harness navigation">
        <a className="phase1-rail-link" href="#phase1-control">
          <span className="phase1-rail-icon">C</span>
          <span className="phase1-rail-text">Control</span>
        </a>
        <a className="phase1-rail-link" href="#phase1-search">
          <span className="phase1-rail-icon">S</span>
          <span className="phase1-rail-text">Card Search</span>
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
              <div className="phase1-build-form-grid">
                <label>
                  Snapshot ID
                  <input
                    value={dbSnapshotId}
                    onChange={(event: ChangeEvent<HTMLInputElement>) => {
                      setDbSnapshotId(event.target.value);
                    }}
                  />
                </label>
                <label>
                  Profile ID
                  <input
                    value={profileId}
                    onChange={(event: ChangeEvent<HTMLInputElement>) => {
                      setProfileId(event.target.value);
                    }}
                  />
                </label>
                <label>
                  Bracket ID
                  <input
                    value={bracketId}
                    onChange={(event: ChangeEvent<HTMLInputElement>) => {
                      setBracketId(event.target.value);
                    }}
                  />
                </label>
                <label>
                  Commander
                  <input
                    value={commander}
                    onChange={(event: ChangeEvent<HTMLInputElement>) => {
                      setCommander(event.target.value);
                    }}
                  />
                </label>
                <label className="phase1-span-2">
                  Cards (one per line or comma-separated)
                  <textarea
                    rows={4}
                    value={cardsInput}
                    onChange={(event: ChangeEvent<HTMLTextAreaElement>) => {
                      setCardsInput(event.target.value);
                    }}
                    placeholder="Card A\nCard B\nCard C"
                  />
                </label>
                <div className="phase1-build-actions">
                  <button type="button" onClick={runBuildApi} disabled={loadingBuild}>
                    {loadingBuild ? "Running /build..." : "Run /build"}
                  </button>
                </div>
              </div>
            )}
          </div>

          <div className="phase1-source-row">
            <span className="phase1-chip">Source: {payloadSource}</span>
            {runtimeError ? <span className="phase1-chip phase1-chip-error">{runtimeError}</span> : null}
          </div>
        </section>

        <section className="phase1-panel" id="phase1-search">
          <h2>Card search (local, deterministic, offline-safe)</h2>

          <div className="phase1-search-grid" onMouseLeave={() => setHoverVisible(false)}>
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
                  {selectedPreviewHasLocalArt ? (
                    <img src={selectedPreviewRow.image_uri || undefined} alt={selectedPreviewRow.name} />
                  ) : (
                    <div className="phase1-preview-placeholder">
                      No local image URI available in snapshot metadata.
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
