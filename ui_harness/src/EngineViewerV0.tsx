import { FormEvent, useMemo, useState } from "react";

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

type RunHistoryEntry = {
  run_key: string;
  build_hash_v1: string;
  request_hash_v1: string;
  db_snapshot_id: string;
  profile_id: string;
  bracket_id: string;
  status: string;
};

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

const DEFAULT_FORM = {
  db_snapshot_id: "20260217_190902",
  profile_id: "focused",
  bracket_id: "B2",
  commander_override: "",
  raw_decklist_text: "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet\n",
};

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

  const [validateResponse, setValidateResponse] = useState<ValidateResponsePayload | null>(null);
  const [buildResponse, setBuildResponse] = useState<BuildResponsePayload | null>(null);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [loadingValidate, setLoadingValidate] = useState(false);
  const [loadingBuild, setLoadingBuild] = useState(false);
  const [selectedPanelKey, setSelectedPanelKey] = useState<string | null>(null);
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [nameOverridesV1, setNameOverridesV1] = useState<NameOverrideV1[]>([]);

  const validateUnknowns = asUnknownRows(validateResponse?.unknowns);
  const hasUnknowns = validateUnknowns.length > 0;

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

  async function postJson(path: string, payload: Record<string, unknown>): Promise<unknown> {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    const text = await response.text();
    let parsed: unknown = null;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = null;
    }

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${text}`);
    }

    return parsed;
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

  async function executeValidate(nameOverrides: NameOverrideV1[] = nameOverridesV1): Promise<ValidateResponsePayload> {
    const payload = buildValidatePayload(nameOverrides);
    const parsed = await postJson("/deck/validate", payload);
    return (asRecord(parsed) ?? {}) as ValidateResponsePayload;
  }

  async function revalidateWithOverrides(nextOverrides: NameOverrideV1[]) {
    setLoadingValidate(true);
    setRuntimeError(null);
    setBuildResponse(null);

    try {
      const normalizedOverrides = normalizeNameOverrides(nextOverrides);
      setNameOverridesV1(normalizedOverrides);
      const response = await executeValidate(normalizedOverrides);
      setValidateResponse(response);
      setSelectedPanelKey(null);
    } catch (error) {
      setRuntimeError(error instanceof Error ? error.message : "Unknown runtime error");
      setValidateResponse(null);
    } finally {
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
    setRuntimeError(null);

    try {
      const nextValidate = await executeValidate(nameOverridesV1);
      setValidateResponse(nextValidate);

      const nextUnknowns = asUnknownRows(nextValidate.unknowns);
      if (nextUnknowns.length > 0 || nextValidate.status !== "OK") {
        setBuildResponse(null);
        setSelectedPanelKey(null);
        return;
      }

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
    } finally {
      setLoadingBuild(false);
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
            <button type="submit" disabled={loadingValidate || loadingBuild}>
              {loadingValidate ? "Validating..." : "Validate"}
            </button>
            <button
              type="button"
              onClick={() => void handleBuild()}
              disabled={loadingValidate || loadingBuild || hasUnknowns}
            >
              {loadingBuild ? "Building..." : "Build"}
            </button>
          </div>
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
