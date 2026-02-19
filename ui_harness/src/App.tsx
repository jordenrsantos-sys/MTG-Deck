import { FormEvent, useMemo, useState } from "react";

type BuildResult = {
  ui_contract_version?: string;
  available_panels_v1?: Record<string, boolean>;
  structural_snapshot_v1?: Record<string, unknown>;
  commander_canonical_slot?: unknown;
  deck_cards_canonical_input_order?: unknown[];
  canonical_slots_all?: unknown[];
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

type ValueChangeEvent = {
  target: {
    value: string;
  };
};

type TabId = "summary" | "canonical" | "structural" | "graph" | "raw";
const UI_CONTRACT_VERSION = "ui_contract_v1";

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

  const resultRecord = responseBody?.result;
  const structuralSnapshot = responseBody?.result?.structural_snapshot_v1;
  const commanderCanonicalSlot = responseBody?.result?.commander_canonical_slot;
  const deckCardsCanonicalInputOrderRaw = responseBody?.result?.deck_cards_canonical_input_order;
  const deckCardsCanonicalInputOrder = Array.isArray(deckCardsCanonicalInputOrderRaw)
    ? deckCardsCanonicalInputOrderRaw
    : null;
  const canonicalSlotsAllRaw = responseBody?.result?.canonical_slots_all;
  const canonicalSlotsAll = Array.isArray(canonicalSlotsAllRaw)
    ? canonicalSlotsAllRaw
    : null;
  const hasCommanderCanonicalSlot = hasOwnField(resultRecord, "commander_canonical_slot");
  const hasDeckCardsCanonicalInputOrder = hasOwnField(resultRecord, "deck_cards_canonical_input_order");
  const hasCanonicalSlotsAll = hasOwnField(resultRecord, "canonical_slots_all");
  const graphV1 = responseBody?.result?.graph_v1;
  const graphNodeDegrees = responseBody?.result?.graph_node_degrees;
  const graphComponents = responseBody?.result?.graph_components;
  const unknowns = Array.isArray(responseBody?.unknowns) ? responseBody.unknowns : [];
  const unknownMessages = unknowns.slice(0, 3).map(extractUnknownMessage);

  const availablePanels = useMemo(() => {
    const rawMap = responseBody?.result?.available_panels_v1;
    return isBooleanMap(rawMap) ? rawMap : {};
  }, [responseBody]);

  const oracleTextLayerSkipped = unknowns.some((item: unknown) => {
    const rec = asRecord(item);
    return rec ? rec.code === "LAYER_SKIPPED_ORACLE_TEXT_REQUIRED" : false;
  });
  const oracleTextBadge =
    availablePanels.has_proof_attempts === false && oracleTextLayerSkipped
      ? "Oracle Text: FORBIDDEN"
      : responseBody
        ? "Oracle Text: (unknown)"
        : null;

  const summaryTabEnabled = contractMatched !== false;
  const canonicalTabVisible =
    availablePanels.has_deck_cards_summary === true ||
    hasCommanderCanonicalSlot ||
    hasDeckCardsCanonicalInputOrder ||
    hasCanonicalSlotsAll;
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
      return slotId.includes(needle) || input.includes(needle) || resolvedName.includes(needle);
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

    try {
      const response = await fetch(`${API_BASE_URL}/build`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestPayload),
      });
      const raw = await response.text();
      setRawResponse(raw);
      setHttpStatus(response.status);
      const parsed = safeJsonParse(raw);
      setResponseBody(parsed);
      const isContractMatch = parsed?.result?.ui_contract_version === UI_CONTRACT_VERSION;
      setContractMatched(isContractMatch);
      setActiveTab(isContractMatch ? "summary" : "raw");
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
          <button type="submit" disabled={loading}>
            {loading ? "Running..." : "Run /build"}
          </button>
        </form>
        {runtimeError ? <div className="error">runtime_error: {runtimeError}</div> : null}
      </section>

      <section className="panel">
        <h2>Inspector Tabs</h2>
        <div className="tabs">
          <button
            onClick={() => setActiveTab("summary")}
            className={activeTab === "summary" ? "active" : ""}
            disabled={!summaryTabEnabled}
          >
            Summary
          </button>
          {canonicalTabVisible ? (
            <button
              onClick={() => setActiveTab("canonical")}
              className={activeTab === "canonical" ? "active" : ""}
              disabled={!canonicalTabEnabled}
            >
              Canonical
            </button>
          ) : null}
          {structuralTabVisible ? (
            <button
              onClick={() => setActiveTab("structural")}
              className={activeTab === "structural" ? "active" : ""}
              disabled={!structuralTabEnabled}
            >
              Structural
            </button>
          ) : null}
          {graphTabVisible ? (
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
            {hasCommanderCanonicalSlot ? (
              <div>
                <div className="section-title">commander_canonical_slot</div>
                <pre>{JSON.stringify(commanderCanonicalSlot, null, 2)}</pre>
              </div>
            ) : null}

            {hasDeckCardsCanonicalInputOrder ? (
              <div>
                <div className="section-title">deck_cards_canonical_input_order</div>
                {deckCardsCanonicalInputOrder ? (
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {deckCardsCanonicalInputOrder.map((entry: unknown, idx: number) => (
                        <tr key={`deck-order-${idx}`}>
                          <td>{idx}</td>
                          <td>{asString(entry)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <pre>{JSON.stringify(deckCardsCanonicalInputOrderRaw ?? null, null, 2)}</pre>
                )}
              </div>
            ) : null}

            {hasCanonicalSlotsAll ? (
              <div>
                <div className="section-title">canonical_slots_all</div>
                {canonicalSlotsAll ? (
                  <>
                    <label className="filter-label">
                      Filter (slot_id / input / resolved_name)
                      <input
                        value={canonicalFilter}
                        onChange={(e: ValueChangeEvent) => setCanonicalFilter(e.target.value)}
                        placeholder="Type to filter"
                      />
                    </label>
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

      <section className="panel">
        <h2>Last Request Payload</h2>
        <pre>{JSON.stringify(lastRequestPayload ?? null, null, 2)}</pre>
      </section>
    </div>
  );
}

export default App;
