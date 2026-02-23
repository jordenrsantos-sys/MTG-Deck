import { useEffect, useMemo, useRef, useState } from "react";

import fixtureBuildResult from "../../fixtures/build_result.json";
import BuildHistoryPanel from "../components/BuildHistoryPanel";
import CanonicalSlotsPanel from "../components/CanonicalSlotsPanel";
import CardHoverPreview from "../components/CardHoverPreview";
import DeckInputPanel from "../components/DeckInputPanel";
import HeaderChips from "../components/HeaderChips";
import PrimitiveExplorerPanel from "../components/PrimitiveExplorerPanel";
import StatusBar from "../components/StatusBar";
import UnknownsPatchesPanel from "../components/UnknownsPatchesPanel";
import type {
  BuildHistoryEntry,
  BuildRequestPayload,
  BuildResponsePayload,
  CardSuggestRow,
  HoverCard,
} from "../components/workspaceTypes";
import {
  DEFAULT_API_BASE,
  asRecord,
  buildNormalizedDeckPreviewLines,
  expandDecklistRowsInInputOrder,
  fetchLatestSnapshotIdFromApi,
  firstNonEmptyString,
  normalizeApiBase,
  parseDecklistInput,
  safeParseJson,
  toPrettyJson,
  toSingleLineSnippet,
} from "../components/workspaceUtils";

type WorkspaceViewProps = {
  onOpenDiagnostics: () => void;
};

const fixtureRoot = asRecord(fixtureBuildResult);
const defaultSnapshotId = firstNonEmptyString(fixtureRoot?.db_snapshot_id) || "";
const defaultProfileId = firstNonEmptyString(fixtureRoot?.profile_id) || "focused";
const defaultBracketId = firstNonEmptyString(fixtureRoot?.bracket_id) || "B2";
const defaultCommander = firstNonEmptyString(getBuildResultCommander(fixtureRoot)) || "Krenko, Mob Boss";

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

export default function WorkspaceView(props: WorkspaceViewProps) {
  const { onOpenDiagnostics } = props;

  const [apiBase, setApiBase] = useState(DEFAULT_API_BASE);
  const [snapshotId, setSnapshotId] = useState(defaultSnapshotId);
  const [profileId, setProfileId] = useState(defaultProfileId);
  const [bracketId, setBracketId] = useState(defaultBracketId);
  const [commander, setCommander] = useState(defaultCommander);
  const [cardsInput, setCardsInput] = useState<string>(
    ["1 Sol Ring", "1 Arcane Signet", "Goblin Matron", "Skirk Prospector", "Impact Tremors"].join("\n"),
  );

  const [validationMessage, setValidationMessage] = useState<string | null>(null);
  const [validating, setValidating] = useState(false);
  const [runningBuild, setRunningBuild] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);

  const [buildResponse, setBuildResponse] = useState<BuildResponsePayload | null>(null);
  const [requestPayload, setRequestPayload] = useState<BuildRequestPayload | null>(null);

  const [historyEntries, setHistoryEntries] = useState<BuildHistoryEntry[]>([]);
  const [selectedHistoryEntryId, setSelectedHistoryEntryId] = useState<string | null>(null);

  const [hoverCard, setHoverCard] = useState<HoverCard | null>(null);
  const [previewImageFailures, setPreviewImageFailures] = useState<Record<string, true>>({});

  const historyCounterRef = useRef(0);

  const parsedDeckRows = useMemo(() => parseDecklistInput(cardsInput), [cardsInput]);
  const deckCardsInPayloadOrder = useMemo(() => expandDecklistRowsInInputOrder(parsedDeckRows), [parsedDeckRows]);
  const normalizedPreviewLines = useMemo(
    () => buildNormalizedDeckPreviewLines(deckCardsInPayloadOrder),
    [deckCardsInPayloadOrder],
  );

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

  function handleCommanderHoverCard(row: CardSuggestRow | null) {
    if (!row) {
      setHoverCard(null);
      return;
    }

    setHoverCard({
      name: row.name,
      oracle_id: row.oracle_id,
      type_line: row.type_line,
      primitive_tags: [],
      source: "suggest",
    });
  }

  function handleCommanderSelect(row: CardSuggestRow) {
    setCommander(row.name);
  }

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

  async function handleValidate(): Promise<void> {
    setValidating(true);
    setRuntimeError(null);

    try {
      const localValidation = runLocalValidate();
      setValidationMessage(localValidation.message);
      if (!localValidation.ok) {
        setRuntimeError(localValidation.message);
      }
    } finally {
      setValidating(false);
    }
  }

  async function handleRunBuild(): Promise<void> {
    setRunningBuild(true);
    setRuntimeError(null);

    try {
      const localValidation = runLocalValidate();
      setValidationMessage(localValidation.message);
      if (!localValidation.ok) {
        setRuntimeError(localValidation.message);
        return;
      }

      const base = normalizeApiBase(apiBase);
      let resolvedSnapshotId = snapshotId.trim();
      if (resolvedSnapshotId === "") {
        resolvedSnapshotId = await fetchLatestSnapshotIdFromApi(base);
        setSnapshotId(resolvedSnapshotId);
      }

      const payload: BuildRequestPayload = {
        db_snapshot_id: resolvedSnapshotId,
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
        body: JSON.stringify(payload),
      });

      const text = await response.text();
      const parsed = safeParseJson(text);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} from /build | response=${toSingleLineSnippet(text) || "(empty)"}`);
      }

      const root = asRecord(parsed);
      if (!root) {
        throw new Error("Build response was not a JSON object.");
      }

      const nextBuildResponse = root as BuildResponsePayload;
      setBuildResponse(nextBuildResponse);
      setRequestPayload(payload);

      const now = new Date();
      historyCounterRef.current += 1;
      const id = `${now.getTime()}-${historyCounterRef.current}`;
      const status = firstNonEmptyString(nextBuildResponse.status) || "UNKNOWN";
      const deckName = firstNonEmptyString(commander, getBuildResultCommander(root)) || "Untitled deck";

      const nextEntry: BuildHistoryEntry = {
        id,
        timestamp_iso: now.toISOString(),
        timestamp_label: buildTimestampLabel(now),
        deck_name: deckName,
        commander_input: commander,
        db_snapshot_id: resolvedSnapshotId,
        profile_id: payload.profile_id,
        bracket_id: payload.bracket_id,
        status,
        request_payload: payload,
        response_body: nextBuildResponse,
      };

      setHistoryEntries((previous: BuildHistoryEntry[]) => [nextEntry, ...previous].slice(0, 20));
      setSelectedHistoryEntryId(id);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown /build runtime error";
      setRuntimeError(message);
    } finally {
      setRunningBuild(false);
    }
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
    setCardsInput(buildCardsInputFromPayloadCards(entry.request_payload.cards));
    setValidationMessage(`Loaded build from history @ ${entry.timestamp_label}.`);
    setRuntimeError(null);
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

  return (
    <div className="workspace-shell">
      <header className="workspace-header">
        <p className="workspace-kicker">MTG Engine Harness · Phase 2</p>
        <h1>Active Deck Workspace</h1>
        <p className="workspace-subtitle">Local-first deck input → build → analysis loop with deterministic rendering.</p>
      </header>

      <div className="workspace-main-grid">
        <aside className="workspace-left-column">
          <DeckInputPanel
            apiBase={apiBase}
            snapshotId={snapshotId}
            profileId={profileId}
            bracketId={bracketId}
            commander={commander}
            cardsInput={cardsInput}
            parsedDeckRows={parsedDeckRows}
            normalizedPreviewLines={normalizedPreviewLines}
            payloadCardCount={deckCardsInPayloadOrder.length}
            validating={validating}
            runningBuild={runningBuild}
            validationMessage={validationMessage}
            onApiBaseChange={setApiBase}
            onSnapshotIdChange={setSnapshotId}
            onProfileIdChange={setProfileId}
            onBracketIdChange={setBracketId}
            onCommanderChange={setCommander}
            onCardsInputChange={setCardsInput}
            onCommanderSelect={handleCommanderSelect}
            onCommanderHoverCard={handleCommanderHoverCard}
            onValidate={() => void handleValidate()}
            onRunBuild={() => void handleRunBuild()}
          />

          <BuildHistoryPanel
            entries={historyEntries}
            selectedEntryId={selectedHistoryEntryId}
            onSelectEntry={handleSelectHistoryEntry}
          />
        </aside>

        <section className="workspace-right-column">
          <section className="workspace-panel workspace-top-actions">
            <button type="button" onClick={onOpenDiagnostics}>
              Open Diagnostics (Phase 1 Harness)
            </button>
            <span className="workspace-muted">Current payload source: /build API response</span>
          </section>

          <HeaderChips buildResponse={buildResponse} />
          <StatusBar buildResponse={buildResponse} loading={runningBuild} runtimeError={runtimeError} />
          <PrimitiveExplorerPanel buildResponse={buildResponse} onHoverCard={setHoverCard} />
          <CardHoverPreview
            apiBase={apiBase}
            snapshotId={snapshotId}
            card={hoverCard}
            failedImageUrls={previewImageFailures}
            onImageError={markPreviewImageFailure}
          />
          <CanonicalSlotsPanel buildResponse={buildResponse} />
          <UnknownsPatchesPanel buildResponse={buildResponse} requestPayload={requestPayload} />

          <section className="workspace-panel">
            <details className="workspace-collapsible">
              <summary>Raw Build JSON</summary>
              <pre className="workspace-json-block">{toPrettyJson(buildResponse || {})}</pre>
            </details>
          </section>
        </section>
      </div>
    </div>
  );
}
