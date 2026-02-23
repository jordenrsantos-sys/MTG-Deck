import { useEffect, useMemo, useRef, useState } from "react";

import fixtureBuildResult from "../../fixtures/build_result.json";
import BuildHistoryPanel from "../components/BuildHistoryPanel";
import CanonicalSlotsPanel from "../components/CanonicalSlotsPanel";
import CardModal from "../components/CardModal";
import CardHoverPreview from "../components/CardHoverPreview";
import DeckInputPanel from "../components/DeckInputPanel";
import DeckPanel, { type DeckPanelCard, type DeckPanelCommander } from "../components/deck/DeckPanel";
import HeaderChips from "../components/HeaderChips";
import LeftRail from "../components/layout/LeftRail";
import PrimitiveExplorerPanel from "../components/PrimitiveExplorerPanel";
import StatusBar from "../components/StatusBar";
import UnknownsPatchesPanel from "../components/UnknownsPatchesPanel";
import GlassPanel from "../ui/primitives/GlassPanel";
import type {
  BuildHistoryEntry,
  BuildRequestPayload,
  BuildResponsePayload,
  CardSuggestRow,
  HoverCard,
} from "../components/workspaceTypes";
import {
  DEFAULT_API_BASE,
  asArray,
  asRecord,
  buildNormalizedDeckPreviewLines,
  expandDecklistRowsInInputOrder,
  fetchLatestSnapshotIdFromApi,
  firstNumber,
  firstNonEmptyString,
  normalizeApiBase,
  parseDecklistInput,
  resolveUnknownSignal,
  safeParseJson,
  toPrettyJson,
  toSingleLineSnippet,
} from "../components/workspaceUtils";

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

function resolveDeckPanelUnknownsCount(buildResponse: BuildResponsePayload | null): number | null {
  const unknownSignal = resolveUnknownSignal(buildResponse);
  if (!unknownSignal.hasUnknownsData) {
    return null;
  }

  return unknownSignal.totalCount;
}

function isExternalBackendWarningMode(searchValue: string): boolean {
  const params = new URLSearchParams(searchValue);
  return params.get("external_backend") === "1";
}

export default function WorkspaceView() {
  const showExternalBackendBanner = useMemo(() => isExternalBackendWarningMode(window.location.search), []);
  const [showExternalBackendHelp, setShowExternalBackendHelp] = useState(false);

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

  const [isCardModalOpen, setIsCardModalOpen] = useState(false);
  const [cardModalOracleId, setCardModalOracleId] = useState<string | null>(null);
  const [cardModalList, setCardModalList] = useState<string[]>([]);
  const [cardModalIndex, setCardModalIndex] = useState(0);

  const historyCounterRef = useRef(0);

  const parsedDeckRows = useMemo(() => parseDecklistInput(cardsInput), [cardsInput]);
  const deckCardsInPayloadOrder = useMemo(() => expandDecklistRowsInInputOrder(parsedDeckRows), [parsedDeckRows]);
  const normalizedPreviewLines = useMemo(
    () => buildNormalizedDeckPreviewLines(deckCardsInPayloadOrder),
    [deckCardsInPayloadOrder],
  );
  const deckPanelCommander = useMemo(() => resolveDeckPanelCommander(buildResponse), [buildResponse]);
  const deckPanelCards = useMemo(
    () => resolveDeckPanelCards(buildResponse, deckPanelCommander),
    [buildResponse, deckPanelCommander],
  );
  const deckPanelUnknownsCount = useMemo(() => resolveDeckPanelUnknownsCount(buildResponse), [buildResponse]);
  const deckPanelDeckSizeTotal = useMemo(() => firstNumber(buildResponse?.deck_size_total), [buildResponse]);
  const deckPanelCardsNeeded = useMemo(() => firstNumber(buildResponse?.cards_needed), [buildResponse]);
  const deckPanelDeckStatus = useMemo(
    () => firstNonEmptyString(buildResponse?.deck_status, asRecord(buildResponse?.result)?.deck_status),
    [buildResponse],
  );
  const deckPanelBuildStatus = useMemo(() => firstNonEmptyString(buildResponse?.status), [buildResponse]);

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
            <div className="workspace-topbar-grid">
              <HeaderChips buildResponse={buildResponse} compact className="workspace-topbar-block" />
              <StatusBar
                buildResponse={buildResponse}
                loading={runningBuild}
                runtimeError={runtimeError}
                compact
                className="workspace-topbar-block workspace-topbar-status"
              />
            </div>
          </GlassPanel>

          <div className="workspace-main-stack">
            <section id="workspace-decks" className="workspace-section-anchor">
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
            </section>

            <section id="workspace-runs" className="workspace-section-anchor">
              <BuildHistoryPanel
                entries={historyEntries}
                selectedEntryId={selectedHistoryEntryId}
                onSelectEntry={handleSelectHistoryEntry}
              />
            </section>

            <section id="workspace-deck-panel" className="workspace-section-anchor">
              <GlassPanel>
                <DeckPanel
                  deckCards={deckPanelCards}
                  commander={deckPanelCommander}
                  onOpenCard={openCardModal}
                  unknownsCount={deckPanelUnknownsCount}
                  deckSizeTotal={deckPanelDeckSizeTotal}
                  cardsNeeded={deckPanelCardsNeeded}
                  deckStatus={deckPanelDeckStatus}
                  buildStatus={deckPanelBuildStatus}
                  unknownsPanelId="workspace-unknowns-panel"
                />
              </GlassPanel>
            </section>

            <GlassPanel>
              <PrimitiveExplorerPanel
                buildResponse={buildResponse}
                onHoverCard={setHoverCard}
                onCardClick={openCardModal}
              />
            </GlassPanel>

            <CardHoverPreview
              apiBase={apiBase}
              snapshotId={snapshotId}
              card={hoverCard}
              failedImageUrls={previewImageFailures}
              onImageError={markPreviewImageFailure}
            />

            <GlassPanel>
              <CanonicalSlotsPanel buildResponse={buildResponse} />
            </GlassPanel>

            <section id="workspace-unknowns-panel" className="workspace-section-anchor">
              <GlassPanel>
                <UnknownsPatchesPanel buildResponse={buildResponse} requestPayload={requestPayload} />
              </GlassPanel>
            </section>

            <GlassPanel className="workspace-developer-data">
              <details className="workspace-collapsible">
                <summary>Developer Data</summary>
                <pre className="workspace-json-block">{toPrettyJson(buildResponse || {})}</pre>
              </details>
            </GlassPanel>
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
    </div>
  );
}
