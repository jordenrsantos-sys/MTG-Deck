import BuildRunnerPanel from "./BuildRunnerPanel";
import CardSuggestInput from "./CardSuggestInput";
import type { CardSuggestRow, ParsedDecklistRow } from "./workspaceTypes";

type DeckInputPanelProps = {
  apiBase: string;
  snapshotId: string;
  profileId: string;
  bracketId: string;
  commander: string;
  cardsInput: string;
  parsedDeckRows: ParsedDecklistRow[];
  normalizedPreviewLines: string[];
  payloadCardCount: number;
  validating: boolean;
  runningBuild: boolean;
  validationMessage: string | null;
  onApiBaseChange: (value: string) => void;
  onSnapshotIdChange: (value: string) => void;
  onProfileIdChange: (value: string) => void;
  onBracketIdChange: (value: string) => void;
  onCommanderChange: (value: string) => void;
  onCardsInputChange: (value: string) => void;
  onCommanderSelect: (row: CardSuggestRow) => void;
  onCommanderHoverCard: (row: CardSuggestRow | null) => void;
  onValidate: () => void;
  onRunBuild: () => void;
};

export default function DeckInputPanel(props: DeckInputPanelProps) {
  const {
    apiBase,
    snapshotId,
    profileId,
    bracketId,
    commander,
    cardsInput,
    parsedDeckRows,
    normalizedPreviewLines,
    payloadCardCount,
    validating,
    runningBuild,
    validationMessage,
    onApiBaseChange,
    onSnapshotIdChange,
    onProfileIdChange,
    onBracketIdChange,
    onCommanderChange,
    onCardsInputChange,
    onCommanderSelect,
    onCommanderHoverCard,
    onValidate,
    onRunBuild,
  } = props;

  return (
    <section className="workspace-panel">
      <details open className="workspace-collapsible">
        <summary>Deck Input + Build Runner</summary>

        <div className="workspace-form-grid">
          <label className="workspace-field">
            <span>API base URL</span>
            <input
              value={apiBase}
              onChange={(event) => {
                onApiBaseChange(event.target.value);
              }}
              placeholder="http://127.0.0.1:8000"
            />
          </label>

          <label className="workspace-field">
            <span>Snapshot ID</span>
            <input
              value={snapshotId}
              onChange={(event) => {
                onSnapshotIdChange(event.target.value);
              }}
              placeholder="20260217_190902"
            />
          </label>

          <label className="workspace-field">
            <span>Profile ID</span>
            <input
              value={profileId}
              onChange={(event) => {
                onProfileIdChange(event.target.value);
              }}
              placeholder="focused"
            />
          </label>

          <label className="workspace-field">
            <span>Bracket ID</span>
            <input
              value={bracketId}
              onChange={(event) => {
                onBracketIdChange(event.target.value);
              }}
              placeholder="B2"
            />
          </label>

          <CardSuggestInput
            label="Commander"
            value={commander}
            placeholder="Krenko, Mob Boss"
            apiBase={apiBase}
            snapshotId={snapshotId}
            limit={12}
            onChange={onCommanderChange}
            onSelect={onCommanderSelect}
            onHoverCard={onCommanderHoverCard}
          />

          <label className="workspace-field workspace-span-2">
            <span>Decklist</span>
            <textarea
              rows={11}
              value={cardsInput}
              onChange={(event) => {
                onCardsInputChange(event.target.value);
              }}
              placeholder={[
                "1 Sol Ring",
                "1x Arcane Signet",
                "Goblin Matron",
                "# comment",
                "// comment",
              ].join("\n")}
            />
          </label>
        </div>

        <BuildRunnerPanel
          parsedRowsCount={parsedDeckRows.length}
          payloadCardsCount={payloadCardCount}
          validating={validating}
          runningBuild={runningBuild}
          validationMessage={validationMessage}
          onValidate={onValidate}
          onRunBuild={onRunBuild}
        />

        <div className="workspace-panel-split">
          <div>
            <h4>Parsed rows (input order)</h4>
            {parsedDeckRows.length === 0 ? (
              <p className="workspace-muted">No parsed rows yet.</p>
            ) : (
              <ol className="workspace-compact-list">
                {parsedDeckRows.map((row: ParsedDecklistRow) => (
                  <li key={`${row.source_order}-${row.name}`}>{`${row.count} ${row.name}`}</li>
                ))}
              </ol>
            )}
          </div>

          <div>
            <h4>Normalized preview</h4>
            {normalizedPreviewLines.length === 0 ? (
              <p className="workspace-muted">No cards in preview.</p>
            ) : (
              <pre className="workspace-json-block">{normalizedPreviewLines.join("\n")}</pre>
            )}
          </div>
        </div>
      </details>
    </section>
  );
}
