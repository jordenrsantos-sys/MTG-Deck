type BuildRunnerPanelProps = {
  parsedRowsCount: number;
  payloadCardsCount: number;
  validating: boolean;
  runningBuild: boolean;
  validationMessage: string | null;
  onValidate: () => void;
  onRunBuild: () => void;
};

export default function BuildRunnerPanel(props: BuildRunnerPanelProps) {
  const {
    parsedRowsCount,
    payloadCardsCount,
    validating,
    runningBuild,
    validationMessage,
    onValidate,
    onRunBuild,
  } = props;

  return (
    <div className="workspace-build-runner">
      <div className="workspace-action-row">
        <button type="button" onClick={onValidate} disabled={validating || runningBuild}>
          {validating ? "Validating..." : "Validate"}
        </button>
        <button type="button" onClick={onRunBuild} disabled={runningBuild}>
          {runningBuild ? "Running /build..." : "Run build"}
        </button>
      </div>

      <div className="workspace-chip-row">
        <span className="workspace-chip">Parsed lines: {parsedRowsCount}</span>
        <span className="workspace-chip">Payload cards: {payloadCardsCount}</span>
        {validationMessage ? <span className="workspace-chip workspace-chip-info">{validationMessage}</span> : null}
      </div>
    </div>
  );
}
