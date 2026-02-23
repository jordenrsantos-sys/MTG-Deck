import Phase1Harness from "../Phase1Harness";

type DiagnosticsViewProps = {
  onBack: () => void;
};

export default function DiagnosticsView(props: DiagnosticsViewProps) {
  const { onBack } = props;

  return (
    <div className="workspace-diagnostics-shell">
      <div className="workspace-diagnostics-topbar">
        <button type="button" onClick={onBack}>
          ← Back to Workspace
        </button>
        <p className="workspace-muted">Diagnostics renders the original Phase 1 harness unchanged.</p>
      </div>
      <Phase1Harness />
    </div>
  );
}
