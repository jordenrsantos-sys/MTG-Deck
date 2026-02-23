import type { BuildResponsePayload } from "./workspaceTypes";
import { asArray, asRecord, firstNonEmptyString, firstNumber, getPath } from "./workspaceUtils";

type StatusBarProps = {
  buildResponse: BuildResponsePayload | null;
  loading: boolean;
  runtimeError: string | null;
  compact?: boolean;
  className?: string;
};

export default function StatusBar(props: StatusBarProps) {
  const { buildResponse, loading, runtimeError, compact = false, className } = props;
  const result = asRecord(buildResponse?.result);
  const unknowns = asArray(buildResponse?.unknowns);
  const classes = ["workspace-panel-content", className].filter(Boolean).join(" ");

  const status = firstNonEmptyString(buildResponse?.status) || "-";
  const deckStatus = firstNonEmptyString(buildResponse?.deck_status) || "-";
  const deckSize = firstNumber(buildResponse?.deck_size_total);
  const cardsNeeded = firstNumber(buildResponse?.cards_needed);
  const cardsToCut = firstNumber(buildResponse?.cards_to_cut);
  const pipelineStage = firstNonEmptyString(result?.build_pipeline_stage) || "-";
  const unknownCanonicalCount = firstNumber(result?.unknowns_canonical_total, getPath(result, ["unknowns_canonical", "length"]));

  if (compact) {
    return (
      <section className={classes}>
        <p className="workspace-topbar-title">Status</p>
        <div className="workspace-chip-row">
          <span className="workspace-chip workspace-chip-info">build: {loading ? "RUNNING" : status}</span>
          <span className="workspace-chip">deck: {deckStatus}</span>
          <span className="workspace-chip">size: {deckSize ?? "-"}</span>
          <span className="workspace-chip">unknowns: {unknowns.length}</span>
          <span className="workspace-chip">canonical unknowns: {unknownCanonicalCount ?? "-"}</span>
          <span className="workspace-chip">
            needed/cut: {cardsNeeded ?? "-"}/{cardsToCut ?? "-"}
          </span>
          <span className="workspace-chip">pipeline: {pipelineStage}</span>
        </div>
        {runtimeError ? <p className="workspace-error-banner">{runtimeError}</p> : null}
      </section>
    );
  }

  return (
    <section className={classes}>
      <details open className="workspace-collapsible">
        <summary>Status Bar</summary>

        <div className="workspace-status-grid">
          <div className="workspace-glass-tile">
            <h5>Build status</h5>
            <p>{loading ? "RUNNING" : status}</p>
          </div>
          <div className="workspace-glass-tile">
            <h5>Deck status</h5>
            <p>{deckStatus}</p>
          </div>
          <div className="workspace-glass-tile">
            <h5>Deck size</h5>
            <p>{deckSize ?? "-"}</p>
          </div>
          <div className="workspace-glass-tile">
            <h5>Unknowns</h5>
            <p>{unknowns.length}</p>
          </div>
          <div className="workspace-glass-tile">
            <h5>Unknowns canonical</h5>
            <p>{unknownCanonicalCount ?? "-"}</p>
          </div>
          <div className="workspace-glass-tile">
            <h5>Cards needed / cut</h5>
            <p>
              {cardsNeeded ?? "-"} / {cardsToCut ?? "-"}
            </p>
          </div>
        </div>

        <p className="workspace-muted">pipeline stage: {pipelineStage}</p>

        {runtimeError ? <p className="workspace-error-banner">{runtimeError}</p> : null}
      </details>
    </section>
  );
}
