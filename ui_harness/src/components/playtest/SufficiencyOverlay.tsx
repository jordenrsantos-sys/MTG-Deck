/**
 * SufficiencyOverlay — Phase 4.9 Stage 3.
 *
 * Read-only display of `probability_checkpoint_layer_v1` per-turn data IF
 * present in BuildResponse. Per HARD safety #10 (Decision 10): all "Why?"
 * tooltip text MUST come from BuildResponse fields — NEVER client-generated.
 * If `probability_checkpoint_layer_v1` is absent, show graceful empty state
 * per autonomous_repair_log soft-safety #1 — do NOT fabricate data.
 *
 * Shape tolerance: the overlay reads `checkpoints` as either an array
 * (indexable by turn-1) or a record keyed by turn number. Each checkpoint
 * is treated as an opaque record; only canonical fields are surfaced
 * (`turn`, `probability`, `status`, `note`/`why`/`reason`). Unknown shape
 * → "no per-turn data" empty state.
 */
import Badge from "../../ui/primitives/Badge";
import Tooltip from "../../ui/primitives/Tooltip";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";

export type ProbabilityCheckpointEntry = {
  turn?: number;
  probability?: number;
  status?: "PASS" | "WARN" | "FAIL" | string;
  note?: string;
  why?: string;
  reason?: string;
  [key: string]: unknown;
};

export type ProbabilityCheckpointLayer = {
  version?: string;
  status?: string;
  checkpoints?: ReadonlyArray<ProbabilityCheckpointEntry> | Record<string, ProbabilityCheckpointEntry>;
  [key: string]: unknown;
};

export type SufficiencyOverlayProps = {
  /** May be undefined / null / wrong-shape — overlay tolerates all per soft-safety #1. */
  layer?: ProbabilityCheckpointLayer | null;
  currentTurn: number;
};

export function statusVariantFromString(status: string | undefined): "success" | "warn" | "error" | "neutral" {
  if (status === "PASS") return "success";
  if (status === "WARN") return "warn";
  if (status === "FAIL") return "error";
  return "neutral";
}

export function findCheckpointForTurn(
  layer: ProbabilityCheckpointLayer | null | undefined,
  turn: number,
): ProbabilityCheckpointEntry | null {
  if (!layer || !layer.checkpoints) return null;
  const cp = layer.checkpoints;
  if (Array.isArray(cp)) {
    const byField = cp.find((e) => typeof e?.turn === "number" && e.turn === turn);
    if (byField) return byField;
    const idx = turn - 1;
    if (idx >= 0 && idx < cp.length) return cp[idx];
    return null;
  }
  if (typeof cp === "object") {
    const direct = (cp as Record<string, ProbabilityCheckpointEntry>)[String(turn)];
    return direct ?? null;
  }
  return null;
}

export default function SufficiencyOverlay({ layer, currentTurn }: SufficiencyOverlayProps) {
  // Per soft-safety #1: graceful fallback when probability_checkpoint_layer_v1
  // is absent or wrong-shape. NEVER fabricate per-turn data (HARD safety #10).
  if (!layer || !layer.checkpoints) {
    return (
      <Card>
        <CardHeader>Sufficiency overlay</CardHeader>
        <CardBody>
          <div className="text-sm text-text-muted" role="status" aria-live="polite">
            Per-turn sufficiency data not available — engine response did not
            include <code>probability_checkpoint_layer_v1</code>.
          </div>
        </CardBody>
      </Card>
    );
  }

  const entry = findCheckpointForTurn(layer, currentTurn);
  if (!entry) {
    return (
      <Card>
        <CardHeader>Sufficiency overlay</CardHeader>
        <CardBody>
          <div className="text-sm text-text-muted" role="status" aria-live="polite">
            No checkpoint recorded for turn {currentTurn}.
          </div>
        </CardBody>
      </Card>
    );
  }

  // Why? tooltip text comes ONLY from BuildResponse fields per HARD safety #10.
  const whyText: string | null =
    typeof entry.why === "string" && entry.why.trim() !== ""
      ? entry.why
      : typeof entry.note === "string" && entry.note.trim() !== ""
        ? entry.note
        : typeof entry.reason === "string" && entry.reason.trim() !== ""
          ? entry.reason
          : null;
  const probability =
    typeof entry.probability === "number" && Number.isFinite(entry.probability)
      ? entry.probability.toFixed(2)
      : "-";
  const status = typeof entry.status === "string" ? entry.status : "UNKNOWN";

  const headlineNode = (
    <span className="flex items-center gap-token-2">
      Turn {currentTurn} · <Badge variant={statusVariantFromString(status)}>{status}</Badge>
      <span className="text-xs text-text-muted">p = {probability}</span>
    </span>
  );

  return (
    <Card>
      <CardHeader>Sufficiency overlay</CardHeader>
      <CardBody>
        {whyText ? (
          <Tooltip content={whyText} placement="top">
            {headlineNode}
          </Tooltip>
        ) : (
          headlineNode
        )}
      </CardBody>
    </Card>
  );
}
