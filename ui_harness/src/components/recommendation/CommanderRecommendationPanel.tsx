/**
 * CommanderRecommendationPanel — Phase 4 BUNDLE Stage 3 (sub-task 4.5).
 *
 * Reads `commander_recommendation_v1` (Stage 0 canonical shape; Phase 2
 * Path C v1 — no theme_match field) and renders top_n=10 CandidateCards
 * with Pick + Why? affordances. INSUFFICIENT_SIGNAL surfaces a fallback
 * panel referencing Phase 2 Path C reduced ambition.
 */
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import Badge from "../../ui/primitives/Badge";
import CandidateCard from "./CandidateCard";
import type { CommanderCandidate, CommanderRecommendation } from "./commanderTypes";

export type CommanderRecommendationPanelProps = {
  recommendation: CommanderRecommendation | null | undefined;
  onPick?: (candidate: CommanderCandidate) => void;
  className?: string;
};

export default function CommanderRecommendationPanel(
  props: CommanderRecommendationPanelProps,
) {
  const { recommendation, onPick, className } = props;
  const status = recommendation?.status ?? "UNKNOWN";
  const candidates = recommendation?.candidates ?? [];
  const consideredCount = recommendation?.considered_count ?? 0;

  return (
    <Card className={className}>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          Commander Recommendations
          <Badge
            variant={
              status === "OK" ? "success" : status === "INSUFFICIENT_SIGNAL" ? "warn" : "neutral"
            }
          >
            {status}
          </Badge>
          {status === "OK" && consideredCount > 0 ? (
            <span className="text-xs text-text-muted">
              {consideredCount} considered → top {candidates.length}
            </span>
          ) : null}
        </span>
      </CardHeader>
      <CardBody>
        {recommendation == null ? (
          <p className="text-sm text-text-muted">
            No recommendation available — run Build to populate.
          </p>
        ) : status === "INSUFFICIENT_SIGNAL" ? (
          <div className="flex flex-col gap-token-2">
            <p className="text-sm text-text-secondary">
              Engine could not recommend a commander. The seed has insufficient
              canonical signal for Phase 2 Path C v1's reduced-ambition recommender
              (color_identity + primitives_overlap + GAME_CHANGERS density only).
            </p>
            <p className="text-xs text-text-muted">
              Phase 2.1 will broaden the recommender once curated commander metadata
              (theme_match + signature_completion + commander_role bracket-tier filter)
              ships. Until then: pick a commander manually OR add more cards to the
              seed for stronger primitive signal.
            </p>
          </div>
        ) : candidates.length === 0 ? (
          <p className="text-sm text-text-muted">
            Recommendation returned status=OK but no candidates. Try widening
            color identity or relaxing bracket constraints.
          </p>
        ) : (
          <ul className="flex flex-col gap-token-2" role="list">
            {candidates.map((c) => (
              <li key={c.oracle_id}>
                <CandidateCard candidate={c} onPick={onPick} />
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}
