/**
 * CandidateCard — Phase 4 BUNDLE Stage 3.
 *
 * One per commander_recommendation_v1.candidates entry. Surfaces name +
 * color identity + edge_score + primitives_overlap + "Pick" + "Why?"
 * affordances per UI_OVERHAUL_DESIGN D10 ("Why?" explainers everywhere
 * — content sourced from BuildResponse fields, never client-generated).
 */
import Button from "../../ui/primitives/Button";
import Badge from "../../ui/primitives/Badge";
import Tooltip from "../../ui/primitives/Tooltip";
import type { CommanderCandidate } from "./commanderTypes";

const COLOR_LETTERS: Record<string, string> = {
  W: "W",
  U: "U",
  B: "B",
  R: "R",
  G: "G",
};

export type CandidateCardProps = {
  candidate: CommanderCandidate;
  onPick?: (candidate: CommanderCandidate) => void;
  className?: string;
};

export default function CandidateCard(props: CandidateCardProps) {
  const { candidate, onPick, className } = props;
  const ci = (candidate.color_identity ?? []).map((c) => COLOR_LETTERS[c] ?? c).join("");
  const ciLabel = ci === "" ? "Colorless" : ci;
  const edge = typeof candidate.edge_score === "number" ? candidate.edge_score.toFixed(3) : "—";
  const overlap =
    typeof candidate.primitives_overlap === "number"
      ? candidate.primitives_overlap.toFixed(2)
      : "—";
  const whyText = `edge_score=${edge} (primitives_overlap=${overlap}, color_identity=${ciLabel})`;
  const classes = [
    "flex items-center justify-between gap-token-3 px-token-2 py-token-2",
    "rounded-token-md border border-glass-border bg-glass-bg",
    "hover:bg-bg-elev-1 transition-colors duration-fast",
    className,
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <article className={classes}>
      <div className="flex flex-col gap-token-1 min-w-0">
        <span className="text-text-primary text-sm font-medium truncate">
          {candidate.name}
        </span>
        <span className="flex items-center gap-token-2 text-xs text-text-muted">
          <Badge variant="info">{ciLabel}</Badge>
          <span>edge {edge}</span>
          <Tooltip content={whyText} placement="bottom">
            <span className="cursor-help underline-offset-2 hover:underline">
              Why?
            </span>
          </Tooltip>
        </span>
      </div>
      <Button
        size="sm"
        onClick={() => onPick?.(candidate)}
        aria-label={`Pick ${candidate.name} as commander`}
      >
        Pick
      </Button>
    </article>
  );
}
