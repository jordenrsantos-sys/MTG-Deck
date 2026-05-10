/**
 * DomainChip — Phase 4 BUNDLE Stage 1.
 *
 * One per sufficiency domain; surfaces domain label + status Badge with
 * color-coded variant. Click → opens the matching DomainDetail surface.
 */
import Badge from "../../ui/primitives/Badge";
import Tooltip from "../../ui/primitives/Tooltip";
import {
  SUFFICIENCY_DOMAIN_LABELS,
  statusToBadgeVariant,
  type SufficiencyDomainStatus,
} from "./sufficiencyTypes";

export type DomainChipProps = {
  domain: string;
  status: SufficiencyDomainStatus | undefined | null;
  codeCount?: number;
  onClick?: () => void;
  active?: boolean;
};

export default function DomainChip(props: DomainChipProps) {
  const { domain, status, codeCount = 0, onClick, active = false } = props;
  const label = SUFFICIENCY_DOMAIN_LABELS[domain] ?? domain;
  const variant = statusToBadgeVariant(status ?? null);
  const stateLabel = status ?? "UNKNOWN";
  const tooltipText =
    codeCount > 0
      ? `${stateLabel} — ${codeCount} code${codeCount === 1 ? "" : "s"} (click for details)`
      : `${stateLabel} (click for details)`;
  const classes = [
    "inline-flex items-center gap-token-2 px-token-2 py-token-1",
    "rounded-token-md border transition-colors duration-fast",
    active
      ? "border-accent bg-bg-elev-2"
      : "border-glass-border bg-glass-bg hover:bg-bg-elev-1",
    "focus:outline-none focus-visible:shadow-focus-ring",
  ].join(" ");
  return (
    <Tooltip content={tooltipText} placement="top">
      <button
        type="button"
        className={classes}
        onClick={onClick}
        aria-pressed={active}
        aria-label={`${label}: ${stateLabel}${codeCount ? `, ${codeCount} codes` : ""}`}
      >
        <span className="text-sm text-text-primary">{label}</span>
        <Badge variant={variant}>{stateLabel}</Badge>
      </button>
    </Tooltip>
  );
}
