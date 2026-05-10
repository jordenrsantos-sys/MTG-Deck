/**
 * DomainDetail — Phase 4 BUNDLE Stage 1.
 *
 * Per-domain drill-down panel. Renders the codes list (failures/warnings)
 * + status. Honors UI_OVERHAUL_DESIGN D10 ("Why?" explainers everywhere
 * — content sourced from BuildResponse fields; never client-generated).
 */
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import Badge from "../../ui/primitives/Badge";
import {
  SUFFICIENCY_DOMAIN_LABELS,
  statusToBadgeVariant,
  type SufficiencyDomainVerdict,
} from "./sufficiencyTypes";

export type DomainDetailProps = {
  domain: string;
  verdict: SufficiencyDomainVerdict | null | undefined;
  className?: string;
};

export default function DomainDetail(props: DomainDetailProps) {
  const { domain, verdict, className } = props;
  const label = SUFFICIENCY_DOMAIN_LABELS[domain] ?? domain;
  const status = verdict?.status ?? "UNKNOWN";
  const codes = verdict?.codes ?? [];
  return (
    <Card className={className}>
      <CardHeader>
        <span className="flex items-center gap-token-2">
          {label}
          <Badge variant={statusToBadgeVariant(status)}>{status}</Badge>
        </span>
      </CardHeader>
      <CardBody>
        {codes.length === 0 ? (
          <p className="text-sm text-text-muted">No codes for this domain.</p>
        ) : (
          <ul className="flex flex-col gap-token-1">
            {codes.map((code) => (
              <li key={code} className="text-sm font-mono text-text-secondary">
                {code}
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}
