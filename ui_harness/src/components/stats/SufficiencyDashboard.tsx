/**
 * SufficiencyDashboard — Phase 4 BUNDLE Stage 1 (sub-task 4.7).
 *
 * Top-of-workspace deck-health widget per UI_OVERHAUL_DESIGN D4.
 * Headline: "N PASS / N WARN / N FAIL" per OQ-3 resolution (six engine
 * domains, no synthesized letter score).
 *
 * Architecture:
 *   - Reads `sufficiency_summary_v1` shape captured at Stage 0.
 *   - Six DomainChips in a row; clicking one expands its DomainDetail panel.
 *   - All content text comes from the BuildResponse payload — no client
 *     fabrication per HARD safety + D10.
 */
import { useState } from "react";
import { Card, CardHeader, CardBody } from "../../ui/primitives/Card";
import Badge from "../../ui/primitives/Badge";
import DomainChip from "./DomainChip";
import DomainDetail from "./DomainDetail";
import {
  SUFFICIENCY_DOMAIN_ORDER,
  computeHeadlineCounts,
  type SufficiencySummary,
} from "./sufficiencyTypes";

export type SufficiencyDashboardProps = {
  summary: SufficiencySummary | null | undefined;
  className?: string;
};

export default function SufficiencyDashboard(props: SufficiencyDashboardProps) {
  const { summary, className } = props;
  const [activeDomain, setActiveDomain] = useState<string | null>(null);
  const counts = computeHeadlineCounts(summary);
  const verdicts = summary?.domain_verdicts ?? {};
  const overallStatus = summary?.status ?? "UNKNOWN";

  return (
    <Card className={className}>
      <CardHeader>
        <span className="flex items-center justify-between gap-token-3 w-full">
          <span className="flex items-center gap-token-2">
            Deck Health
            <Badge
              variant={
                overallStatus === "PASS"
                  ? "success"
                  : overallStatus === "WARN"
                  ? "warn"
                  : overallStatus === "FAIL"
                  ? "error"
                  : "neutral"
              }
            >
              {overallStatus}
            </Badge>
          </span>
          <span className="text-sm text-text-muted">
            {counts.pass} PASS · {counts.warn} WARN · {counts.fail} FAIL
            {counts.unknown > 0 ? ` · ${counts.unknown} unknown` : ""}
          </span>
        </span>
      </CardHeader>
      <CardBody>
        {summary == null ? (
          <p className="text-sm text-text-muted">
            No sufficiency summary available yet — run Build to populate.
          </p>
        ) : (
          <>
            <div
              className="flex flex-wrap gap-token-2 mb-token-3"
              role="group"
              aria-label="Sufficiency domains"
            >
              {SUFFICIENCY_DOMAIN_ORDER.map((domain) => {
                const v = verdicts[domain];
                return (
                  <DomainChip
                    key={domain}
                    domain={domain}
                    status={v?.status ?? "UNKNOWN"}
                    codeCount={v?.codes?.length ?? 0}
                    active={activeDomain === domain}
                    onClick={() =>
                      setActiveDomain((prev) => (prev === domain ? null : domain))
                    }
                  />
                );
              })}
            </div>
            {activeDomain ? (
              <DomainDetail domain={activeDomain} verdict={verdicts[activeDomain]} />
            ) : null}
          </>
        )}
      </CardBody>
    </Card>
  );
}
