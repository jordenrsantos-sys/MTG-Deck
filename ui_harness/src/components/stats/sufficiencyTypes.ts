/**
 * Sufficiency types — Phase 4 BUNDLE Stage 1.
 *
 * Shapes mirror Stage 0's canonical capture from
 * `repo/api/engine/layers/sufficiency_summary_v1.py`:
 *
 *   {
 *     version: string,
 *     status: "PASS" | "WARN" | "FAIL",
 *     reason_code: string | null,
 *     codes: string[],
 *     failures: string[],
 *     warnings: string[],
 *     domain_verdicts: { <domain>: { status, codes } },
 *     thresholds_used: object,
 *     versions_used: object,
 *   }
 *
 * Six engine domains per OQ-3 resolution: probability / stress / resilience /
 * commander_reliability / required_effects / bracket_compliance.
 */
export type SufficiencyDomainStatus = "PASS" | "WARN" | "FAIL" | "UNKNOWN";

export type SufficiencyDomainVerdict = {
  status: SufficiencyDomainStatus;
  codes: ReadonlyArray<string>;
};

export type SufficiencySummary = {
  version?: string;
  status?: SufficiencyDomainStatus;
  reason_code?: string | null;
  codes?: ReadonlyArray<string>;
  failures?: ReadonlyArray<string>;
  warnings?: ReadonlyArray<string>;
  domain_verdicts?: Record<string, SufficiencyDomainVerdict>;
  thresholds_used?: Record<string, unknown>;
  versions_used?: Record<string, unknown>;
};

export const SUFFICIENCY_DOMAIN_ORDER: ReadonlyArray<string> = [
  "probability",
  "stress",
  "resilience",
  "commander_reliability",
  "required_effects",
  "bracket_compliance",
];

export const SUFFICIENCY_DOMAIN_LABELS: Record<string, string> = {
  probability: "Probability",
  stress: "Stress",
  resilience: "Resilience",
  commander_reliability: "Commander Reliability",
  required_effects: "Required Effects",
  bracket_compliance: "Bracket Compliance",
};

export type SufficiencyHeadlineCounts = {
  pass: number;
  warn: number;
  fail: number;
  unknown: number;
  total: number;
};

/**
 * Tally pass/warn/fail across the six canonical domains. Missing domains
 * count as UNKNOWN per autonomous_repair_log soft-safety #7 (missing
 * bracket_compliance field → warning badge only; here we distinguish
 * UNKNOWN to surface a neutral count instead of failing).
 */
export function computeHeadlineCounts(
  summary: SufficiencySummary | null | undefined,
): SufficiencyHeadlineCounts {
  const counts: SufficiencyHeadlineCounts = {
    pass: 0,
    warn: 0,
    fail: 0,
    unknown: 0,
    total: SUFFICIENCY_DOMAIN_ORDER.length,
  };
  const verdicts = summary?.domain_verdicts ?? {};
  for (const domain of SUFFICIENCY_DOMAIN_ORDER) {
    const v = verdicts[domain];
    if (!v) {
      counts.unknown += 1;
      continue;
    }
    const s = v.status;
    if (s === "PASS") counts.pass += 1;
    else if (s === "WARN") counts.warn += 1;
    else if (s === "FAIL") counts.fail += 1;
    else counts.unknown += 1;
  }
  return counts;
}

export function statusToBadgeVariant(
  status: SufficiencyDomainStatus | undefined | null,
): "success" | "warn" | "error" | "neutral" {
  if (status === "PASS") return "success";
  if (status === "WARN") return "warn";
  if (status === "FAIL") return "error";
  return "neutral";
}
