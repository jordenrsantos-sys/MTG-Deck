/**
 * Commander recommendation types — Phase 4 BUNDLE Stage 3 (sub-task 4.5).
 *
 * Mirrors Stage 0's canonical capture from
 * `repo/api/engine/layers/commander_recommendation_v1.py`:
 *
 *   {
 *     version: "commander_recommendation_v1",
 *     status: "OK" | "INSUFFICIENT_SIGNAL",
 *     top_n: number,
 *     considered_count: number,
 *     candidates: [{ name, color_identity, edge_score, primitives_overlap, oracle_id }],
 *   }
 *
 * Phase 2 Path C v1 reduced ambition: NO `theme_match` field. Per
 * autonomous_repair_log soft-safety #3 (INSUFFICIENT_SIGNAL on commander
 * rec → fallback panel), the panel surfaces this state explicitly.
 */
export type CommanderCandidate = {
  name: string;
  oracle_id: string;
  color_identity: ReadonlyArray<string>;
  edge_score: number;
  primitives_overlap: number;
};

export type CommanderRecommendationStatus = "OK" | "INSUFFICIENT_SIGNAL" | "UNKNOWN";

export type CommanderRecommendation = {
  version?: string;
  status?: CommanderRecommendationStatus;
  top_n?: number;
  considered_count?: number;
  candidates?: ReadonlyArray<CommanderCandidate>;
};
