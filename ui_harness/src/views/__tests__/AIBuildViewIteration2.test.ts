/**
 * AIBuildView — Pillar D iteration 2 UI surface tests.
 *
 * These tests grep the AIBuildView.tsx source for the iteration-2 UI
 * elements rather than rendering React (the project uses
 * environment="node" and does not depend on @testing-library/react;
 * existing AppRouter / Workspace tests follow the same pattern).
 *
 * The surfaces we verify exist:
 *   1. LLM-unavailable warning banner (data-testid="llm-unavailable-banner")
 *   2. LLM metrics block with cost / latency / token counts
 *      (data-testid="llm-metrics-block")
 *   3. Summary narrative paragraph (data-testid="summary-narrative")
 *   4. Novel combo flags list (data-testid="novel-combo-flags-list")
 *   5. Consider-adding list (data-testid="consider-adding-list")
 *   6. Intent-analysis card with implicit_themes + suggested_extensions
 *   7. Type definitions for llm_metrics, novel_combo_flags, etc.
 *
 * If any of these go missing (refactor / cleanup), this catches the
 * regression. Iteration-3 may delete or rename some of these; update
 * the regex set accordingly.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

const SOURCE_PATH = resolve(__dirname, "../AIBuildView.tsx");
const SOURCE = readFileSync(SOURCE_PATH, "utf8");


describe("AIBuildView iteration 2 — type definitions", () => {
  test("LlmCall type defined", () => {
    expect(SOURCE).toMatch(/type LlmCall\s*=\s*{/);
  });

  test("LlmMetrics type defined", () => {
    expect(SOURCE).toMatch(/type LlmMetrics\s*=\s*{/);
  });

  test("NovelComboFlag type defined", () => {
    expect(SOURCE).toMatch(/type NovelComboFlag\s*=\s*{/);
  });

  test("ConsiderAdding type defined", () => {
    expect(SOURCE).toMatch(/type ConsiderAdding\s*=\s*{/);
  });

  test("IntentAnalysis type defined", () => {
    expect(SOURCE).toMatch(/type IntentAnalysis\s*=\s*{/);
  });

  test("Summary type extended with iteration-2 fields", () => {
    expect(SOURCE).toMatch(/llm_metrics\?:\s*LlmMetrics/);
    expect(SOURCE).toMatch(/summary_narrative\?:/);
    expect(SOURCE).toMatch(/consider_adding\?:/);
    expect(SOURCE).toMatch(/novel_combo_flags\?:/);
    expect(SOURCE).toMatch(/intent_analysis\?:/);
  });
});


describe("AIBuildView iteration 2 — render surfaces", () => {
  test("LLM-unavailable banner rendered when warning present", () => {
    expect(SOURCE).toContain('data-testid="llm-unavailable-banner"');
    expect(SOURCE).toMatch(/LLM_LAYER_UNAVAILABLE/);
    expect(SOURCE).toMatch(/LLM reasoning unavailable/);
    expect(SOURCE).toMatch(/ANTHROPIC_API_KEY/);
  });

  test("LLM metrics block rendered with cost / latency / tokens", () => {
    expect(SOURCE).toContain('data-testid="llm-metrics-block"');
    expect(SOURCE).toMatch(/Built in/);
    expect(SOURCE).toMatch(/LLM call/);
    expect(SOURCE).toMatch(/total_cost_usd/);
    expect(SOURCE).toMatch(/total_input_tokens/);
    expect(SOURCE).toMatch(/total_output_tokens/);
  });

  test("Per-call breakdown details surface in expandable section", () => {
    expect(SOURCE).toMatch(/Per-call breakdown/);
  });

  test("Summary narrative card rendered when present", () => {
    expect(SOURCE).toContain('data-testid="summary-narrative"');
    expect(SOURCE).toMatch(/Deck narrative/);
  });

  test("Novel combo flags list rendered when non-empty", () => {
    expect(SOURCE).toContain('data-testid="novel-combo-flags-list"');
    expect(SOURCE).toMatch(/Combos surfaced/);
    expect(SOURCE).toMatch(/Spellbook combo/);
    expect(SOURCE).toMatch(/Novel/);
  });

  test("Consider-adding list rendered when non-empty", () => {
    expect(SOURCE).toContain('data-testid="consider-adding-list"');
    expect(SOURCE).toMatch(/Suggested additions to evaluate/);
  });

  test("Intent-analysis card includes win condition, themes, extensions", () => {
    expect(SOURCE).toMatch(/Intent analysis/);
    expect(SOURCE).toMatch(/Likely win condition/);
    expect(SOURCE).toMatch(/Implicit themes inferred/);
    expect(SOURCE).toMatch(/Suggested extensions/);
  });

  test("Iter2 narrative section is gated by status === 'OK' + at least one populated field", () => {
    // Should only render when we have something to show — avoids an empty
    // shell on iteration-1 fallback builds.
    expect(SOURCE).toContain('data-testid="iter2-narrative-section"');
    // The gating condition references the four fields.
    expect(SOURCE).toMatch(/summary_narrative \|\|/);
    expect(SOURCE).toMatch(/novel_combo_flags/);
    expect(SOURCE).toMatch(/consider_adding/);
    expect(SOURCE).toMatch(/intent_analysis/);
  });
});


describe("AIBuildView iteration 2 — backwards compatibility", () => {
  test("iteration-1 fields still rendered (Creativity envelope, Theme coherence)", () => {
    expect(SOURCE).toMatch(/Creativity envelope/);
    expect(SOURCE).toMatch(/Theme coherence/);
    expect(SOURCE).toMatch(/Staples avoided/);
  });

  test("iteration-1 Apply to Workspace flow preserved", () => {
    expect(SOURCE).toMatch(/Apply to Workspace/);
    expect(SOURCE).toMatch(/ACTIVE_DECK_STORAGE_KEY/);
  });
});
