/**
 * AIBuildView — Mega-task v5 Phase 3 streaming surfaces.
 *
 * Source-grep contract tests that catch regressions if a refactor removes
 * the SSE wiring, the progress panel, or the phase-label mapping.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const SOURCE_PATH = resolve(__dirname, "../AIBuildView.tsx");
const SOURCE = readFileSync(SOURCE_PATH, "utf8");

describe("AIBuildView Phase 3 — useBuildStreaming wiring", () => {
  test("imports useBuildStreaming hook", () => {
    expect(SOURCE).toMatch(/import\s*{\s*useBuildStreaming\s*}\s*from\s*["']\.\.\/hooks\/useBuildStreaming["']/);
  });

  test("instantiates the stream hook with apiBaseUrl", () => {
    expect(SOURCE).toMatch(/useBuildStreaming\(\{\s*apiBaseUrl:\s*API_BASE_URL\s*\}\)/);
  });

  test("derives building from stream.isStreaming", () => {
    expect(SOURCE).toMatch(/const building\s*=\s*stream\.isStreaming/);
  });

  test("_build calls stream.start with the request payload", () => {
    expect(SOURCE).toMatch(/await stream\.start\(\{/);
    expect(SOURCE).toMatch(/db_snapshot_id:\s*snap/);
  });

  test("useEffect mirrors stream.finalResponse to local response state", () => {
    expect(SOURCE).toMatch(/if \(stream\.finalResponse != null\)/);
    expect(SOURCE).toMatch(/setResponse\(stream\.finalResponse as BuildResponse\)/);
  });

  test("useEffect surfaces stream.errorMessage", () => {
    expect(SOURCE).toMatch(/if \(stream\.errorMessage\)/);
    expect(SOURCE).toMatch(/setErrorMessage\(stream\.errorMessage\)/);
  });
});

describe("AIBuildView Phase 3 — build progress panel", () => {
  test("renders a progress panel with the documented test id", () => {
    expect(SOURCE).toMatch(/data-testid="build-progress-panel"/);
  });

  test("progress panel exposes elapsed seconds with test id", () => {
    expect(SOURCE).toMatch(/data-testid="build-progress-elapsed"/);
  });

  test("progress panel shows cumulative LLM cost", () => {
    expect(SOURCE).toMatch(/LLM cost so far/);
    expect(SOURCE).toMatch(/cumulativeCostUsd\.toFixed\(4\)/);
  });

  test("progress panel is aria-live=polite for screen readers", () => {
    expect(SOURCE).toMatch(/aria-live="polite"/);
  });

  test("phase label mapping covers every server-emitted phase", () => {
    // Each of these phases is emitted by the backend's _emit_progress; the
    // UI mapping must handle them (or fall back gracefully to the raw id).
    const phases = [
      "intent_interpreter",
      "candidate_pool",
      "select_deck",
      "c21_c22_parallel",
      "validate_swap",
      "final_critic",
      "mana_base",
      "card_advantage",
      "structural_safety_net",
      "complete",
    ];
    for (const phase of phases) {
      // Match the key in the label record.
      expect(SOURCE).toMatch(new RegExp(`${phase}:\\s*["']`));
    }
  });
});

describe("AIBuildView Phase 3 — backwards compat", () => {
  test("the old non-streaming fetch to /agent/build_deck_v1 is replaced", () => {
    // The synchronous fetch must not appear inside _build anymore — the hook
    // now drives the request. (The non-streaming endpoint still exists on
    // the server for Python/programmatic clients; the UI just doesn't use it.)
    const buildFn = SOURCE.match(/async function _build\(\)[\s\S]*?\n  \}/)?.[0] ?? "";
    expect(buildFn).not.toContain("/agent/build_deck_v1");
  });
});
