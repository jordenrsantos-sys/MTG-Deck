/**
 * Vitest tests for CommanderTypeahead — Mega-task v7 Phase 2.
 *
 * The ui_harness test environment is "node" (no jsdom, no testing-library),
 * so we verify component behavior via source-level contract assertions
 * rather than DOM rendering. The contract: (1) hits /cards/suggest with
 * commander_only=true, (2) falls back to fuzzy=true on empty exact result,
 * (3) debounces 250ms, (4) handles keyboard navigation, (5) calls onChange
 * on select, (6) AIBuildView wires the component into the commander field.
 *
 * Live UI verification happens via the dev server + browser walkthrough in
 * Phase 8 (the chrome-devtools-mcp substitute documented in the v7 progress
 * log Phase 0 risk note).
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const COMPONENT_PATH = resolve(__dirname, "..", "CommanderTypeahead.tsx");
const AI_BUILD_VIEW_PATH = resolve(__dirname, "..", "..", "views", "AIBuildView.tsx");

function readFile(p: string): string {
  return readFileSync(p, "utf8");
}

describe("CommanderTypeahead source contract — v7 Phase 2", () => {
  const source = readFile(COMPONENT_PATH);

  test("exports a default React functional component", () => {
    expect(source).toMatch(/export default function CommanderTypeahead/);
  });

  test("hits /cards/suggest with commander_only=true", () => {
    expect(source).toContain("/cards/suggest?q=");
    expect(source).toContain("commander_only=true");
  });

  test("falls back to fuzzy=true after empty exact match", () => {
    expect(source).toContain("fuzzy=true");
    // The fuzzy fetch happens after exactRows.length === 0 — verify both
    // exact + fuzzy URLs are built.
    expect(source).toMatch(/exactUrl\s*=/);
    expect(source).toMatch(/fuzzyUrl\s*=/);
  });

  test("debounces at 250ms per kickoff spec", () => {
    expect(source).toMatch(/const DEBOUNCE_MS\s*=\s*250/);
  });

  test("handles ArrowUp / ArrowDown / Enter / Escape keys", () => {
    expect(source).toContain('"ArrowUp"');
    expect(source).toContain('"ArrowDown"');
    expect(source).toContain('"Enter"');
    expect(source).toContain('"Escape"');
  });

  test("calls onChange when a row is selected", () => {
    expect(source).toMatch(/onChange\(row\.name\)/);
  });

  test("surfaces fuzzy suggestion as 'Did you mean: <name>?'", () => {
    expect(source).toContain("Did you mean:");
    expect(source).toMatch(/setFuzzySuggestion\(fuzzyRows\[0\]\)/);
  });

  test("aborts in-flight fetches on unmount / new query", () => {
    expect(source).toContain("AbortController");
    expect(source).toContain("controller.abort()");
  });

  test("ignores stale responses via requestId guard", () => {
    expect(source).toMatch(/requestId\s*!==\s*requestIdRef\.current/);
  });

  test("dropdown listbox has accessible role + aria attributes", () => {
    expect(source).toMatch(/role="listbox"/);
    expect(source).toMatch(/role="option"/);
    expect(source).toMatch(/aria-selected=/);
    expect(source).toMatch(/aria-autocomplete=/);
  });

  test("does not perform less-than-2 character queries", () => {
    expect(source).toMatch(/query\.length\s*<\s*2/);
  });
});

describe("AIBuildView wires in the typeahead — v7 Phase 2", () => {
  const source = readFile(AI_BUILD_VIEW_PATH);

  test("imports CommanderTypeahead from the components directory", () => {
    expect(source).toMatch(/from\s+"\.\.\/components\/CommanderTypeahead"/);
  });

  test("renders <CommanderTypeahead> in the Commander field", () => {
    expect(source).toContain("<CommanderTypeahead");
    expect(source).toMatch(/value=\{commander\}/);
    // setCommander reaches the typeahead's onChange.
    expect(source).toMatch(/onChange=\{\(next\)\s*=>\s*setCommander\(next\)\}/);
  });

  test("passes apiBase + snapshotId so the typeahead can fetch", () => {
    expect(source).toMatch(/apiBase=\{API_BASE_URL\}/);
    expect(source).toMatch(/snapshotId=\{snapshotId\}/);
  });

  test("does NOT also render the legacy plain <Input> for Commander", () => {
    // Pre-v7 had <Input value={commander} ... aria-label="Commander name" />.
    // Should be removed in v7 Phase 2.
    expect(source).not.toMatch(/<Input[^>]*value=\{commander\}/);
  });
});
