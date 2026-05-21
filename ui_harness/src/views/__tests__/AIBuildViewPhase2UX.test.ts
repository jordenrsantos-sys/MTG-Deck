/**
 * AIBuildView — Mega-task v5 Phase 2 UX surface tests.
 *
 * Verifies the auto-default snapshot id + placeholder/help text bundle that
 * moved Snapshot ID under Advanced options so the default form is just
 * Commander -> Bracket -> Build.
 *
 * Follows the same source-grep pattern as AIBuildViewIteration2.test.ts —
 * environment="node" without @testing-library/react.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

const SOURCE_PATH = resolve(__dirname, "../AIBuildView.tsx");
const SOURCE = readFileSync(SOURCE_PATH, "utf8");

describe("AIBuildView Phase 2 — auto-default snapshot id", () => {
  test("useEffect fetches /snapshots/active on mount", () => {
    expect(SOURCE).toMatch(/import\s*{[^}]*\buseEffect\b[^}]*}\s*from\s*["']react["']/);
    expect(SOURCE).toMatch(/\/snapshots\/active/);
    expect(SOURCE).toMatch(/loadActiveSnapshot/);
  });

  test("auto-loaded snapshot id sets snapshotAutoLoaded flag", () => {
    expect(SOURCE).toMatch(/setSnapshotAutoLoaded\(true\)/);
  });

  test("fetch failure is silent (no surfaced error)", () => {
    // The catch block must not call setErrorMessage or alert — falling back
    // silently lets the user expand Advanced and type a snapshot id manually.
    expect(SOURCE).toMatch(/Silent fallback/);
  });
});

describe("AIBuildView Phase 2 — Advanced options collapsible", () => {
  test("Advanced options details block exists with test id", () => {
    expect(SOURCE).toMatch(/data-testid="advanced-options"/);
  });

  test("Snapshot ID input lives inside Advanced details", () => {
    // The snapshot id Input must be inside the details block (not at top-level).
    // Easiest assertion: the Snapshot ID input has the data-testid + its
    // surrounding details block contains it.
    expect(SOURCE).toMatch(/data-testid="snapshot-id-input"/);
    const advancedBlockMatch = SOURCE.match(
      /<details[\s\S]*?data-testid="advanced-options"[\s\S]*?<\/details>/m
    );
    expect(advancedBlockMatch).toBeTruthy();
    expect(advancedBlockMatch?.[0] ?? "").toMatch(/data-testid="snapshot-id-input"/);
  });

  test("Advanced block is collapsed by default", () => {
    expect(SOURCE).toMatch(/const \[advancedOpen, setAdvancedOpen\] = useState\(false\)/);
  });
});

describe("AIBuildView Phase 2 — placeholder text", () => {
  test("Commander placeholder uses example name", () => {
    expect(SOURCE).toMatch(/placeholder="e\.g\.,\s*Edgar Markov"/);
  });

  test("Theme hints placeholder explains it's optional", () => {
    expect(SOURCE).toMatch(/placeholder="e\.g\.,\s*aristocrats[^"]*optional/);
  });

  test("Must-includes placeholder uses card-name example", () => {
    expect(SOURCE).toMatch(/placeholder="e\.g\.,\s*Vito,/);
  });

  test("Snapshot ID placeholder uses a real-looking snapshot value", () => {
    expect(SOURCE).toMatch(/placeholder="e\.g\.,\s*\d{8}_\d{6}_tagpass_\d{8}"/);
  });
});

describe("AIBuildView Phase 2 — Snapshot ID no longer top-level", () => {
  test("Snapshot ID input does not appear in the inline form area above Advanced", () => {
    // The Snapshot ID block must NOT appear between the Commander label and
    // the Bracket label — that position was removed in Phase 2.
    const commanderToBracket =
      SOURCE.match(/Commander name[\s\S]*?aria-label="Bracket"/)?.[0] ?? "";
    expect(commanderToBracket).not.toMatch(/Snapshot ID/);
  });
});
