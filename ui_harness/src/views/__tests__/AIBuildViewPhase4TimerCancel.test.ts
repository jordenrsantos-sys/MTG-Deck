/**
 * AIBuildView — Mega-task v5 Phase 4 stopwatch + cancel + timeout tests.
 *
 * Source-grep contract checks that verify the UX bundle's Phase 4 pieces:
 *   - Wall-clock stopwatch driven by setInterval (vs server-emitted elapsed_s)
 *   - 240s client-side timeout aborts the stream + surfaces explicit error
 *   - Actionable Cancel button replaces the disabled "Building…" button
 *   - Cancel calls stream.cancel() + sets "Build cancelled." message
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const SOURCE_PATH = resolve(__dirname, "../AIBuildView.tsx");
const SOURCE = readFileSync(SOURCE_PATH, "utf8");

describe("AIBuildView Phase 4 — wall-clock stopwatch", () => {
  test("BUILD_TIMEOUT_SECONDS constant set to 240", () => {
    expect(SOURCE).toMatch(/const BUILD_TIMEOUT_SECONDS\s*=\s*240/);
  });

  test("BUILD_TYPICAL_LOW_S + BUILD_TYPICAL_HIGH_S constants exist", () => {
    expect(SOURCE).toMatch(/const BUILD_TYPICAL_LOW_S\s*=\s*110/);
    expect(SOURCE).toMatch(/const BUILD_TYPICAL_HIGH_S\s*=\s*130/);
  });

  test("stopwatch element has the documented test id + aria-live", () => {
    expect(SOURCE).toMatch(/data-testid="build-stopwatch"/);
    const block =
      SOURCE.match(/<span[\s\S]*?data-testid="build-stopwatch"[\s\S]*?<\/span>/)?.[0] ?? "";
    expect(block).toMatch(/aria-live="polite"/);
  });

  test("stopwatch displays wallSeconds + typical-range expectation anchor", () => {
    expect(SOURCE).toMatch(/wallSeconds\.toFixed\(0\)/);
    expect(SOURCE).toMatch(/typical \{BUILD_TYPICAL_LOW_S\}-\{BUILD_TYPICAL_HIGH_S\}s/);
  });

  test("setInterval driven by Date.now() (not setTimeout chain)", () => {
    expect(SOURCE).toMatch(/window\.setInterval/);
    expect(SOURCE).toMatch(/Date\.now\(\)/);
  });

  test("interval is cleared on unmount via cleanup function", () => {
    expect(SOURCE).toMatch(/window\.clearInterval\(intervalId\)/);
  });
});

describe("AIBuildView Phase 4 — 240s timeout", () => {
  test("interval-tick checks elapsed > BUILD_TIMEOUT_SECONDS and aborts", () => {
    expect(SOURCE).toMatch(/elapsed > BUILD_TIMEOUT_SECONDS/);
  });

  test("timeout calls stream.cancel and sets explicit error message", () => {
    // The kickoff requires the exact error wording:
    expect(SOURCE).toMatch(
      /Build exceeded expected duration\. Check engine logs in launch_dev\.cmd terminal\./
    );
  });
});

describe("AIBuildView Phase 4 — actionable cancel button", () => {
  test("cancel button replaces the disabled Building… button", () => {
    expect(SOURCE).toMatch(/data-testid="cancel-build-button"/);
    // The old `disabled={building}` Build-button branch is replaced with a
    // ternary that shows EITHER the Build button OR the Cancel button.
    expect(SOURCE).toMatch(
      /building \? \(\s*<Button[\s\S]*?Cancel build[\s\S]*?\) : \(\s*<Button[\s\S]*?Build deck/
    );
  });

  test("_cancelBuild calls stream.cancel + stream.reset + clears response", () => {
    const cancelFn = SOURCE.match(/function _cancelBuild\(\)[\s\S]*?\n  \}/)?.[0] ?? "";
    expect(cancelFn).toMatch(/stream\.cancel\(\)/);
    expect(cancelFn).toMatch(/stream\.reset\(\)/);
    expect(cancelFn).toMatch(/setResponse\(null\)/);
    expect(cancelFn).toMatch(/Build cancelled/);
  });

  test("Apply to Workspace button stays hidden while building", () => {
    // Avoid showing the post-build action while the build is still in flight.
    expect(SOURCE).toMatch(/response\?\.status === "OK" && !building/);
  });
});
