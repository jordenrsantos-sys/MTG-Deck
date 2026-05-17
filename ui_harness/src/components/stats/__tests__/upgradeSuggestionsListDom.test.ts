/**
 * Vitest DOM-render tests for UpgradeSuggestionsList — v1.3 Stage 2.
 *
 * **CRITICAL** per spec: the v1.2 Stage 2 close-out was a false-pass
 * because the existing logic-level tests asserted on appliedKeys state
 * transitions but never on rendered DOM. Live-retest 2026-05-10 then
 * verified that neither "Applied ✓" badge nor cumulative counter
 * actually appeared after click — root cause was the parent unmounting
 * the panel on USER_EDIT_DECK_TEXT (now fixed in WorkspaceView via
 * the upgradeSnapshotRows hoist).
 *
 * These tests use react-dom/server.renderToString (built-in to
 * react-dom@18.3.1; no new dependency) to assert the actual HTML
 * output contains the expected text. Per AUTOMATION_RULES halt-and-ask
 * + autonomous_repair_log soft-safety: no @testing-library/react
 * install — the renderToString approach is sufficient because we
 * seed state via the new `initialAppliedIndexes` prop and assert
 * on the static HTML.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import UpgradeSuggestionsList from "../UpgradeSuggestionsList";
import type { UpgradeSwapSuggestion } from "../../../lib/workspaceDeckState";

const SAMPLE_ROWS: ReadonlyArray<UpgradeSwapSuggestion> = [
  { cut_name: "Forest", add_name: "Sol Ring", reasons_v1: ["mana_acceleration"] },
  { cut_name: "Mountain", add_name: "Arcane Signet", reasons_v1: ["mana_acceleration"] },
  { cut_name: "Plains", add_name: "Smothering Tithe", reasons_v1: ["card_advantage"] },
];

function renderPanel(opts: {
  initialAppliedIndexes?: ReadonlyArray<number>;
  rows?: ReadonlyArray<UpgradeSwapSuggestion>;
}): string {
  const rows = opts.rows ?? SAMPLE_ROWS;
  const raw = renderToString(
    React.createElement(UpgradeSuggestionsList, {
      rows,
      decklistText: "1 Forest\n1 Mountain\n1 Plains",
      onDecklistChange: () => undefined,
      onClear: () => undefined,
      initialAppliedIndexes: opts.initialAppliedIndexes,
    }),
  );
  // React inserts <!-- --> separators between adjacent text/expression
  // children in renderToString output. Strip them so substring assertions
  // on visible text work cleanly. (See React docs on `renderToString`
  // text-comment-separator behavior.)
  return raw.replace(/<!-- -->/g, "");
}

describe("UpgradeSuggestionsList — rendered DOM contains expected text", () => {
  test("initial render shows 3 Apply buttons, NO 'Applied ✓', NO counter badge", () => {
    const html = renderPanel({});
    // Each row should have an Apply button + aria-label for the swap.
    expect(html).toContain("Apply swap: Forest → Sol Ring");
    expect(html).toContain("Apply swap: Mountain → Arcane Signet");
    expect(html).toContain("Apply swap: Plains → Smothering Tithe");
    // No applied badges yet.
    expect(html).not.toContain("Applied ✓");
    // No cumulative counter badge — only the total info Badge ("3") is shown.
    expect(html).not.toContain("applied</span>");
    expect(html).not.toContain("/3 applied");
  });

  test("with initialAppliedIndexes=[0] → row 0 shows 'Applied ✓' + counter shows '1/3 applied'", () => {
    const html = renderPanel({ initialAppliedIndexes: [0] });
    expect(html).toContain("Applied ✓");
    expect(html).toContain("1/3 applied");
    // Apply button for row 0 should NOT appear; rows 1 and 2 should still have Apply.
    expect(html).not.toContain("Apply swap: Forest → Sol Ring");
    expect(html).toContain("Apply swap: Mountain → Arcane Signet");
    expect(html).toContain("Apply swap: Plains → Smothering Tithe");
  });

  test("with initialAppliedIndexes=[0, 1] → counter shows '2/3 applied' + two badges", () => {
    const html = renderPanel({ initialAppliedIndexes: [0, 1] });
    // Counter shows 2/3 applied.
    expect(html).toContain("2/3 applied");
    // 2 Applied ✓ badges + 1 remaining Apply button.
    const appliedCount = (html.match(/Applied ✓/g) ?? []).length;
    expect(appliedCount).toBe(2);
    expect(html).toContain("Apply swap: Plains → Smothering Tithe");
    expect(html).not.toContain("Apply swap: Forest → Sol Ring");
    expect(html).not.toContain("Apply swap: Mountain → Arcane Signet");
  });

  test("all 3 applied → counter shows '3/3 applied' + Apply all disabled", () => {
    const html = renderPanel({ initialAppliedIndexes: [0, 1, 2] });
    expect(html).toContain("3/3 applied");
    const appliedCount = (html.match(/Applied ✓/g) ?? []).length;
    expect(appliedCount).toBe(3);
    // Apply all button should have disabled attribute set.
    expect(html).toContain("disabled");
    // No per-row Apply buttons remaining (only Apply all in header).
    expect(html).not.toContain("Apply swap:");
  });

  test("counter badge visible text follows 'N/M applied' format", () => {
    const html = renderPanel({ initialAppliedIndexes: [0, 1] });
    // Visible text in the cumulative-counter Badge.
    expect(html).toContain("2/3 applied");
  });

  test("Applied ✓ badge appears in the row that was applied (and not in unapplied rows)", () => {
    const html = renderPanel({ initialAppliedIndexes: [0] });
    // Row 0 (Forest → Sol Ring) shows Applied ✓ — assert by counting badges + Apply buttons.
    expect((html.match(/Applied ✓/g) ?? []).length).toBe(1);
    expect((html.match(/Apply swap:/g) ?? []).length).toBe(2);
    // Row 0's cut+add still appear (in the strikethrough/highlight spans), just without the Apply button.
    expect(html).toContain("Forest");
    expect(html).toContain("Sol Ring");
  });

  test("empty rows array → empty-state copy renders (no panel collapse)", () => {
    const html = renderPanel({ rows: [] });
    expect(html).toContain("No upgrade suggestions");
    expect(html).not.toContain("Applied ✓");
    // Counter must NOT show when there are no rows + no applies.
    expect(html).not.toContain("applied");
  });

  test("cut_name + add_name render in expected positions (regression check)", () => {
    const html = renderPanel({});
    expect(html).toContain("Forest");
    expect(html).toContain("Sol Ring");
    // Cut name is rendered with line-through class (strikethrough).
    expect(html).toContain("line-through");
  });

  test("reasons_v1 badges render with tooltip placement", () => {
    const html = renderPanel({});
    expect(html).toContain("mana_acceleration");
    expect(html).toContain("card_advantage");
  });
});
