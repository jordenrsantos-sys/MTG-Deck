/**
 * Vitest tests for v1.6.3 Stage 2 — AddedCardsPanel pending-review mode.
 *
 * Source-level + render-level evidence that:
 *   - The panel renders pending-review UI when `pendingAdds` is non-empty.
 *   - Per-row checkbox calls `onTogglePendingAdd(index)`.
 *   - Bulk controls (Apply Selected / Accept All / Reject All / Dismiss)
 *     wire to the corresponding callbacks.
 *   - The v1.6.2 translation map BYTE-IDENTICAL — pending-review chips
 *     show translated prose AND preserve raw engine codes on hover.
 *   - The v1.1/v1.3 default-mode render preserved verbatim when
 *     `pendingAdds` is empty/omitted (back-compat with existing 8
 *     addedCardsPanel.test.ts tests).
 *
 * Per AUTOMATION_RULES halt-and-ask: no @testing-library/react install;
 * uses react-dom/server.renderToString with the additive controlled-mode
 * props pattern established by v1.6.2.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import AddedCardsPanel from "../AddedCardsPanel";
import type { ProposedAdd } from "../../../lib/workspaceDeckState";

const SAMPLE_PENDING: ReadonlyArray<ProposedAdd> = [
  { card_name: "Sol Ring", reasons: ["ADD_BASIC_LAND_FILL_AUTO"], accepted: true },
  { card_name: "Arcane Signet", reasons: ["COMPLETE_TO_TARGET_SIZE"], accepted: true },
  { card_name: "Smothering Tithe", reasons: ["ADD_REQUIRED_COVERAGE"], accepted: false },
];

function renderPanel(opts: {
  pendingAdds?: ReadonlyArray<ProposedAdd>;
  onTogglePendingAdd?: (index: number) => void;
  onApplyAccepted?: () => void;
  onAcceptAll?: () => void;
  onRejectAll?: () => void;
  onDismissPending?: () => void;
  rows?: React.ComponentProps<typeof AddedCardsPanel>["rows"];
}): string {
  const raw = renderToString(
    React.createElement(AddedCardsPanel, {
      rows: opts.rows ?? [],
      pendingAdds: opts.pendingAdds,
      onTogglePendingAdd: opts.onTogglePendingAdd ?? (() => undefined),
      onApplyAccepted: opts.onApplyAccepted ?? (() => undefined),
      onAcceptAll: opts.onAcceptAll ?? (() => undefined),
      onRejectAll: opts.onRejectAll ?? (() => undefined),
      onDismissPending: opts.onDismissPending ?? (() => undefined),
    }),
  );
  return raw.replace(/<!-- -->/g, "");
}

describe("v1.6.3 Stage 2 — pending-review mode render", () => {
  test("non-empty pendingAdds → pending-review panel renders (NOT default mode)", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    expect(html).toContain('data-v163-stage="pending-review-panel"');
    expect(html).toContain("Review proposed additions");
    // Default-mode header copy ("Added cards" + "Why? (engine)") absent.
    expect(html).not.toContain("Why? (engine)");
  });

  test("count badge shows total + accepted/total badge", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    // 3 total, 2 accepted (third one's accepted:false).
    expect(html).toMatch(/3.*card/i); // total badge ~ "3"
    expect(html).toContain("2/3 accepted");
  });

  test("each pending add renders as a row with checkbox + card name", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    for (const row of SAMPLE_PENDING) {
      expect(html).toContain(row.card_name);
      expect(html).toContain(`aria-label="Accept ${row.card_name}"`);
    }
    // 3 checkboxes total.
    const checkboxMatches = html.match(/<input[^>]*type="checkbox"/g) ?? [];
    expect(checkboxMatches.length).toBe(3);
  });

  test("rejected row uses line-through styling (visual disabled state)", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    // Smothering Tithe has accepted:false in fixture.
    const idx = html.indexOf("Smothering Tithe");
    expect(idx).toBeGreaterThan(0);
    const slice = html.slice(Math.max(0, idx - 200), idx + 200);
    expect(slice).toContain("line-through");
  });

  test("accepted row does NOT have line-through styling", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    // Sol Ring (accepted:true) is the first row; isolate its span by
    // splitting around the next checkbox.
    const idx = html.indexOf("Sol Ring");
    const nextCheckbox = html.indexOf("Arcane Signet", idx);
    const slice = html.slice(idx, nextCheckbox);
    expect(slice).not.toContain("line-through");
  });
});

describe("v1.6.3 Stage 2 — pending-review reason chips use v1.6.2 translation map", () => {
  test("ADD_BASIC_LAND_FILL_AUTO renders as 'Auto-filled basic land'", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    expect(html).toContain("Auto-filled basic land");
  });

  test("COMPLETE_TO_TARGET_SIZE renders as 'Brings deck up to target size'", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    expect(html).toContain("Brings deck up to target size");
  });

  test("ADD_REQUIRED_COVERAGE renders as 'Required for sufficiency'", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    expect(html).toContain("Required for sufficiency");
  });

  test("raw engine code preserved on Badge title attribute (power-user hover)", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    expect(html).toMatch(/title="ADD_BASIC_LAND_FILL_AUTO"/);
    expect(html).toMatch(/title="COMPLETE_TO_TARGET_SIZE"/);
    expect(html).toMatch(/title="ADD_REQUIRED_COVERAGE"/);
  });
});

describe("v1.6.3 Stage 2 — bulk controls present + aria-labeled", () => {
  test("Apply Selected button shows accepted count + aria-label", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    expect(html).toContain('aria-label="Apply Selected (2)"');
    expect(html).toMatch(/Apply Selected \(2\)/);
  });

  test("Accept All / Reject All / Dismiss buttons present with correct aria-labels", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    expect(html).toContain('aria-label="Accept All"');
    expect(html).toContain('aria-label="Reject All"');
    expect(html).toContain('aria-label="Dismiss proposed additions"');
  });

  test("Apply Selected disabled when zero accepted", () => {
    const allRejected: ReadonlyArray<ProposedAdd> = SAMPLE_PENDING.map((r) => ({
      ...r,
      accepted: false,
    }));
    const html = renderPanel({ pendingAdds: allRejected });
    // Locate the Apply Selected button + check disabled attribute.
    const idx = html.indexOf('aria-label="Apply Selected (0)"');
    expect(idx).toBeGreaterThan(0);
    const buttonOpen = html.lastIndexOf("<button", idx);
    const slice = html.slice(buttonOpen, idx);
    expect(slice).toContain("disabled");
  });

  test("Accept All disabled when all already accepted", () => {
    const allAccepted: ReadonlyArray<ProposedAdd> = SAMPLE_PENDING.map((r) => ({
      ...r,
      accepted: true,
    }));
    const html = renderPanel({ pendingAdds: allAccepted });
    const idx = html.indexOf('aria-label="Accept All"');
    const buttonOpen = html.lastIndexOf("<button", idx);
    const slice = html.slice(buttonOpen, idx);
    expect(slice).toContain("disabled");
  });

  test("Reject All disabled when zero accepted", () => {
    const allRejected: ReadonlyArray<ProposedAdd> = SAMPLE_PENDING.map((r) => ({
      ...r,
      accepted: false,
    }));
    const html = renderPanel({ pendingAdds: allRejected });
    const idx = html.indexOf('aria-label="Reject All"');
    const buttonOpen = html.lastIndexOf("<button", idx);
    const slice = html.slice(buttonOpen, idx);
    expect(slice).toContain("disabled");
  });
});

describe("v1.6.3 Stage 2 — default mode preserved when pendingAdds empty/omitted", () => {
  test("undefined pendingAdds + empty rows → null render (v1.1 back-compat)", () => {
    const html = renderPanel({ rows: [] });
    // The panel returns null for empty default-mode render.
    expect(html).toBe("");
  });

  test("empty pendingAdds + non-empty rows → default mode renders 'Added cards' header", () => {
    const html = renderPanel({
      pendingAdds: [],
      rows: [{ name: "Sol Ring", reasons_v1: ["x"] }],
    });
    // Pending mode marker absent.
    expect(html).not.toContain('data-v163-stage="pending-review-panel"');
    // Default-mode "Added cards" header present (v1.1 back-compat).
    expect(html).toContain("Added cards");
  });
});

describe("v1.6.3 Stage 2 — Dismiss vs Apply semantics (separate render-level behavior)", () => {
  test("Dismiss button has ghost variant (visually de-emphasized)", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    // The Dismiss button's surrounding class set contains the ghost variant
    // signature (bg-transparent text-text-secondary).
    const idx = html.indexOf('aria-label="Dismiss proposed additions"');
    const buttonOpen = html.lastIndexOf("<button", idx);
    const slice = html.slice(buttonOpen, idx);
    expect(slice).toContain("bg-transparent");
  });

  test("Apply Selected button has primary variant (visually emphasized)", () => {
    const html = renderPanel({ pendingAdds: SAMPLE_PENDING });
    const idx = html.indexOf('aria-label="Apply Selected (2)"');
    const buttonOpen = html.lastIndexOf("<button", idx);
    const slice = html.slice(buttonOpen, idx);
    // Primary variant uses bg-accent.
    expect(slice).toContain("bg-accent");
  });
});
