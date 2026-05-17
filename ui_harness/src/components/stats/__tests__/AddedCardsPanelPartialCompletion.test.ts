/**
 * Vitest tests for v1.7 Stage 1 — AddedCardsPanel partial-completion mode.
 *
 * The engine returns `added_cards_v1: []` whenever `complete_status: "ERROR"`
 * is set (e.g., when `baseline_unknowns_v1` is non-empty), even though
 * `completed_decklist_text_v1` IS populated with the full completed deck.
 * v1.3's WorkspaceView-level diff fallback only fires when WorkspaceView
 * computes it explicitly. v1.7 Stage 1 surfaces the same diff at the
 * AddedCardsPanel boundary so callers that mount the panel directly with
 * `deckText` + `completedDecklistText` props get a "partial completion"
 * render with a per-card "Engine completed deck — reason details unavailable"
 * chip + an "Apply All" button.
 *
 * Mode resolution (post-v1.7):
 *   - pendingAdds.length > 0       → pending-review mode (v1.6.3)
 *   - rows.length > 0              → default applied mode (v1.1 / v1.3)
 *   - completedDecklistText differs from deckText AND diff > 0
 *                                  → NEW partial-completion mode (v1.7 Stage 1)
 *   - otherwise                    → hidden
 *
 * Per AUTOMATION_RULES halt-and-ask + addedCardsPanel.test.ts precedent:
 * no @testing-library/react install; tests use react-dom/server.renderToString.
 *
 * fail-then-pass discipline: this file is authored BEFORE the panel
 * implementation; it MUST fail on the v1.6.4 baseline (panel returns null
 * with empty rows + empty pendingAdds regardless of deckText props).
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import AddedCardsPanel from "../AddedCardsPanel";

const DECK_TEXT = "1 Krenko, Mob Boss\n10 Mountain";
const COMPLETED_TEXT =
  "1 Krenko, Mob Boss\n10 Mountain\n1 Sol Ring\n1 Arcane Signet";

function renderPanel(opts: {
  deckText?: string;
  completedDecklistText?: string;
  onApplyAllPartial?: () => void;
}): string {
  const raw = renderToString(
    React.createElement(AddedCardsPanel, {
      rows: [],
      pendingAdds: [],
      deckText: opts.deckText,
      completedDecklistText: opts.completedDecklistText,
      onApplyAllPartial: opts.onApplyAllPartial,
    } as React.ComponentProps<typeof AddedCardsPanel>),
  );
  return raw.replace(/<!-- -->/g, "");
}

describe("v1.7 Stage 1 — partial-completion mode render", () => {
  test("non-empty diff → partial-completion panel renders", () => {
    const html = renderPanel({
      deckText: DECK_TEXT,
      completedDecklistText: COMPLETED_TEXT,
    });
    expect(html).toContain('data-v17-stage="partial-completion-panel"');
    expect(html).toContain("Partial completion");
  });

  test("each diff card renders as a row with the fallback reason chip", () => {
    const html = renderPanel({
      deckText: DECK_TEXT,
      completedDecklistText: COMPLETED_TEXT,
    });
    expect(html).toContain("Sol Ring");
    expect(html).toContain("Arcane Signet");
    // Fallback reason chip text appears at least once per diff card.
    const matches = html.match(/Engine completed deck — reason details unavailable/g) ?? [];
    expect(matches.length).toBeGreaterThanOrEqual(2);
  });

  test("count badge reports the diff card count (2)", () => {
    const html = renderPanel({
      deckText: DECK_TEXT,
      completedDecklistText: COMPLETED_TEXT,
    });
    // 2 diff cards.
    expect(html).toMatch(/>2</);
  });

  test("Apply All button wires to onApplyAllPartial callback", () => {
    const html = renderPanel({
      deckText: DECK_TEXT,
      completedDecklistText: COMPLETED_TEXT,
      onApplyAllPartial: () => undefined,
    });
    expect(html).toContain('aria-label="Apply All partial-completion additions"');
    // Apply All button is enabled when the callback is provided.
    const idx = html.indexOf('aria-label="Apply All partial-completion additions"');
    expect(idx).toBeGreaterThan(0);
    const buttonOpen = html.lastIndexOf("<button", idx);
    const slice = html.slice(buttonOpen, idx);
    expect(slice).not.toContain('disabled=""');
  });

  test("Apply All button disabled when no callback wired", () => {
    const html = renderPanel({
      deckText: DECK_TEXT,
      completedDecklistText: COMPLETED_TEXT,
      // onApplyAllPartial intentionally omitted.
    });
    const idx = html.indexOf('aria-label="Apply All partial-completion additions"');
    expect(idx).toBeGreaterThan(0);
    const buttonOpen = html.lastIndexOf("<button", idx);
    const slice = html.slice(buttonOpen, idx);
    expect(slice).toContain("disabled");
  });
});

describe("v1.7 Stage 1 — partial-completion mode resolution", () => {
  test("empty deckText + empty completedDecklistText → hidden", () => {
    const html = renderPanel({ deckText: "", completedDecklistText: "" });
    expect(html).toBe("");
  });

  test("identical deckText and completedDecklistText → hidden (zero diff)", () => {
    const html = renderPanel({
      deckText: DECK_TEXT,
      completedDecklistText: DECK_TEXT,
    });
    expect(html).toBe("");
  });

  test("undefined completedDecklistText → hidden", () => {
    const html = renderPanel({ deckText: DECK_TEXT });
    expect(html).toBe("");
  });
});

describe("v1.7 Stage 1 — existing modes preserved (back-compat)", () => {
  test("non-empty rows still trigger v1.1/v1.3 default applied mode, NOT partial-completion", () => {
    const raw = renderToString(
      React.createElement(AddedCardsPanel, {
        rows: [{ name: "Sol Ring", reasons_v1: ["x"] }],
        deckText: DECK_TEXT,
        completedDecklistText: COMPLETED_TEXT,
      } as React.ComponentProps<typeof AddedCardsPanel>),
    );
    const html = raw.replace(/<!-- -->/g, "");
    // Default-mode "Added cards" header present; partial-completion marker absent.
    expect(html).toContain("Added cards");
    expect(html).not.toContain('data-v17-stage="partial-completion-panel"');
  });

  test("non-empty pendingAdds still trigger v1.6.3 pending-review mode, NOT partial-completion", () => {
    const raw = renderToString(
      React.createElement(AddedCardsPanel, {
        rows: [],
        pendingAdds: [
          { card_name: "Sol Ring", reasons: ["x"], accepted: true },
        ],
        deckText: DECK_TEXT,
        completedDecklistText: COMPLETED_TEXT,
        onTogglePendingAdd: () => undefined,
        onApplyAccepted: () => undefined,
      } as React.ComponentProps<typeof AddedCardsPanel>),
    );
    const html = raw.replace(/<!-- -->/g, "");
    expect(html).toContain('data-v163-stage="pending-review-panel"');
    expect(html).not.toContain('data-v17-stage="partial-completion-panel"');
  });
});
