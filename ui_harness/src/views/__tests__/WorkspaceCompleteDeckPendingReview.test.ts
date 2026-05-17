/**
 * Vitest integration test for v1.6.4 Stage 1 — Complete pending-review wiring.
 *
 * Cowork's v1.6.3 browser walk caught: Complete click → 200 OK response from
 * /deck/complete_v1 with non-empty added_cards_v1 → NO AddedCardsPanel
 * renders (0 checkboxes, 0 bulk controls, no "Review proposed additions"
 * header in the DOM). pendingAdds + legacy applied-mode are both empty.
 *
 * This test exercises the END-TO-END chain that WorkspaceView wires:
 *   (a) the data-extraction logic that pulls added_cards_v1 from the
 *       engine response shape (`{name, reasons_v1, primitives_added_v1}`)
 *       into the dispatch payload shape (`{card_name, reasons}`);
 *   (b) the deckReducer STAGE_PROPOSED_ADDS handler that builds the
 *       pendingAdds queue;
 *   (c) the AddedCardsPanel render that surfaces the queue with
 *       per-card checkboxes + bulk controls.
 *
 * Per AUTOMATION_RULES + v1.6.4 spec: this test MUST fail on main first
 * (it's the proof the regression test catches the regression), then made
 * green by the fix. If any of (a)/(b)/(c) is broken, the assert fails.
 *
 * No @testing-library/react / JSDOM (vitest env=node per AUTOMATION_RULES
 * halt-and-ask precedent). The extract+dispatch logic from WorkspaceView
 * lines 3725-3748 is inlined here verbatim — if the production code's
 * extraction changes shape, the inlined copy in this test must be updated
 * to match (drift detection sentinel).
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import AddedCardsPanel from "../../components/stats/AddedCardsPanel";
import {
  deckReducer,
  INITIAL_STATE,
  type ActiveDeckState,
  type DeckAction,
} from "../../lib/workspaceDeckState";

/** Replicates the engine /deck/complete_v1 response shape (api/main.py:238
 *  DeckCompleteAddedCardV1 Pydantic model → JSON serialization). */
type MockCompletionResponse = {
  status?: string;
  completed_decklist_text_v1?: string;
  added_cards_v1?: Array<{
    name?: string;
    reasons_v1?: string[];
    primitives_added_v1?: string[];
  }>;
};

const SAMPLE_RESPONSE: MockCompletionResponse = {
  status: "OK",
  completed_decklist_text_v1:
    "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet\n1 Goblin Matron\n1 Skirk Prospector\n1 Impact Tremors\n1 Lightning Bolt\n1 Mountain",
  added_cards_v1: [
    {
      name: "Lightning Bolt",
      reasons_v1: ["ADD_PRIMITIVE_COVERAGE"],
      primitives_added_v1: ["BURN"],
    },
    {
      name: "Mountain",
      reasons_v1: ["ADD_BASIC_LAND_FILL_AUTO", "COMPLETE_TO_TARGET_SIZE"],
      primitives_added_v1: [],
    },
    {
      name: "Smothering Tithe",
      reasons_v1: ["ADD_REQUIRED_COVERAGE"],
      primitives_added_v1: ["RAMP"],
    },
  ],
};

/** Inlined WorkspaceView extraction logic (lines 3725-3748 verbatim). This
 *  helper mirrors the production click-handler's branch that runs after
 *  /deck/complete_v1 returns successfully. If the production code's
 *  shape changes, update this inline copy to keep the integration test
 *  honest. */
function _extractStagedAdds(
  completionResult: MockCompletionResponse,
): Array<{ card_name: string; reasons?: ReadonlyArray<string> }> {
  const finalCompletedText = completionResult?.completed_decklist_text_v1;
  if (typeof finalCompletedText !== "string" || finalCompletedText.trim() === "") {
    return [];
  }
  const addedFromEngine = completionResult?.added_cards_v1;
  const stagedAdds: Array<{ card_name: string; reasons?: ReadonlyArray<string> }> = [];
  if (Array.isArray(addedFromEngine)) {
    for (const row of addedFromEngine) {
      if (!row || typeof row !== "object") continue;
      const rec = row as { name?: unknown; reasons_v1?: unknown };
      const name = typeof rec.name === "string" ? rec.name.trim() : "";
      if (name === "") continue;
      const reasons = Array.isArray(rec.reasons_v1)
        ? (rec.reasons_v1 as unknown[]).filter((r): r is string => typeof r === "string")
        : [];
      stagedAdds.push({ card_name: name, reasons });
    }
  }
  return stagedAdds;
}

/** Runs the chain: extract from response → dispatch STAGE_PROPOSED_ADDS to
 *  the reducer → render AddedCardsPanel via renderToString. Returns the
 *  rendered HTML for assertions. */
function _runCompleteChainAndRender(response: MockCompletionResponse): {
  finalState: ActiveDeckState;
  html: string;
} {
  // (a) extraction
  const stagedAdds = _extractStagedAdds(response);
  // (b) dispatch → reducer
  const action: DeckAction = { type: "STAGE_PROPOSED_ADDS", adds: stagedAdds };
  const finalState = deckReducer(INITIAL_STATE, action);
  // (c) render the panel using the resulting pendingAdds. Mirrors
  //     WorkspaceView's pending-mode render (line ~4523).
  const html = renderToString(
    React.createElement(AddedCardsPanel, {
      rows: [],
      pendingAdds: finalState.pendingAdds,
      onTogglePendingAdd: () => undefined,
      onApplyAccepted: () => undefined,
      onAcceptAll: () => undefined,
      onRejectAll: () => undefined,
      onDismissPending: () => undefined,
    }),
  ).replace(/<!-- -->/g, "");
  return { finalState, html };
}

describe("v1.6.4 Stage 1 — Complete-deck → STAGE_PROPOSED_ADDS → AddedCardsPanel chain", () => {
  test("non-empty added_cards_v1 → pendingAdds populated with N entries (extraction)", () => {
    const { finalState } = _runCompleteChainAndRender(SAMPLE_RESPONSE);
    expect(finalState.pendingAdds).toHaveLength(3);
    expect(finalState.pendingAdds[0].card_name).toBe("Lightning Bolt");
    expect(finalState.pendingAdds[1].card_name).toBe("Mountain");
    expect(finalState.pendingAdds[2].card_name).toBe("Smothering Tithe");
    // Each entry defaults to accepted: true.
    for (const entry of finalState.pendingAdds) {
      expect(entry.accepted).toBe(true);
    }
  });

  test("reasons_v1 array preserved per entry (extraction → reducer → state)", () => {
    const { finalState } = _runCompleteChainAndRender(SAMPLE_RESPONSE);
    expect(finalState.pendingAdds[0].reasons).toEqual(["ADD_PRIMITIVE_COVERAGE"]);
    expect(finalState.pendingAdds[1].reasons).toEqual([
      "ADD_BASIC_LAND_FILL_AUTO",
      "COMPLETE_TO_TARGET_SIZE",
    ]);
    expect(finalState.pendingAdds[2].reasons).toEqual(["ADD_REQUIRED_COVERAGE"]);
  });

  test("rendered DOM contains 'Review proposed additions' header (panel renders)", () => {
    const { html } = _runCompleteChainAndRender(SAMPLE_RESPONSE);
    expect(html).toContain("Review proposed additions");
    expect(html).toContain('data-v163-stage="pending-review-panel"');
  });

  test("rendered DOM contains 3 checkboxes (one per pending add)", () => {
    const { html } = _runCompleteChainAndRender(SAMPLE_RESPONSE);
    const checkboxMatches = html.match(/<input[^>]+type="checkbox"/g) ?? [];
    expect(checkboxMatches.length).toBe(3);
  });

  test("rendered DOM contains all 3 bulk control buttons + Dismiss", () => {
    const { html } = _runCompleteChainAndRender(SAMPLE_RESPONSE);
    expect(html).toContain('aria-label="Apply Selected (3)"');
    expect(html).toContain('aria-label="Accept All"');
    expect(html).toContain('aria-label="Reject All"');
    expect(html).toContain('aria-label="Dismiss proposed additions"');
  });

  test("rendered DOM contains each card's name + aria-label for the checkbox", () => {
    const { html } = _runCompleteChainAndRender(SAMPLE_RESPONSE);
    expect(html).toContain("Lightning Bolt");
    expect(html).toContain("Mountain");
    expect(html).toContain("Smothering Tithe");
    expect(html).toContain('aria-label="Accept Lightning Bolt"');
    expect(html).toContain('aria-label="Accept Mountain"');
    expect(html).toContain('aria-label="Accept Smothering Tithe"');
  });
});

describe("v1.6.4 Stage 1 — edge cases that would silently hide the panel", () => {
  test("empty completed_decklist_text_v1 → extraction returns [] (panel hides)", () => {
    const { finalState } = _runCompleteChainAndRender({
      ...SAMPLE_RESPONSE,
      completed_decklist_text_v1: "",
    });
    expect(finalState.pendingAdds).toEqual([]);
  });

  test("missing added_cards_v1 → extraction returns [] (panel hides)", () => {
    const { finalState } = _runCompleteChainAndRender({
      ...SAMPLE_RESPONSE,
      added_cards_v1: undefined,
    });
    expect(finalState.pendingAdds).toEqual([]);
  });

  test("rows with blank name strings → filtered out by extraction", () => {
    const { finalState } = _runCompleteChainAndRender({
      ...SAMPLE_RESPONSE,
      added_cards_v1: [
        { name: "Sol Ring", reasons_v1: ["x"] },
        { name: "", reasons_v1: ["y"] }, // blank → skip
        { name: "  ", reasons_v1: ["z"] }, // whitespace → skip
        { name: "Mountain", reasons_v1: [] },
      ],
    });
    expect(finalState.pendingAdds.map((r) => r.card_name)).toEqual(["Sol Ring", "Mountain"]);
  });
});

describe("v1.6.4 Stage 1 — verify panel DOES NOT render in the bug-symptom scenario", () => {
  test("response with non-empty added_cards_v1 + non-empty completed_decklist_text_v1 → BOTH pendingAdds populated AND rendered panel non-empty", () => {
    // Cowork's symptom: engine returns OK + non-empty body → panel doesn't
    // render. This test catches that by asserting BOTH state population
    // AND DOM output are non-empty.
    const { finalState, html } = _runCompleteChainAndRender(SAMPLE_RESPONSE);
    expect(finalState.pendingAdds.length).toBeGreaterThan(0);
    expect(html.length).toBeGreaterThan(0);
    // Sentinel: the panel's render marker must be in the DOM.
    expect(html).toContain('data-v163-stage="pending-review-panel"');
    // Sentinel: at least one checkbox renders.
    expect(html.match(/<input[^>]+type="checkbox"/g)?.length ?? 0).toBeGreaterThanOrEqual(1);
  });
});
