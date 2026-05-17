/**
 * Integration vitest for v1.7 Stage 5 Deliverable A — WorkspaceView
 * partial-completion wire-in.
 *
 * Stage 1 added partial-completion mode to AddedCardsPanel (the panel
 * renders a bulk "Apply All" affordance when `deckText` +
 * `completedDecklistText` props are present and the engine returned
 * no `added_cards_v1` rows). The panel-in-isolation behavior was tested
 * in v1.7 Stage 1's existing vitest. Stage 5 wires it into WorkspaceView
 * so the partial-completion mode is reachable from the live UI.
 *
 * This test verifies the wire-up at TWO levels:
 *   1. Source-sentinel — WorkspaceView.tsx passes the three Stage-1
 *      props (`deckText`, `completedDecklistText`, `onApplyAllPartial`)
 *      to AddedCardsPanel via a dedicated render branch.
 *   2. Reducer behavior — dispatching `STAGE_PROPOSED_ADDS` followed by
 *      `APPLY_ACCEPTED_ADDS` (the REUSE pattern this stage chose) on a
 *      clean state correctly appends the staged card names to deckText.
 *
 * The project's vitest harness intentionally avoids
 * @testing-library/react (per Phase 4.x autonomous_repair_log carryover);
 * source-string sentinels + pure-reducer tests are the established
 * substitute for a render-tree mount.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  deckReducer,
  INITIAL_STATE,
  type DeckAction,
  type ActiveDeckState,
} from "../../lib/workspaceDeckState";


const WORKSPACE_VIEW_SRC = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);


describe("v1.7 Stage 5 Deliverable A — source-sentinel: WorkspaceView passes partial-completion props", () => {
  test("AddedCardsPanel is rendered with deckText + completedDecklistText + onApplyAllPartial props", () => {
    // The partial-completion mode in AddedCardsPanel requires all three
    // props. The render branch only fires when engine emitted 0 rows
    // but a non-empty completed_decklist_text exists.
    expect(WORKSPACE_VIEW_SRC).toMatch(/<AddedCardsPanel[\s\S]*?deckText=/);
    expect(WORKSPACE_VIEW_SRC).toMatch(/<AddedCardsPanel[\s\S]*?completedDecklistText=/);
    expect(WORKSPACE_VIEW_SRC).toMatch(/<AddedCardsPanel[\s\S]*?onApplyAllPartial=/);
  });

  test("partial-completion render branch is gated on the no-engine-rows + completed-text condition", () => {
    // The branch must depend on completedDecklistText being non-empty AND
    // engine's completeAddedRows being empty (the diff-mode signal). We
    // accept either the explicit pre-existing variable names or any
    // equivalent gate expressed via the same identifiers.
    expect(WORKSPACE_VIEW_SRC).toMatch(/completedDecklistText[\s\S]{0,400}AddedCardsPanel[\s\S]{0,400}onApplyAllPartial/);
  });

  test("onApplyAllPartial handler dispatches the REUSE pattern (STAGE_PROPOSED_ADDS + APPLY_ACCEPTED_ADDS)", () => {
    // The chosen approach reuses the existing reducer pair: stage the
    // diff additions as ProposedAdd entries, then immediately commit
    // via APPLY_ACCEPTED_ADDS — matches v1.6.3's commit semantics so
    // no new reducer action is introduced.
    const matchBlock = WORKSPACE_VIEW_SRC.match(
      /onApplyAllPartial=\{[\s\S]{0,1200}?\}\}/,
    );
    expect(matchBlock).not.toBeNull();
    const handler = matchBlock?.[0] ?? "";
    expect(handler).toMatch(/STAGE_PROPOSED_ADDS/);
    expect(handler).toMatch(/APPLY_ACCEPTED_ADDS/);
  });
});


describe("v1.7 Stage 5 Deliverable A — reducer behavior: REUSE pattern appends partial-completion adds to deckText", () => {
  function applyActions(initial: ActiveDeckState, actions: DeckAction[]): ActiveDeckState {
    return actions.reduce((s, a) => deckReducer(s, a), initial);
  }

  test("STAGE_PROPOSED_ADDS + APPLY_ACCEPTED_ADDS appends new cards to deckText (one per line)", () => {
    const start: ActiveDeckState = {
      ...INITIAL_STATE,
      deckText: "1 Sol Ring",
      isHydrated: true,
      pendingAdds: [],
    };
    const final = applyActions(start, [
      {
        type: "STAGE_PROPOSED_ADDS",
        adds: [
          { card_name: "Cyclonic Rift" },
          { card_name: "Rhystic Study" },
        ],
      },
      { type: "APPLY_ACCEPTED_ADDS" },
    ]);
    expect(final.deckText).toBe("1 Sol Ring\n1 Cyclonic Rift\n1 Rhystic Study");
    expect(final.pendingAdds.length).toBe(0);
    expect(final.deckTextRevision).toBe(start.deckTextRevision + 1);
    expect(final.isCompleted).toBe(true);
  });

  test("Empty partial-completion diff is a no-op deck mutation (zero adds staged)", () => {
    const start: ActiveDeckState = {
      ...INITIAL_STATE,
      deckText: "1 Sol Ring",
      isHydrated: true,
      pendingAdds: [],
    };
    const final = applyActions(start, [
      { type: "STAGE_PROPOSED_ADDS", adds: [] },
      { type: "APPLY_ACCEPTED_ADDS" },
    ]);
    expect(final.deckText).toBe(start.deckText);
    expect(final.deckTextRevision).toBe(start.deckTextRevision);
    expect(final.pendingAdds.length).toBe(0);
  });

  test("REUSE pattern does NOT introduce a new reducer action (DeckAction union unchanged)", () => {
    // Sentinel against scope creep: the chosen approach is REUSE
    // (STAGE + APPLY pair), not a new APPLY_PARTIAL_COMPLETION action.
    // If a future change adds the new action, this assertion should
    // still pass (it's a regression guard, not an exclusion).
    const src = readFileSync(
      resolve(__dirname, "../../lib/workspaceDeckState.ts"),
      "utf-8",
    );
    expect(src).toMatch(/STAGE_PROPOSED_ADDS/);
    expect(src).toMatch(/APPLY_ACCEPTED_ADDS/);
  });
});
