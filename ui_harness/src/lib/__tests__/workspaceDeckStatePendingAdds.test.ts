/**
 * Vitest tests for v1.6.3 Stage 2 — pending-adds reducer extension.
 *
 * DeckAction grew 12 → 16 with four new variants (STAGE_PROPOSED_ADDS /
 * TOGGLE_PROPOSED_ADD / APPLY_ACCEPTED_ADDS / DISMISS_PROPOSED_ADDS).
 * INITIAL_STATE gained EXACTLY ONE new field (`pendingAdds: []`); all
 * other field values BYTE-IDENTICAL from v1.6.2.
 *
 * Per HARD safety #2: reducer additive-only — these tests assert:
 *   - INITIAL_STATE shape sentinel (only ONE new key added; all existing
 *     keys present with their v1.6.2 values).
 *   - Each new action handler does what it claims (stage / toggle /
 *     apply with deckText mutation / dismiss without mutation).
 *   - All 12 existing action handlers still work BYTE-IDENTICAL — the
 *     existing 58 reducer tests in workspaceDeckState.test.ts continue
 *     to pass (verified via separate file).
 */
import { describe, expect, test } from "vitest";
import {
  deckReducer,
  INITIAL_STATE,
  type ActiveDeckState,
} from "../workspaceDeckState";

describe("v1.6.3 Stage 2 — INITIAL_STATE shape sentinel (pendingAdds: [] additive)", () => {
  test("pendingAdds field present + empty array default", () => {
    expect(INITIAL_STATE.pendingAdds).toEqual([]);
    expect(Array.isArray(INITIAL_STATE.pendingAdds)).toBe(true);
  });

  test("INITIAL_STATE preserves ALL v1.6.2 keys with their BYTE-IDENTICAL values", () => {
    // Sentinel: each key from v1.6.2 INITIAL_STATE still has its prior value.
    expect(INITIAL_STATE.commander).toBe("Krenko, Mob Boss");
    expect(INITIAL_STATE.deckText).toBe(
      "1 Sol Ring\n1 Arcane Signet\nGoblin Matron\nSkirk Prospector\nImpact Tremors",
    );
    expect(INITIAL_STATE.deckTextRevision).toBe(0);
    expect(INITIAL_STATE.source).toBe("fallback");
    expect(INITIAL_STATE.isHydrated).toBe(false);
    expect(INITIAL_STATE.buildResponse).toBeNull();
    expect(INITIAL_STATE.buildPending).toBe(false);
    expect(INITIAL_STATE.buildError).toBeNull();
    expect(INITIAL_STATE.isCompleted).toBe(false);
    expect(INITIAL_STATE.completePending).toBe(false);
    expect(INITIAL_STATE.completeError).toBeNull();
    expect(INITIAL_STATE.upgradePending).toBe(false);
    expect(INITIAL_STATE.upgradeSuggestions).toBeNull();
    expect(INITIAL_STATE.upgradeError).toBeNull();
    expect(INITIAL_STATE.lastUpgradedAt).toBeNull();
  });

  test("INITIAL_STATE has EXACTLY ONE new key compared to v1.6.2 (pendingAdds)", () => {
    const v162Keys = new Set([
      "commander", "deckText", "deckTextRevision", "source", "isHydrated",
      "buildResponse", "buildPending", "buildError",
      "isCompleted", "completePending", "completeError",
      "upgradePending", "upgradeSuggestions", "upgradeError", "lastUpgradedAt",
    ]);
    const currentKeys = Object.keys(INITIAL_STATE);
    const newKeys = currentKeys.filter((k) => !v162Keys.has(k));
    expect(newKeys).toEqual(["pendingAdds"]);
  });
});

describe("v1.6.3 Stage 2 — STAGE_PROPOSED_ADDS", () => {
  test("stages adds with accepted:true default", () => {
    const s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [
        { card_name: "Sol Ring", reasons: ["ADD_BASIC_LAND_FILL_AUTO"] },
        { card_name: "Arcane Signet", reasons: [] },
      ],
    });
    expect(s.pendingAdds).toHaveLength(2);
    expect(s.pendingAdds[0]).toEqual({
      card_name: "Sol Ring",
      reasons: ["ADD_BASIC_LAND_FILL_AUTO"],
      accepted: true,
    });
    expect(s.pendingAdds[1].accepted).toBe(true);
  });

  test("clears completePending + completeError when adds land", () => {
    let s: ActiveDeckState = {
      ...INITIAL_STATE,
      completePending: true,
      completeError: "prior error",
    };
    s = deckReducer(s, { type: "STAGE_PROPOSED_ADDS", adds: [{ card_name: "Sol Ring" }] });
    expect(s.completePending).toBe(false);
    expect(s.completeError).toBeNull();
  });

  test("replaces prior pendingAdds (second stage overrides first)", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    s = deckReducer(s, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Mountain" }, { card_name: "Forest" }],
    });
    expect(s.pendingAdds.map((r) => r.card_name)).toEqual(["Mountain", "Forest"]);
  });

  test("filters malformed entries (missing card_name / non-string)", () => {
    const s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [
        { card_name: "Sol Ring" },
        { card_name: "" }, // blank → skipped
        { card_name: "  " }, // whitespace → skipped
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        { card_name: 42 as any }, // non-string → skipped
        { card_name: "Arcane Signet", reasons: ["x", 7 as unknown as string, "y"] },
      ],
    });
    expect(s.pendingAdds.map((r) => r.card_name)).toEqual(["Sol Ring", "Arcane Signet"]);
    // reasons array filtered to strings only.
    expect(s.pendingAdds[1].reasons).toEqual(["x", "y"]);
  });
});

describe("v1.6.3 Stage 2 — TOGGLE_PROPOSED_ADD", () => {
  test("flips accepted flag for a valid index", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }, { card_name: "Mountain" }],
    });
    expect(s.pendingAdds[0].accepted).toBe(true);
    s = deckReducer(s, { type: "TOGGLE_PROPOSED_ADD", index: 0 });
    expect(s.pendingAdds[0].accepted).toBe(false);
    expect(s.pendingAdds[1].accepted).toBe(true); // unchanged
    s = deckReducer(s, { type: "TOGGLE_PROPOSED_ADD", index: 0 });
    expect(s.pendingAdds[0].accepted).toBe(true);
  });

  test("out-of-range index is a no-op (defensive)", () => {
    const s1 = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    const s2 = deckReducer(s1, { type: "TOGGLE_PROPOSED_ADD", index: 99 });
    expect(s2).toBe(s1); // referentially equal — no state change
  });

  test("negative index is a no-op", () => {
    const s1 = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    const s2 = deckReducer(s1, { type: "TOGGLE_PROPOSED_ADD", index: -1 });
    expect(s2).toBe(s1);
  });
});

describe("v1.6.3 Stage 2 — APPLY_ACCEPTED_ADDS", () => {
  test("appends accepted card_names to deckText (one per line)", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [
        { card_name: "Sol Ring", reasons: ["x"] },
        { card_name: "Mountain", reasons: ["y"] },
        { card_name: "Forest", reasons: ["z"] },
      ],
    });
    // Reject "Mountain".
    s = deckReducer(s, { type: "TOGGLE_PROPOSED_ADD", index: 1 });
    s = deckReducer(s, { type: "APPLY_ACCEPTED_ADDS" });
    expect(s.deckText).toContain("1 Sol Ring");
    expect(s.deckText).toContain("1 Forest");
    expect(s.deckText).not.toContain("1 Mountain");
  });

  test("clears pendingAdds after apply", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    s = deckReducer(s, { type: "APPLY_ACCEPTED_ADDS" });
    expect(s.pendingAdds).toEqual([]);
  });

  test("upgrades source fallback → manual (matches USER_EDIT_DECK_TEXT semantics)", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    expect(s.source).toBe("fallback"); // INITIAL_STATE source
    s = deckReducer(s, { type: "APPLY_ACCEPTED_ADDS" });
    expect(s.source).toBe("manual");
  });

  test("preserves non-fallback source on apply (archidekt stays archidekt)", () => {
    let s: ActiveDeckState = INITIAL_STATE;
    s = deckReducer(s, {
      type: "HYDRATE_FROM_IMPORT_SLOT",
      commander: "Shelob",
      decklist: "1 Webslinger",
      source: "archidekt",
    });
    s = deckReducer(s, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    s = deckReducer(s, { type: "APPLY_ACCEPTED_ADDS" });
    expect(s.source).toBe("archidekt");
  });

  test("bumps deckTextRevision + sets isCompleted: true + clears build/upgrade state", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    const beforeRev = s.deckTextRevision;
    s = deckReducer(s, { type: "APPLY_ACCEPTED_ADDS" });
    expect(s.deckTextRevision).toBe(beforeRev + 1);
    expect(s.isCompleted).toBe(true);
    expect(s.buildResponse).toBeNull();
    expect(s.upgradeSuggestions).toBeNull();
  });

  test("zero accepted (user rejected all) → no deck mutation but pendingAdds clears", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }, { card_name: "Mountain" }],
    });
    s = deckReducer(s, { type: "TOGGLE_PROPOSED_ADD", index: 0 });
    s = deckReducer(s, { type: "TOGGLE_PROPOSED_ADD", index: 1 });
    const beforeDeck = s.deckText;
    s = deckReducer(s, { type: "APPLY_ACCEPTED_ADDS" });
    expect(s.deckText).toBe(beforeDeck); // unchanged
    expect(s.pendingAdds).toEqual([]);
  });

  test("apply on empty pendingAdds (defensive) → no-op", () => {
    const s = deckReducer(INITIAL_STATE, { type: "APPLY_ACCEPTED_ADDS" });
    expect(s.deckText).toBe(INITIAL_STATE.deckText);
    expect(s.pendingAdds).toEqual([]);
  });
});

describe("v1.6.3 Stage 2 — DISMISS_PROPOSED_ADDS", () => {
  test("clears pendingAdds without mutating deckText", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }, { card_name: "Mountain" }],
    });
    const beforeDeck = s.deckText;
    const beforeRev = s.deckTextRevision;
    s = deckReducer(s, { type: "DISMISS_PROPOSED_ADDS" });
    expect(s.pendingAdds).toEqual([]);
    expect(s.deckText).toBe(beforeDeck);
    expect(s.deckTextRevision).toBe(beforeRev);
  });

  test("dismiss with empty queue is a no-op (referentially stable)", () => {
    const s = deckReducer(INITIAL_STATE, { type: "DISMISS_PROPOSED_ADDS" });
    expect(s).toBe(INITIAL_STATE);
  });

  test("dismiss preserves source / isCompleted / buildResponse (no side effects)", () => {
    let s: ActiveDeckState = INITIAL_STATE;
    s = deckReducer(s, {
      type: "HYDRATE_FROM_IMPORT_SLOT",
      commander: "X",
      decklist: "1 Y",
      source: "archidekt",
    });
    s = deckReducer(s, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    s = deckReducer(s, { type: "DISMISS_PROPOSED_ADDS" });
    expect(s.source).toBe("archidekt"); // unchanged
    expect(s.isCompleted).toBe(false);
    expect(s.buildResponse).toBeNull();
  });
});

describe("v1.6.3 Stage 2 — interaction with existing v1.6.2 actions", () => {
  test("USER_CLEAR_DECK resets pendingAdds (via INITIAL_STATE spread)", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    expect(s.pendingAdds).toHaveLength(1);
    s = deckReducer(s, { type: "USER_CLEAR_DECK" });
    expect(s.pendingAdds).toEqual([]);
  });

  test("RESET resets pendingAdds (returns INITIAL_STATE)", () => {
    let s = deckReducer(INITIAL_STATE, {
      type: "STAGE_PROPOSED_ADDS",
      adds: [{ card_name: "Sol Ring" }],
    });
    s = deckReducer(s, { type: "RESET" });
    expect(s).toBe(INITIAL_STATE);
    expect(s.pendingAdds).toEqual([]);
  });
});
