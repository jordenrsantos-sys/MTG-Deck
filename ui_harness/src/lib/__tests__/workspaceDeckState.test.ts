/**
 * Vitest tests for lib/workspaceDeckState (Phase 4.13.2).
 *
 * Reducer is pure — tests exercise it directly with no React mounting.
 * Coverage targets every action + idempotency + USER_EDIT source-upgrade
 * edge cases + BUILD_ERROR buildResponse preservation + RESET + the
 * full-lifecycle persistence-guard test (the bug fix this refactor
 * exists to deliver).
 */
import { describe, expect, test } from "vitest";
import {
  deckReducer,
  INITIAL_STATE,
  type ActiveDeckState,
  type BuildResponseShape,
  type DeckSource,
} from "../workspaceDeckState";

const HYDRATED_BASE: ActiveDeckState = {
  ...INITIAL_STATE,
  isHydrated: true,
};

function _afterHydrate(source: DeckSource = "archidekt"): ActiveDeckState {
  return deckReducer(INITIAL_STATE, {
    type: "HYDRATE_FROM_IMPORT_SLOT",
    commander: "Shelob, Child of Ungoliant",
    decklist: "1 Sol Ring\n1 Goblin Matron",
    source,
  });
}

describe("INITIAL_STATE shape", () => {
  test("commander/deckText/source/isHydrated/buildResponse defaults", () => {
    expect(INITIAL_STATE.commander).toBe("Krenko, Mob Boss");
    expect(INITIAL_STATE.deckText.length).toBeGreaterThan(0);
    expect(INITIAL_STATE.source).toBe("fallback");
    expect(INITIAL_STATE.isHydrated).toBe(false);
    expect(INITIAL_STATE.buildResponse).toBeNull();
    expect(INITIAL_STATE.buildPending).toBe(false);
    expect(INITIAL_STATE.buildError).toBeNull();
    expect(INITIAL_STATE.deckTextRevision).toBe(0);
  });
});

describe("HYDRATE_FROM_IMPORT_SLOT", () => {
  test("populates commander+deckText, sets source, flips isHydrated", () => {
    const next = _afterHydrate("archidekt");
    expect(next.commander).toBe("Shelob, Child of Ungoliant");
    expect(next.deckText).toContain("Sol Ring");
    expect(next.source).toBe("archidekt");
    expect(next.isHydrated).toBe(true);
    expect(next.deckTextRevision).toBe(1);
  });

  test("idempotent — second dispatch returns identical state (StrictMode defense)", () => {
    const after1 = _afterHydrate("archidekt");
    const after2 = deckReducer(after1, {
      type: "HYDRATE_FROM_IMPORT_SLOT",
      commander: "Different Commander",
      decklist: "1 Different Card",
      source: "arena_text",
    });
    expect(after2).toBe(after1);
    expect(after2.commander).toBe("Shelob, Child of Ungoliant");
    expect(after2.source).toBe("archidekt");
  });

  test("accepts every parser-registry source", () => {
    const sources: DeckSource[] = ["archidekt", "arena_text", "mtgo_text", "plain_text", "file_upload"];
    for (const src of sources) {
      const next = deckReducer(INITIAL_STATE, {
        type: "HYDRATE_FROM_IMPORT_SLOT",
        commander: "X",
        decklist: "1 Y",
        source: src,
      });
      expect(next.source).toBe(src);
    }
  });
});

describe("HYDRATE_FROM_ACTIVE_SLOT", () => {
  test("populates from active-deck slot with the slot's source label", () => {
    const next = deckReducer(INITIAL_STATE, {
      type: "HYDRATE_FROM_ACTIVE_SLOT",
      commander: "Atraxa",
      decklist: "1 Sol Ring",
      source: "saved_deck",
    });
    expect(next.commander).toBe("Atraxa");
    expect(next.source).toBe("saved_deck");
    expect(next.isHydrated).toBe(true);
  });

  test("idempotent — second dispatch returns identical state", () => {
    const after1 = deckReducer(INITIAL_STATE, {
      type: "HYDRATE_FROM_ACTIVE_SLOT",
      commander: "A",
      decklist: "1 X",
      source: "manual",
    });
    const after2 = deckReducer(after1, {
      type: "HYDRATE_FROM_ACTIVE_SLOT",
      commander: "B",
      decklist: "1 Y",
      source: "build",
    });
    expect(after2).toBe(after1);
  });
});

describe("HYDRATE_NO_SOURCE", () => {
  test("flips isHydrated, preserves fallback values", () => {
    const next = deckReducer(INITIAL_STATE, { type: "HYDRATE_NO_SOURCE" });
    expect(next.isHydrated).toBe(true);
    expect(next.source).toBe("fallback");
    expect(next.commander).toBe(INITIAL_STATE.commander);
    expect(next.deckText).toBe(INITIAL_STATE.deckText);
  });

  test("idempotent — second dispatch returns identical state", () => {
    const after1 = deckReducer(INITIAL_STATE, { type: "HYDRATE_NO_SOURCE" });
    const after2 = deckReducer(after1, { type: "HYDRATE_NO_SOURCE" });
    expect(after2).toBe(after1);
  });
});

describe("USER_EDIT_COMMANDER", () => {
  test("upgrades source from fallback → manual on first user edit", () => {
    const hydrated = deckReducer(INITIAL_STATE, { type: "HYDRATE_NO_SOURCE" });
    const next = deckReducer(hydrated, { type: "USER_EDIT_COMMANDER", commander: "Atraxa" });
    expect(next.commander).toBe("Atraxa");
    expect(next.source).toBe("manual");
  });

  test("preserves real source when present (archidekt remains archidekt)", () => {
    const hydrated = _afterHydrate("archidekt");
    const next = deckReducer(hydrated, { type: "USER_EDIT_COMMANDER", commander: "Replaced" });
    expect(next.commander).toBe("Replaced");
    expect(next.source).toBe("archidekt");
  });

  test("preserves real source — saved_deck remains saved_deck", () => {
    const hydrated = _afterHydrate("saved_deck");
    const next = deckReducer(hydrated, { type: "USER_EDIT_COMMANDER", commander: "X" });
    expect(next.source).toBe("saved_deck");
  });

  test("clears buildResponse on commander change (stale)", () => {
    const withBuild: ActiveDeckState = { ...HYDRATED_BASE, buildResponse: { foo: 1 } };
    const next = deckReducer(withBuild, { type: "USER_EDIT_COMMANDER", commander: "Y" });
    expect(next.buildResponse).toBeNull();
  });

  test("noop when commander unchanged", () => {
    const hydrated = _afterHydrate("archidekt");
    const next = deckReducer(hydrated, { type: "USER_EDIT_COMMANDER", commander: hydrated.commander });
    expect(next).toBe(hydrated);
  });
});

describe("USER_EDIT_DECK_TEXT", () => {
  test("upgrades source from fallback → manual on first user edit", () => {
    const hydrated = deckReducer(INITIAL_STATE, { type: "HYDRATE_NO_SOURCE" });
    const next = deckReducer(hydrated, { type: "USER_EDIT_DECK_TEXT", deckText: "1 Plains" });
    expect(next.source).toBe("manual");
    expect(next.deckTextRevision).toBe(hydrated.deckTextRevision + 1);
  });

  test("preserves real source when present", () => {
    const hydrated = _afterHydrate("arena_text");
    const next = deckReducer(hydrated, { type: "USER_EDIT_DECK_TEXT", deckText: "1 Mountain" });
    expect(next.source).toBe("arena_text");
  });

  test("bumps deckTextRevision on every text change", () => {
    let s = HYDRATED_BASE;
    s = deckReducer(s, { type: "USER_EDIT_DECK_TEXT", deckText: "a" });
    s = deckReducer(s, { type: "USER_EDIT_DECK_TEXT", deckText: "b" });
    s = deckReducer(s, { type: "USER_EDIT_DECK_TEXT", deckText: "c" });
    expect(s.deckTextRevision).toBe(HYDRATED_BASE.deckTextRevision + 3);
  });

  test("clears buildResponse on deckText change", () => {
    const withBuild: ActiveDeckState = { ...HYDRATED_BASE, buildResponse: { foo: 1 } };
    const next = deckReducer(withBuild, { type: "USER_EDIT_DECK_TEXT", deckText: "1 Forest" });
    expect(next.buildResponse).toBeNull();
  });

  test("noop when deckText unchanged", () => {
    const hydrated = _afterHydrate("archidekt");
    const next = deckReducer(hydrated, { type: "USER_EDIT_DECK_TEXT", deckText: hydrated.deckText });
    expect(next).toBe(hydrated);
  });
});

describe("LOAD_SAVED_DECK", () => {
  test("replaces commander+deckText, sets source=saved_deck, clears build", () => {
    const withBuild: ActiveDeckState = { ...HYDRATED_BASE, buildResponse: { foo: 1 }, buildError: "old" };
    const next = deckReducer(withBuild, {
      type: "LOAD_SAVED_DECK",
      commander: "Krenko",
      decklist: "1 Goblin Matron",
    });
    expect(next.commander).toBe("Krenko");
    expect(next.deckText).toBe("1 Goblin Matron");
    expect(next.source).toBe("saved_deck");
    expect(next.buildResponse).toBeNull();
    expect(next.buildError).toBeNull();
    expect(next.deckTextRevision).toBe(withBuild.deckTextRevision + 1);
  });
});

describe("BUILD_PENDING / BUILD_SUCCESS / BUILD_ERROR / CLEAR_BUILD_RESPONSE", () => {
  test("BUILD_PENDING flips buildPending, clears buildError", () => {
    const withErr: ActiveDeckState = { ...HYDRATED_BASE, buildError: "earlier" };
    const next = deckReducer(withErr, { type: "BUILD_PENDING" });
    expect(next.buildPending).toBe(true);
    expect(next.buildError).toBeNull();
  });

  test("BUILD_SUCCESS sets buildResponse, flips pending=false, upgrades fallback→build", () => {
    const hydrated = deckReducer(INITIAL_STATE, { type: "HYDRATE_NO_SOURCE" });
    const pending = deckReducer(hydrated, { type: "BUILD_PENDING" });
    const response: BuildResponseShape = { result: { sufficiency_summary_v1: { status: "PASS" } } };
    const next = deckReducer(pending, { type: "BUILD_SUCCESS", response });
    expect(next.buildResponse).toEqual(response);
    expect(next.buildPending).toBe(false);
    expect(next.source).toBe("build");
  });

  test("BUILD_SUCCESS preserves real source (archidekt stays archidekt)", () => {
    const hydrated = _afterHydrate("archidekt");
    const next = deckReducer(hydrated, { type: "BUILD_SUCCESS", response: { x: 1 } });
    expect(next.source).toBe("archidekt");
    expect(next.buildResponse).toEqual({ x: 1 });
  });

  test("BUILD_ERROR PRESERVES prior buildResponse (transient error)", () => {
    const withBuild: ActiveDeckState = { ...HYDRATED_BASE, buildResponse: { prior: true } };
    const pending = deckReducer(withBuild, { type: "BUILD_PENDING" });
    const next = deckReducer(pending, { type: "BUILD_ERROR", error: "HTTP 500" });
    expect(next.buildResponse).toEqual({ prior: true });
    expect(next.buildPending).toBe(false);
    expect(next.buildError).toBe("HTTP 500");
  });

  test("CLEAR_BUILD_RESPONSE clears both buildResponse + buildError", () => {
    const withBuild: ActiveDeckState = { ...HYDRATED_BASE, buildResponse: { x: 1 }, buildError: "e" };
    const next = deckReducer(withBuild, { type: "CLEAR_BUILD_RESPONSE" });
    expect(next.buildResponse).toBeNull();
    expect(next.buildError).toBeNull();
  });

  test("CLEAR_BUILD_RESPONSE noop when nothing to clear", () => {
    const next = deckReducer(HYDRATED_BASE, { type: "CLEAR_BUILD_RESPONSE" });
    expect(next).toBe(HYDRATED_BASE);
  });
});

describe("RESET", () => {
  test("returns INITIAL_STATE", () => {
    const dirty = _afterHydrate("archidekt");
    const next = deckReducer(dirty, { type: "RESET" });
    expect(next).toBe(INITIAL_STATE);
  });
});

describe("UPGRADE_PENDING / UPGRADE_SUCCESS / UPGRADE_ERROR / CLEAR_UPGRADE_SUGGESTIONS — v1.1", () => {
  test("INITIAL_STATE includes Upgrade defaults", () => {
    expect(INITIAL_STATE.upgradePending).toBe(false);
    expect(INITIAL_STATE.upgradeSuggestions).toBeNull();
    expect(INITIAL_STATE.upgradeError).toBeNull();
    expect(INITIAL_STATE.lastUpgradedAt).toBeNull();
  });

  test("UPGRADE_PENDING flips upgradePending true, clears upgradeError, preserves prior suggestions", () => {
    const withPrior: ActiveDeckState = {
      ...HYDRATED_BASE,
      upgradeSuggestions: [{ cut_name: "old_cut", add_name: "old_add" }],
      upgradeError: "earlier",
    };
    const next = deckReducer(withPrior, { type: "UPGRADE_PENDING" });
    expect(next.upgradePending).toBe(true);
    expect(next.upgradeError).toBeNull();
    expect(next.upgradeSuggestions).toEqual([{ cut_name: "old_cut", add_name: "old_add" }]);
  });

  test("UPGRADE_SUCCESS captures suggestions verbatim + sets lastUpgradedAt + clears pending", () => {
    const suggestions = [
      { cut_name: "Forest", add_name: "Sol Ring", reasons_v1: ["mana_acceleration"] },
      { cut_name: "Plains", add_name: "Arcane Signet" },
    ];
    const pending = deckReducer(HYDRATED_BASE, { type: "UPGRADE_PENDING" });
    const next = deckReducer(pending, {
      type: "UPGRADE_SUCCESS",
      suggestions,
      nowIso: "2026-05-10T18:00:00Z",
    });
    expect(next.upgradePending).toBe(false);
    expect(next.upgradeError).toBeNull();
    expect(next.upgradeSuggestions).toEqual(suggestions);
    expect(next.lastUpgradedAt).toBe("2026-05-10T18:00:00Z");
  });

  test("UPGRADE_SUCCESS without nowIso defaults to empty string (reducer is pure — no Date.now)", () => {
    const next = deckReducer(HYDRATED_BASE, {
      type: "UPGRADE_SUCCESS",
      suggestions: [{ cut_name: "X", add_name: "Y" }],
    });
    expect(next.lastUpgradedAt).toBe("");
  });

  test("UPGRADE_ERROR PRESERVES prior upgradeSuggestions (transient error)", () => {
    const withPrior: ActiveDeckState = {
      ...HYDRATED_BASE,
      upgradeSuggestions: [{ cut_name: "Forest", add_name: "Sol Ring" }],
    };
    const pending = deckReducer(withPrior, { type: "UPGRADE_PENDING" });
    const next = deckReducer(pending, { type: "UPGRADE_ERROR", error: "HTTP 500" });
    expect(next.upgradePending).toBe(false);
    expect(next.upgradeError).toBe("HTTP 500");
    expect(next.upgradeSuggestions).toEqual([{ cut_name: "Forest", add_name: "Sol Ring" }]);
  });

  test("CLEAR_UPGRADE_SUGGESTIONS clears both suggestions + error", () => {
    const withPrior: ActiveDeckState = {
      ...HYDRATED_BASE,
      upgradeSuggestions: [{ cut_name: "X", add_name: "Y" }],
      upgradeError: "stale",
    };
    const next = deckReducer(withPrior, { type: "CLEAR_UPGRADE_SUGGESTIONS" });
    expect(next.upgradeSuggestions).toBeNull();
    expect(next.upgradeError).toBeNull();
  });

  test("CLEAR_UPGRADE_SUGGESTIONS noop when nothing to clear", () => {
    const next = deckReducer(HYDRATED_BASE, { type: "CLEAR_UPGRADE_SUGGESTIONS" });
    expect(next).toBe(HYDRATED_BASE);
  });

  test("USER_EDIT_DECK_TEXT invalidates stale upgradeSuggestions", () => {
    const withPrior: ActiveDeckState = {
      ...HYDRATED_BASE,
      upgradeSuggestions: [{ cut_name: "X", add_name: "Y" }],
    };
    const next = deckReducer(withPrior, { type: "USER_EDIT_DECK_TEXT", deckText: "1 New Card" });
    expect(next.upgradeSuggestions).toBeNull();
  });

  test("USER_EDIT_COMMANDER invalidates stale upgradeSuggestions", () => {
    const withPrior: ActiveDeckState = {
      ...HYDRATED_BASE,
      upgradeSuggestions: [{ cut_name: "X", add_name: "Y" }],
    };
    const next = deckReducer(withPrior, { type: "USER_EDIT_COMMANDER", commander: "Different" });
    expect(next.upgradeSuggestions).toBeNull();
  });

  test("LOAD_SAVED_DECK invalidates stale upgradeSuggestions", () => {
    const withPrior: ActiveDeckState = {
      ...HYDRATED_BASE,
      upgradeSuggestions: [{ cut_name: "X", add_name: "Y" }],
    };
    const next = deckReducer(withPrior, { type: "LOAD_SAVED_DECK", commander: "Atraxa", decklist: "1 New" });
    expect(next.upgradeSuggestions).toBeNull();
  });

  test("RESET clears Upgrade state too", () => {
    const dirty: ActiveDeckState = {
      ...HYDRATED_BASE,
      upgradeSuggestions: [{ cut_name: "X", add_name: "Y" }],
      upgradePending: true,
      upgradeError: "e",
      lastUpgradedAt: "2026-05-10",
    };
    const next = deckReducer(dirty, { type: "RESET" });
    expect(next.upgradePending).toBe(false);
    expect(next.upgradeSuggestions).toBeNull();
    expect(next.upgradeError).toBeNull();
    expect(next.lastUpgradedAt).toBeNull();
  });

  test("UPGRADE_SUCCESS preserves source label (archidekt stays archidekt)", () => {
    const hydrated = _afterHydrate("archidekt");
    const next = deckReducer(hydrated, {
      type: "UPGRADE_SUCCESS",
      suggestions: [{ cut_name: "X", add_name: "Y" }],
    });
    expect(next.source).toBe("archidekt");
  });
});

describe("COMPLETE_PENDING / COMPLETE_SUCCESS / COMPLETE_ERROR — Phase 4.14", () => {
  test("INITIAL_STATE includes new fields with safe defaults", () => {
    expect(INITIAL_STATE.isCompleted).toBe(false);
    expect(INITIAL_STATE.completePending).toBe(false);
    expect(INITIAL_STATE.completeError).toBeNull();
  });

  test("COMPLETE_PENDING flips completePending true, clears completeError", () => {
    const withErr: ActiveDeckState = { ...HYDRATED_BASE, completeError: "earlier" };
    const next = deckReducer(withErr, { type: "COMPLETE_PENDING" });
    expect(next.completePending).toBe(true);
    expect(next.completeError).toBeNull();
  });

  test("COMPLETE_SUCCESS sets new deckText, bumps revision, isCompleted=true, clears buildResponse", () => {
    const hydrated = _afterHydrate("archidekt");
    const built = deckReducer(hydrated, { type: "BUILD_SUCCESS", response: { result: { ok: true } } });
    expect(built.buildResponse).not.toBeNull();
    const completed = deckReducer(built, {
      type: "COMPLETE_SUCCESS",
      deckText: "1 Sol Ring\n1 Webslinger\n... 99 cards",
    });
    expect(completed.deckText).toContain("99 cards");
    expect(completed.deckTextRevision).toBe(built.deckTextRevision + 1);
    expect(completed.isCompleted).toBe(true);
    expect(completed.completePending).toBe(false);
    expect(completed.completeError).toBeNull();
    expect(completed.buildResponse).toBeNull();
  });

  test("COMPLETE_SUCCESS preserves source (archidekt stays archidekt)", () => {
    const hydrated = _afterHydrate("archidekt");
    const completed = deckReducer(hydrated, { type: "COMPLETE_SUCCESS", deckText: "1 New Card" });
    expect(completed.source).toBe("archidekt");
  });

  test("COMPLETE_ERROR preserves prior deckText + buildResponse, clears pending", () => {
    const withBuild: ActiveDeckState = { ...HYDRATED_BASE, buildResponse: { keep: true } };
    const pending = deckReducer(withBuild, { type: "COMPLETE_PENDING" });
    const next = deckReducer(pending, { type: "COMPLETE_ERROR", error: "HTTP 500" });
    expect(next.deckText).toBe(withBuild.deckText);
    expect(next.buildResponse).toEqual({ keep: true });
    expect(next.completePending).toBe(false);
    expect(next.completeError).toBe("HTTP 500");
  });

  test("USER_EDIT_DECK_TEXT after COMPLETE_SUCCESS resets isCompleted (deck is now stale)", () => {
    const hydrated = _afterHydrate("archidekt");
    const completed = deckReducer(hydrated, { type: "COMPLETE_SUCCESS", deckText: "1 Plains" });
    expect(completed.isCompleted).toBe(true);
    const edited = deckReducer(completed, { type: "USER_EDIT_DECK_TEXT", deckText: "1 Plains\n1 Mountain" });
    expect(edited.isCompleted).toBe(false);
  });

  test("USER_EDIT_COMMANDER also resets isCompleted (different deck identity)", () => {
    const hydrated = _afterHydrate("archidekt");
    const completed = deckReducer(hydrated, { type: "COMPLETE_SUCCESS", deckText: "x" });
    const edited = deckReducer(completed, { type: "USER_EDIT_COMMANDER", commander: "Different Cmdr" });
    expect(edited.isCompleted).toBe(false);
  });

  test("LOAD_SAVED_DECK resets isCompleted (loading a different deck)", () => {
    const hydrated = _afterHydrate("archidekt");
    const completed = deckReducer(hydrated, { type: "COMPLETE_SUCCESS", deckText: "x" });
    const loaded = deckReducer(completed, { type: "LOAD_SAVED_DECK", commander: "Atraxa", decklist: "1 X" });
    expect(loaded.isCompleted).toBe(false);
  });

  test("RESET clears Complete state too", () => {
    const hydrated = _afterHydrate("archidekt");
    const completed = deckReducer(hydrated, { type: "COMPLETE_SUCCESS", deckText: "x" });
    const reset = deckReducer(completed, { type: "RESET" });
    expect(reset.isCompleted).toBe(false);
    expect(reset.completePending).toBe(false);
    expect(reset.completeError).toBeNull();
  });
});

describe("Persistence guard — full-lifecycle smoke test", () => {
  /**
   * The bug being eliminated: persistence-effect ordering let default
   * (fallback) state leak into the active-deck slot before hydration.
   *
   * The guard: persistence MUST only fire when isHydrated && source !== "fallback".
   *
   * Walk through the lifecycle and assert when the guard would allow vs
   * block a write. This is the canonical regression test for the entire
   * 4.13.2 architectural refactor.
   */
  function _shouldPersist(s: ActiveDeckState): boolean {
    return s.isHydrated && s.source !== "fallback";
  }

  test("INITIAL_STATE: blocked (not hydrated)", () => {
    expect(_shouldPersist(INITIAL_STATE)).toBe(false);
  });

  test("after HYDRATE_NO_SOURCE: blocked (source still fallback)", () => {
    const next = deckReducer(INITIAL_STATE, { type: "HYDRATE_NO_SOURCE" });
    expect(_shouldPersist(next)).toBe(false);
  });

  test("after HYDRATE_NO_SOURCE → USER_EDIT_DECK_TEXT: allowed (source upgraded to manual)", () => {
    let s: ActiveDeckState = INITIAL_STATE;
    s = deckReducer(s, { type: "HYDRATE_NO_SOURCE" });
    s = deckReducer(s, { type: "USER_EDIT_DECK_TEXT", deckText: "1 Mountain" });
    expect(_shouldPersist(s)).toBe(true);
    expect(s.source).toBe("manual");
  });

  test("after HYDRATE_FROM_IMPORT_SLOT: allowed immediately (source=archidekt)", () => {
    const s = _afterHydrate("archidekt");
    expect(_shouldPersist(s)).toBe(true);
  });

  test("StrictMode double-hydrate: SECOND HYDRATE no-op; first hydrate's source preserved", () => {
    let s: ActiveDeckState = INITIAL_STATE;
    // Real lifecycle: mount → hydrate from IMPORT (Shelob/archidekt) →
    // unmount → remount → hydrate AGAIN. Second pass must be a no-op so
    // a stale active-deck slot can't clobber the first hydration.
    s = deckReducer(s, {
      type: "HYDRATE_FROM_IMPORT_SLOT",
      commander: "Shelob, Child of Ungoliant",
      decklist: "1 Webslinger",
      source: "archidekt",
    });
    s = deckReducer(s, {
      type: "HYDRATE_FROM_ACTIVE_SLOT",
      commander: "Krenko, Mob Boss",
      decklist: "1 Goblin Matron",
      source: "manual",
    });
    expect(s.commander).toBe("Shelob, Child of Ungoliant");
    expect(s.source).toBe("archidekt");
    expect(_shouldPersist(s)).toBe(true);
  });
});

describe("v1.6.2 Stage 2 — USER_CLEAR_DECK reducer action (additive extension 11 → 12)", () => {
  test("from a real-deck state → returns INITIAL_STATE-shaped values but isHydrated:true", () => {
    // Start with a populated deck (hydrate then build to mimic real flow).
    let s: ActiveDeckState = INITIAL_STATE;
    s = deckReducer(s, {
      type: "HYDRATE_FROM_IMPORT_SLOT",
      commander: "Shelob, Child of Ungoliant",
      decklist: "1 Webslinger\n1 Sol Ring",
      source: "archidekt",
    });
    expect(s.commander).toBe("Shelob, Child of Ungoliant");
    expect(s.source).toBe("archidekt");

    // Clear.
    const cleared = deckReducer(s, { type: "USER_CLEAR_DECK" });

    // Mirrors INITIAL_STATE: commander = fallback Krenko, deckText = fallback,
    // source = "fallback", build/complete/upgrade state all reset.
    expect(cleared.commander).toBe(INITIAL_STATE.commander);
    expect(cleared.deckText).toBe(INITIAL_STATE.deckText);
    expect(cleared.source).toBe("fallback");
    expect(cleared.buildResponse).toBeNull();
    expect(cleared.buildPending).toBe(false);
    expect(cleared.buildError).toBeNull();
    expect(cleared.isCompleted).toBe(false);
    expect(cleared.completePending).toBe(false);
    expect(cleared.completeError).toBeNull();
    expect(cleared.upgradeSuggestions).toBeNull();
    expect(cleared.upgradeError).toBeNull();
    expect(cleared.upgradePending).toBe(false);

    // CRITICAL: isHydrated MUST stay true so the WorkspaceView hydration
    // useEffect doesn't immediately re-fire from the staged-import slot
    // and overwrite the user's explicit clear.
    expect(cleared.isHydrated).toBe(true);
  });

  test("from INITIAL_STATE (unhydrated) → same shape but isHydrated:true", () => {
    const cleared = deckReducer(INITIAL_STATE, { type: "USER_CLEAR_DECK" });
    expect(cleared.commander).toBe(INITIAL_STATE.commander);
    expect(cleared.deckText).toBe(INITIAL_STATE.deckText);
    expect(cleared.source).toBe("fallback");
    expect(cleared.isHydrated).toBe(true);
  });

  test("INITIAL_STATE itself unchanged (BYTE-IDENTICAL per HARD safety)", () => {
    // Defense-in-depth: confirm INITIAL_STATE wasn't mutated by the reducer
    // (the handler returns `{ ...INITIAL_STATE, isHydrated: true }`, not
    // a mutation in place).
    expect(INITIAL_STATE.isHydrated).toBe(false);
    expect(INITIAL_STATE.source).toBe("fallback");
    expect(INITIAL_STATE.commander).toBe("Krenko, Mob Boss");
  });

  test("after USER_CLEAR_DECK → source resets to 'fallback' (persistence-blocked semantic preserved)", () => {
    let s: ActiveDeckState = INITIAL_STATE;
    s = deckReducer(s, {
      type: "HYDRATE_FROM_IMPORT_SLOT",
      commander: "X",
      decklist: "1 Y",
      source: "archidekt",
    });
    s = deckReducer(s, { type: "USER_CLEAR_DECK" });
    // Source must reset to "fallback" so the persistence gate (which
    // checks source !== "fallback") blocks the prior deck from being
    // persisted. _shouldPersist is local to a different describe block;
    // we assert the underlying source state directly here.
    expect(s.source).toBe("fallback");
  });

  test("USER_CLEAR_DECK is a no-op when state is already at INITIAL_STATE shape (idempotent semantic)", () => {
    const cleared1 = deckReducer(INITIAL_STATE, { type: "USER_CLEAR_DECK" });
    const cleared2 = deckReducer(cleared1, { type: "USER_CLEAR_DECK" });
    // Both reducer passes produce the same SHAPE (modulo deckTextRevision
    // counter, which v1.6.2 USER_CLEAR_DECK doesn't increment — it returns
    // INITIAL_STATE wholesale).
    expect(cleared2.commander).toBe(cleared1.commander);
    expect(cleared2.deckText).toBe(cleared1.deckText);
    expect(cleared2.source).toBe(cleared1.source);
    expect(cleared2.isHydrated).toBe(cleared1.isHydrated);
  });

  test("after USER_CLEAR_DECK → USER_EDIT_DECK_TEXT: source upgraded to 'manual'", () => {
    let s: ActiveDeckState = INITIAL_STATE;
    s = deckReducer(s, {
      type: "HYDRATE_FROM_IMPORT_SLOT",
      commander: "X",
      decklist: "1 Y",
      source: "archidekt",
    });
    s = deckReducer(s, { type: "USER_CLEAR_DECK" });
    s = deckReducer(s, { type: "USER_EDIT_DECK_TEXT", deckText: "1 Mountain" });
    expect(s.source).toBe("manual");
    expect(s.deckText).toBe("1 Mountain");
  });
});
