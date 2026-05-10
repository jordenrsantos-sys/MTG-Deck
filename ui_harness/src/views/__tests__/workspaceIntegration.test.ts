/**
 * Vitest tests for the WorkspaceView integration adapters
 * (Phase 4 BUNDLE Integration / 4.13).
 *
 * Per AUTOMATION_RULES halt-and-ask + autonomous_repair_log soft-safety
 * #7: rendering full WorkspaceView would require @testing-library/react +
 * jsdom (HARD halt territory). The regression-prevention contract is
 * therefore expressed at the logic level: given a BuildResponse, do the
 * extracted wiring helpers correctly project to component props + decide
 * which panels render?
 */
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
  ACTIVE_DECK_STORAGE_KEY,
  buildActiveDeckPayload,
  extractCommanderRecommendation,
  extractSufficiencySummary,
  extractSwapSuggestions,
  shouldShowCommanderRecommendation,
  shouldShowSufficiencyDashboard,
  shouldShowSwapSuggestions,
} from "../workspaceIntegrationAdapters";
import type { BuildResponsePayload } from "../../components/workspaceTypes";

function mkBuildResponse(result: Record<string, unknown>): BuildResponsePayload {
  return { result } as unknown as BuildResponsePayload;
}

describe("extractSufficiencySummary", () => {
  test("null/empty BuildResponse → null", () => {
    expect(extractSufficiencySummary(null)).toBeNull();
    expect(extractSufficiencySummary(undefined)).toBeNull();
    expect(extractSufficiencySummary(mkBuildResponse({}))).toBeNull();
  });

  test("present sufficiency summary returned verbatim", () => {
    const summary = {
      version: "v1",
      status: "WARN" as const,
      domain_verdicts: { probability: { status: "PASS" as const, codes: [] } },
    };
    const out = extractSufficiencySummary(mkBuildResponse({ sufficiency_summary_v1: summary }));
    expect(out).toEqual(summary);
  });

  test("non-object sufficiency_summary_v1 → null (defensive)", () => {
    expect(extractSufficiencySummary(mkBuildResponse({ sufficiency_summary_v1: "bad" }))).toBeNull();
  });
});

describe("extractSwapSuggestions", () => {
  test("returns [] when seed_to_deck_v1 absent", () => {
    expect(extractSwapSuggestions(mkBuildResponse({}))).toEqual([]);
    expect(extractSwapSuggestions(null)).toEqual([]);
  });

  test("returns the swap_suggestions array verbatim", () => {
    const swaps = [
      { out: "Plains", in: "Sol Ring", rationale_code: "MANA_FIX" },
      { out: "Forest", in: "Llanowar Elves" },
    ];
    const out = extractSwapSuggestions(
      mkBuildResponse({ seed_to_deck_v1: { swap_suggestions: swaps } }),
    );
    expect(out).toEqual(swaps);
  });

  test("non-array swap_suggestions → []", () => {
    expect(
      extractSwapSuggestions(mkBuildResponse({ seed_to_deck_v1: { swap_suggestions: "garbage" } })),
    ).toEqual([]);
  });
});

describe("extractCommanderRecommendation", () => {
  test("null/empty → null", () => {
    expect(extractCommanderRecommendation(null)).toBeNull();
    expect(extractCommanderRecommendation(mkBuildResponse({}))).toBeNull();
  });

  test("present recommendation returned verbatim", () => {
    const rec = { version: "v1", status: "OK" as const, top_n: 10, candidates: [] };
    expect(
      extractCommanderRecommendation(mkBuildResponse({ commander_recommendation_v1: rec })),
    ).toEqual(rec);
  });
});

describe("shouldShowSufficiencyDashboard", () => {
  test("false when no summary present", () => {
    expect(shouldShowSufficiencyDashboard(null)).toBe(false);
    expect(shouldShowSufficiencyDashboard(mkBuildResponse({}))).toBe(false);
  });
  test("true when summary present", () => {
    expect(
      shouldShowSufficiencyDashboard(
        mkBuildResponse({ sufficiency_summary_v1: { status: "PASS" } }),
      ),
    ).toBe(true);
  });
});

describe("shouldShowSwapSuggestions", () => {
  test("true when any domain shows FAIL", () => {
    const buildResponse = mkBuildResponse({
      sufficiency_summary_v1: {
        status: "FAIL",
        domain_verdicts: {
          probability: { status: "PASS", codes: [] },
          stress: { status: "FAIL", codes: ["CARD_TOO_SLOW"] },
        },
      },
    });
    expect(shouldShowSwapSuggestions(buildResponse)).toBe(true);
  });

  test("true when swap_suggestions non-empty even on PASS sufficiency", () => {
    const buildResponse = mkBuildResponse({
      sufficiency_summary_v1: { status: "PASS", domain_verdicts: {} },
      seed_to_deck_v1: { swap_suggestions: [{ out: "X", in: "Y" }] },
    });
    expect(shouldShowSwapSuggestions(buildResponse)).toBe(true);
  });

  test("false when sufficiency PASS and no swaps", () => {
    const buildResponse = mkBuildResponse({
      sufficiency_summary_v1: {
        status: "PASS",
        domain_verdicts: { probability: { status: "PASS", codes: [] } },
      },
    });
    expect(shouldShowSwapSuggestions(buildResponse)).toBe(false);
  });
});

describe("shouldShowCommanderRecommendation", () => {
  test("true when commander empty / null", () => {
    expect(shouldShowCommanderRecommendation("")).toBe(true);
    expect(shouldShowCommanderRecommendation(null)).toBe(true);
    expect(shouldShowCommanderRecommendation(undefined)).toBe(true);
    expect(shouldShowCommanderRecommendation("   ")).toBe(true);
  });
  test("false when commander present", () => {
    expect(shouldShowCommanderRecommendation("Krenko, Mob Boss")).toBe(false);
  });
});

describe("buildActiveDeckPayload + ACTIVE_DECK_STORAGE_KEY", () => {
  test("constant matches the storage namespace specified in the bundle spec", () => {
    expect(ACTIVE_DECK_STORAGE_KEY).toBe("mtgdb:workspace:active_deck_v1");
  });
  test("payload shape includes commander/decklist/source/staged_at_iso", () => {
    const p = buildActiveDeckPayload({
      commander: "Krenko",
      decklist: "1 Sol Ring",
      source: "import",
      nowIso: "2026-05-10T05:00:00.000Z",
    });
    expect(p).toEqual({
      commander: "Krenko",
      decklist: "1 Sol Ring",
      source: "import",
      staged_at_iso: "2026-05-10T05:00:00.000Z",
    });
  });
  test("staged_at_iso defaults to current time when nowIso omitted", () => {
    const before = Date.now();
    const p = buildActiveDeckPayload({ commander: "X", decklist: "1 Y", source: "build" });
    const parsed = new Date(p.staged_at_iso).getTime();
    expect(parsed).toBeGreaterThanOrEqual(before - 5);
    expect(parsed).toBeLessThanOrEqual(Date.now() + 5);
  });
});

// Save Deck namespace fix + legacy migration coverage.
import {
  _LEGACY_KEY_FOR_TESTS,
  _MIGRATION_FLAG_KEY_FOR_TESTS,
  runLegacyMigrationIfNeeded,
} from "../../lib/decks/legacyMigration";
import { _resetStorageAdapterForTests, _setStorageAdapterForTests, type StorageAdapter } from "../../lib/storage";
import { listSavedDecks } from "../../lib/decks/savedDecks";

function createMemoryAdapter(): StorageAdapter {
  const store = new Map<string, string>();
  return {
    save: <T>(k: string, v: T) => {
      store.set(k, JSON.stringify(v));
    },
    load: <T>(k: string) => {
      const raw = store.get(k);
      if (raw === undefined) return null as T | null;
      try {
        return JSON.parse(raw) as T;
      } catch {
        return null as T | null;
      }
    },
    list: (prefix: string) => Array.from(store.keys()).filter((k) => k.startsWith(prefix)),
    delete: (k: string) => {
      store.delete(k);
    },
    clear: (prefix?: string) => {
      const target = prefix ?? "mtgdb:";
      for (const k of Array.from(store.keys())) if (k.startsWith(target)) store.delete(k);
    },
  };
}

function _attachLocalStorageStub(): { teardown: () => void; setItem: (k: string, v: string) => void } {
  const store = new Map<string, string>();
  const stub = {
    getItem: (k: string) => (store.has(k) ? store.get(k)! : null),
    setItem: (k: string, v: string) => {
      store.set(k, v);
    },
    removeItem: (k: string) => {
      store.delete(k);
    },
    get length() {
      return store.size;
    },
    key: (i: number) => Array.from(store.keys())[i] ?? null,
    clear: () => {
      store.clear();
    },
  };
  const prevWindow = (globalThis as { window?: unknown }).window;
  (globalThis as { window?: unknown }).window = { localStorage: stub };
  return {
    teardown: () => {
      (globalThis as { window?: unknown }).window = prevWindow;
    },
    setItem: (k, v) => stub.setItem(k, v),
  };
}

describe("legacyMigration — Phase 4.13 Stage 2", () => {
  let teardown: () => void = () => {};
  let setLegacyEntry: (k: string, v: string) => void = () => {};

  beforeEach(() => {
    _setStorageAdapterForTests(createMemoryAdapter());
    const stub = _attachLocalStorageStub();
    teardown = stub.teardown;
    setLegacyEntry = stub.setItem;
  });
  afterEach(() => {
    teardown();
    _resetStorageAdapterForTests();
    vi.restoreAllMocks();
  });

  test("no-legacy case → reports zero imports + sets the flag", () => {
    const result = runLegacyMigrationIfNeeded();
    expect(result.alreadyDone).toBe(false);
    expect(result.importedCount).toBe(0);
    expect(result.skippedCount).toBe(0);
    expect(result.errors).toEqual([]);
    // Subsequent call short-circuits.
    const second = runLegacyMigrationIfNeeded();
    expect(second.alreadyDone).toBe(true);
  });

  test("legacy entries imported into mtgdb:decks:* via saveDeck", () => {
    const legacy = [
      { name: "Krenko Goblins", commander: "Krenko, Mob Boss", deckText: "1 Sol Ring\n1 Goblin Matron", updatedAtMs: 1000 },
      { name: "Atraxa Superfriends", commander: "Atraxa", deckText: "1 Sol Ring", updatedAtMs: 2000 },
    ];
    setLegacyEntry(_LEGACY_KEY_FOR_TESTS, JSON.stringify(legacy));

    const result = runLegacyMigrationIfNeeded();
    expect(result.alreadyDone).toBe(false);
    expect(result.importedCount).toBe(2);
    expect(result.skippedCount).toBe(0);
    const idx = listSavedDecks();
    expect(idx).toHaveLength(2);
    const names = idx.map((e) => e.name).sort();
    expect(names).toEqual(["Atraxa Superfriends", "Krenko Goblins"]);
  });

  test("malformed legacy entries are skipped (no infinite-retry loop)", () => {
    const legacy = [
      { name: "ok", commander: "X", deckText: "1 Sol Ring", updatedAtMs: 1 },
      { name: "", commander: "missing-name", deckText: "1 Plains" },
      { name: "missing-deck", commander: "Y", deckText: "" },
    ];
    setLegacyEntry(_LEGACY_KEY_FOR_TESTS, JSON.stringify(legacy));

    const result = runLegacyMigrationIfNeeded();
    expect(result.importedCount).toBe(1);
    expect(result.skippedCount).toBe(2);
    expect(listSavedDecks()).toHaveLength(1);
  });

  test("flag short-circuits subsequent runs even when new legacy entries appear", () => {
    runLegacyMigrationIfNeeded();
    setLegacyEntry(
      _LEGACY_KEY_FOR_TESTS,
      JSON.stringify([{ name: "After Migration", commander: "Z", deckText: "1 Plains", updatedAtMs: 1 }]),
    );
    const result = runLegacyMigrationIfNeeded();
    expect(result.alreadyDone).toBe(true);
    expect(result.importedCount).toBe(0);
    expect(listSavedDecks()).toHaveLength(0);
  });

  test("MIGRATION_FLAG_KEY constant matches namespace specified in spec", () => {
    expect(_MIGRATION_FLAG_KEY_FOR_TESTS).toBe("mtgdb:decks:_migration_v1_legacy_imported");
  });
});

// Goldfish active-deck bridge coverage.
import { decklistTextToGoldfishCards } from "../GoldfishView";

describe("decklistTextToGoldfishCards — Phase 4.13 Stage 3 bridge helper", () => {
  test("empty / whitespace text → []", () => {
    expect(decklistTextToGoldfishCards("")).toEqual([]);
    expect(decklistTextToGoldfishCards("   ")).toEqual([]);
  });
  test("expands count-name lines into copies", () => {
    const out = decklistTextToGoldfishCards("3 Forest\n1 Sol Ring");
    expect(out).toHaveLength(4);
    expect(out.filter((c) => c.name === "Forest")).toHaveLength(3);
    expect(out.filter((c) => c.name === "Sol Ring")).toHaveLength(1);
  });
  test("skips comments (# / //) + blank lines", () => {
    const out = decklistTextToGoldfishCards("# Aristocrats\n1 Sol Ring\n\n// note\n2 Bone Splinters");
    expect(out).toHaveLength(3);
    expect(out.map((c) => c.name).sort()).toEqual(["Bone Splinters", "Bone Splinters", "Sol Ring"]);
  });
  test("bare-name line treated as count=1", () => {
    const out = decklistTextToGoldfishCards("Goblin Matron");
    expect(out).toHaveLength(1);
    expect(out[0].name).toBe("Goblin Matron");
  });
  test("each card gets a unique instanceId", () => {
    const out = decklistTextToGoldfishCards("3 Forest");
    const ids = new Set(out.map((c) => c.instanceId));
    expect(ids.size).toBe(3);
  });
});
