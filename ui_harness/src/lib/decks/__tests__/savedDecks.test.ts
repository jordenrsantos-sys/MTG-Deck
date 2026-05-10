/** Vitest tests for lib/decks/savedDecks (Phase 4.12a). */
import { beforeEach, describe, expect, test } from "vitest";
import { _resetStorageAdapterForTests, _setStorageAdapterForTests } from "../../storage";
import type { StorageAdapter } from "../../storage";
import { deleteDeck, listSavedDecks, loadDeck, makeDeckId, saveDeck } from "../savedDecks";

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
    list: (prefix: string) => {
      const out: string[] = [];
      for (const k of store.keys()) if (k.startsWith(prefix)) out.push(k);
      return out;
    },
    delete: (k: string) => {
      store.delete(k);
    },
    clear: (prefix?: string) => {
      const target = prefix ?? "mtgdb:";
      for (const k of Array.from(store.keys())) if (k.startsWith(target)) store.delete(k);
    },
  };
}

describe("savedDecks", () => {
  beforeEach(() => {
    _setStorageAdapterForTests(createMemoryAdapter());
  });

  test("makeDeckId is deterministic per (name, ms) pair", () => {
    expect(makeDeckId("My Deck", 1000)).toBe("my-deck-1000");
    expect(makeDeckId("My Deck", 1000)).toBe("my-deck-1000");
    expect(makeDeckId("ATRAXA superfriends!", 2000)).toBe("atraxa-superfriends-2000");
  });

  test("makeDeckId falls back to 'deck' for empty/special-char names", () => {
    expect(makeDeckId("", 5)).toBe("deck-5");
    expect(makeDeckId("***", 9)).toBe("deck-9");
  });

  test("saveDeck round-trip via loadDeck", () => {
    const deck = saveDeck({
      name: "Krenko",
      decklist: "1 Sol Ring\n1 Goblin Matron",
      nowMs: 100,
    });
    expect(deck.id).toBe("krenko-100");
    expect(deck.summary.totalCards).toBe(2);
    const loaded = loadDeck("krenko-100");
    expect(loaded).not.toBeNull();
    expect(loaded?.decklist).toContain("Sol Ring");
  });

  test("listSavedDecks returns newest-first by savedAt", () => {
    saveDeck({ name: "Older", decklist: "1 Plains", nowMs: 100 });
    saveDeck({ name: "Newer", decklist: "1 Forest", nowMs: 200 });
    const idx = listSavedDecks();
    expect(idx).toHaveLength(2);
    expect(idx[0].name).toBe("Newer");
    expect(idx[1].name).toBe("Older");
  });

  test("saving the same id again replaces the index entry (no duplicate)", () => {
    saveDeck({ name: "Krenko", decklist: "1 Sol Ring", nowMs: 100 });
    saveDeck({
      id: "krenko-100",
      name: "Krenko",
      decklist: "1 Sol Ring\n1 Skirk Prospector",
      nowMs: 150,
    });
    const idx = listSavedDecks();
    expect(idx).toHaveLength(1);
    const loaded = loadDeck("krenko-100");
    expect(loaded?.summary.totalCards).toBe(2);
  });

  test("deleteDeck removes both deck and index entry", () => {
    saveDeck({ name: "Bye", decklist: "1 Card", nowMs: 1 });
    expect(listSavedDecks()).toHaveLength(1);
    const id = listSavedDecks()[0].id;
    deleteDeck(id);
    expect(listSavedDecks()).toHaveLength(0);
    expect(loadDeck(id)).toBeNull();
  });

  test("totalCards counts respect '<n> Card' patterns + skips comments", () => {
    const deck = saveDeck({
      name: "Counting",
      decklist: "# header\n1 Sol Ring\n2 Forest\n// comment\n  3 Mountain  ",
      nowMs: 1,
    });
    expect(deck.summary.totalCards).toBe(6);
  });

  test("commander_oracle_id is preserved when supplied", () => {
    const deck = saveDeck({
      name: "Atraxa",
      decklist: "1 Sol Ring",
      commander_oracle_id: "abc-123",
      nowMs: 1,
    });
    expect(loadDeck(deck.id)?.commander_oracle_id).toBe("abc-123");
  });

  // Reset to default after the suite so other suites pick the live adapter.
  test("teardown resets adapter for following suites", () => {
    _resetStorageAdapterForTests();
    expect(true).toBe(true);
  });
});
