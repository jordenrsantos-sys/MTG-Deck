/** Vitest tests for lib/preferences (Phase 4.6 global+per-deck override). */
import { describe, expect, test } from "vitest";
import {
  clearPerDeckPreference,
  createMemoryBackend,
  deriveDeckId,
  readPreference,
  writePreference,
} from "../preferences";

describe("readPreference / writePreference (global scope)", () => {
  test("write + read round-trip", () => {
    const backend = createMemoryBackend();
    writePreference("groupBy", "color", { backend });
    expect(readPreference("groupBy", { backend, fallback: "type" })).toBe("color");
  });

  test("missing key returns fallback", () => {
    const backend = createMemoryBackend();
    expect(readPreference("groupBy", { backend, fallback: "type" })).toBe("type");
  });

  test("namespace prefix isolates from other keys", () => {
    const backend = createMemoryBackend();
    backend.setItem("unrelated_key", "noise");
    writePreference("groupBy", "color", { backend });
    expect(readPreference("groupBy", { backend, fallback: "type" })).toBe("color");
  });
});

describe("global default + per-deck override", () => {
  test("per-deck override wins when present", () => {
    const backend = createMemoryBackend();
    writePreference("groupBy", "color", { backend, scope: "global" });
    writePreference("groupBy", "cmc", { backend, deckId: "deck-123" });
    expect(readPreference("groupBy", { backend, deckId: "deck-123", fallback: "type" })).toBe("cmc");
  });

  test("global default is used when no per-deck override", () => {
    const backend = createMemoryBackend();
    writePreference("groupBy", "color", { backend, scope: "global" });
    expect(readPreference("groupBy", { backend, deckId: "deck-no-override", fallback: "type" })).toBe(
      "color",
    );
  });

  test("clearPerDeckPreference falls through to global", () => {
    const backend = createMemoryBackend();
    writePreference("groupBy", "color", { backend, scope: "global" });
    writePreference("groupBy", "cmc", { backend, deckId: "d1" });
    expect(readPreference("groupBy", { backend, deckId: "d1" })).toBe("cmc");
    clearPerDeckPreference("groupBy", "d1", backend);
    expect(readPreference("groupBy", { backend, deckId: "d1", fallback: "type" })).toBe("color");
  });

  test("writePreference with deckId defaults to deck scope", () => {
    const backend = createMemoryBackend();
    writePreference("groupBy", "rarity", { backend, deckId: "d2" });
    // Deck-scoped write should NOT pollute global.
    expect(readPreference("groupBy", { backend, fallback: "type" })).toBe("type");
    expect(readPreference("groupBy", { backend, deckId: "d2", fallback: "type" })).toBe("rarity");
  });
});

describe("deriveDeckId", () => {
  test("uses build_id when present", () => {
    expect(deriveDeckId({ build_id: "abc-123" })).toBe("build:abc-123");
  });

  test("falls back to build_hash_v1", () => {
    expect(deriveDeckId({ build_hash_v1: "deadbeef" })).toBe("build:deadbeef");
  });

  test("derives stable hash from snapshot+commander when neither build_* present", () => {
    const a = deriveDeckId({ snapshot_id: "snap-1", commander: "Krenko" });
    const b = deriveDeckId({ snapshot_id: "snap-1", commander: "Krenko" });
    expect(a).toBe(b);
    expect(a.startsWith("derived:")).toBe(true);
  });

  test("commander case insensitive in hash", () => {
    const a = deriveDeckId({ snapshot_id: "s", commander: "Atraxa" });
    const b = deriveDeckId({ snapshot_id: "s", commander: "ATRAXA" });
    expect(a).toBe(b);
  });

  test("falls back to anon when nothing supplied", () => {
    expect(deriveDeckId({})).toBe("anon");
  });
});

describe("graceful degradation when backend unavailable", () => {
  test("readPreference returns fallback when backend null", () => {
    expect(readPreference("groupBy", { fallback: "type", backend: undefined as never })).toBeTypeOf(
      "string",
    );
  });
});
