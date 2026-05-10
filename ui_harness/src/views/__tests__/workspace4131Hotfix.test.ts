/**
 * Vitest tests for Phase 4.13.1 hotfix adapters (workspace remount restore +
 * Build button + source labeling + Goldfish banner). Per AUTOMATION_RULES
 * halt-and-ask + autonomous_repair_log soft-safety #7 (carried forward from
 * 4.13): no @testing-library/react install — logic-level adapter coverage.
 */
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
  buildBuildRequestBody,
  buildGoldfishBannerText,
  callBuildEndpoint,
  normalizeActiveDeckSource,
  restoreFromActiveDeckSlot,
  type ActiveDeckPayload,
} from "../workspaceIntegrationAdapters";

const ACTIVE_DECK_KEY = "mtgdb:workspace:active_deck_v1";

function _attachLocalStorageStub(): { teardown: () => void; store: Map<string, string> } {
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
    store,
  };
}

describe("Stage 1 — restoreFromActiveDeckSlot", () => {
  let teardown: () => void = () => {};
  let store: Map<string, string> = new Map();

  beforeEach(() => {
    ({ teardown, store } = _attachLocalStorageStub());
  });
  afterEach(() => {
    teardown();
  });

  test("empty slot → null", () => {
    expect(restoreFromActiveDeckSlot()).toBeNull();
  });

  test("valid slot returns {commander, decklist, source}", () => {
    const payload: ActiveDeckPayload = {
      commander: "Krenko, Mob Boss",
      decklist: "1 Sol Ring\n1 Goblin Matron",
      source: "archidekt",
      staged_at_iso: "2026-05-10T06:00:00.000Z",
    };
    store.set(ACTIVE_DECK_KEY, JSON.stringify(payload));
    const restored = restoreFromActiveDeckSlot();
    expect(restored).not.toBeNull();
    expect(restored?.commander).toBe("Krenko, Mob Boss");
    expect(restored?.decklist).toContain("Sol Ring");
    expect(restored?.source).toBe("archidekt");
  });

  test("corrupt JSON → null (no throw)", () => {
    store.set(ACTIVE_DECK_KEY, "{not valid json");
    expect(restoreFromActiveDeckSlot()).toBeNull();
  });

  test("missing commander or decklist → null (defensive)", () => {
    store.set(ACTIVE_DECK_KEY, JSON.stringify({ commander: "", decklist: "1 Sol Ring", source: "manual" }));
    expect(restoreFromActiveDeckSlot()).toBeNull();
    store.set(ACTIVE_DECK_KEY, JSON.stringify({ commander: "X", decklist: "", source: "manual" }));
    expect(restoreFromActiveDeckSlot()).toBeNull();
  });

  test("does NOT clear the slot (Goldfish needs it)", () => {
    store.set(
      ACTIVE_DECK_KEY,
      JSON.stringify({ commander: "Krenko", decklist: "1 Sol Ring", source: "manual", staged_at_iso: "x" }),
    );
    restoreFromActiveDeckSlot();
    expect(store.has(ACTIVE_DECK_KEY)).toBe(true);
  });

  test("non-string source falls back to 'manual'", () => {
    store.set(
      ACTIVE_DECK_KEY,
      JSON.stringify({ commander: "X", decklist: "1 Y", source: 42, staged_at_iso: "x" }),
    );
    expect(restoreFromActiveDeckSlot()?.source).toBe("manual");
  });
});

describe("Stage 2 — buildBuildRequestBody", () => {
  test("constructs canonical /build payload from current state", () => {
    const body = buildBuildRequestBody({
      snapshotId: "20260217_190902",
      profileId: "focused",
      bracketId: "B2",
      commander: "Krenko, Mob Boss",
      cards: ["Sol Ring", "Goblin Matron"],
    });
    expect(body).toEqual({
      db_snapshot_id: "20260217_190902",
      profile_id: "focused",
      bracket_id: "B2",
      format: "commander",
      commander: "Krenko, Mob Boss",
      cards: ["Sol Ring", "Goblin Matron"],
      engine_patches_v0: [],
    });
  });

  test("trims whitespace + falls back to focused/B2 defaults", () => {
    const body = buildBuildRequestBody({
      snapshotId: "  snap  ",
      profileId: "  ",
      bracketId: "",
      commander: "  Atraxa  ",
      cards: [],
    });
    expect(body.db_snapshot_id).toBe("snap");
    expect(body.profile_id).toBe("focused");
    expect(body.bracket_id).toBe("B2");
    expect(body.commander).toBe("Atraxa");
  });
});

describe("Stage 2 — callBuildEndpoint", () => {
  test("ok=true on 200 with parsed JSON", async () => {
    const fetcher = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ result: { sufficiency_summary_v1: { status: "PASS" } } }),
    });
    const result = await callBuildEndpoint("http://localhost:8000", buildBuildRequestBody({
      snapshotId: "x", profileId: "focused", bracketId: "B2", commander: "C", cards: [],
    }), fetcher as unknown as typeof fetch);
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.response).toBeTruthy();
  });

  test("ok=false on 500 status — preserves prior buildResponse semantically", async () => {
    const fetcher = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => "{\"error\":\"boom\"}",
    });
    const result = await callBuildEndpoint("http://localhost:8000", buildBuildRequestBody({
      snapshotId: "x", profileId: "focused", bracketId: "B2", commander: "C", cards: [],
    }), fetcher as unknown as typeof fetch);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("HTTP 500");
  });

  test("ok=false on network error (no throw)", async () => {
    const fetcher = vi.fn().mockRejectedValue(new Error("ECONNREFUSED"));
    const result = await callBuildEndpoint("http://localhost:8000", buildBuildRequestBody({
      snapshotId: "x", profileId: "focused", bracketId: "B2", commander: "C", cards: [],
    }), fetcher as unknown as typeof fetch);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("network error");
  });

  test("ok=false on non-JSON 200 response", async () => {
    const fetcher = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => "<html>nope</html>",
    });
    const result = await callBuildEndpoint("http://localhost:8000", buildBuildRequestBody({
      snapshotId: "x", profileId: "focused", bracketId: "B2", commander: "C", cards: [],
    }), fetcher as unknown as typeof fetch);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("non-JSON");
  });
});

describe("Stage 3 — normalizeActiveDeckSource", () => {
  test("recognized parser-registry sources pass through verbatim", () => {
    expect(normalizeActiveDeckSource("archidekt")).toBe("archidekt");
    expect(normalizeActiveDeckSource("arena_text")).toBe("arena_text");
    expect(normalizeActiveDeckSource("mtgo_text")).toBe("mtgo_text");
    expect(normalizeActiveDeckSource("plain_text")).toBe("plain_text");
    expect(normalizeActiveDeckSource("file_upload")).toBe("file_upload");
  });

  test("legacy enum values still recognized (backward compat)", () => {
    expect(normalizeActiveDeckSource("import")).toBe("import");
    expect(normalizeActiveDeckSource("build")).toBe("build");
    expect(normalizeActiveDeckSource("saved_deck")).toBe("saved_deck");
    expect(normalizeActiveDeckSource("manual")).toBe("manual");
  });

  test("'saved_deck:<id>' suffix collapses to 'saved_deck'", () => {
    expect(normalizeActiveDeckSource("saved_deck:krenko-100")).toBe("saved_deck");
    expect(normalizeActiveDeckSource("SAVED_DECK:abc")).toBe("saved_deck");
  });

  test("null / empty / unknown → 'manual'", () => {
    expect(normalizeActiveDeckSource(null)).toBe("manual");
    expect(normalizeActiveDeckSource(undefined)).toBe("manual");
    expect(normalizeActiveDeckSource("")).toBe("manual");
    expect(normalizeActiveDeckSource("???")).toBe("manual");
  });

  test("case-insensitive matching", () => {
    expect(normalizeActiveDeckSource("Archidekt")).toBe("archidekt");
    expect(normalizeActiveDeckSource("ARENA_TEXT")).toBe("arena_text");
  });
});

describe("Stage 4 — buildGoldfishBannerText", () => {
  test("playtest_staged → success badge + 'Using staged playtest deck'", () => {
    const banner = buildGoldfishBannerText({ deckSource: "playtest_staged" });
    expect(banner.variant).toBe("success");
    expect(banner.text).toBe("Using staged playtest deck");
  });

  test("synthetic → neutral + import-deck call-to-action", () => {
    const banner = buildGoldfishBannerText({ deckSource: "synthetic" });
    expect(banner.variant).toBe("neutral");
    expect(banner.text).toContain("sample deck");
    expect(banner.text).toContain("#import");
  });

  test("workspace_active with commander + source → info + 'Using imported deck: <name> · source: <src>'", () => {
    const banner = buildGoldfishBannerText({
      deckSource: "workspace_active",
      workspaceCommander: "Krenko, Mob Boss",
      workspaceSource: "archidekt",
    });
    expect(banner.variant).toBe("info");
    expect(banner.text).toBe("Using imported deck: Krenko, Mob Boss · source: archidekt");
  });

  test("workspace_active with no commander → fallback to 'active deck'", () => {
    const banner = buildGoldfishBannerText({
      deckSource: "workspace_active",
      workspaceCommander: "",
      workspaceSource: "manual",
    });
    expect(banner.text).toBe("Using imported deck: active deck · source: manual");
  });

  test("commander name truncated when >32 chars (banner under 80 chars)", () => {
    const banner = buildGoldfishBannerText({
      deckSource: "workspace_active",
      workspaceCommander: "An Extremely Long Commander Name That Goes Forever And Ever",
      workspaceSource: "archidekt",
    });
    expect(banner.text.length).toBeLessThan(80);
    expect(banner.text).toContain("…");
  });

  test("unknown workspace source normalizes to 'manual' inside banner", () => {
    const banner = buildGoldfishBannerText({
      deckSource: "workspace_active",
      workspaceCommander: "X",
      workspaceSource: "???",
    });
    expect(banner.text).toContain("source: manual");
  });
});
