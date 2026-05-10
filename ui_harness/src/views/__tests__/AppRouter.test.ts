/**
 * Vitest tests for AppRouter — Phase 4.12.1 hotfix regression coverage.
 *
 * The bug: `openWorkspace()` set `window.location.hash = ""`, which
 * `parseHash("")` mapped to "landing" per the Phase 4.12b explicit-landing
 * decision. The hashchange listener then re-ran `setView(parseHash(""))`
 * = "landing", clobbering the explicit `setView("workspace")` and
 * orphaning the staged-import slot. The fix sets the hash to `"#workspace"`
 * so the listener routes to WorkspaceView consistently.
 *
 * These tests cover:
 *   1. parseHash("#workspace") → "workspace" (the URL the fix produces).
 *   2. parseHash("") → "landing" (DESIGN INTENT preserved per 4.12b).
 *   3. Staged-import slot round-trip via ImportRoute exports — pre-write
 *      fixture → readStagedImport returns it → clearStagedImport empties
 *      → readStagedImport returns null. (This is the bridge that was
 *      orphaned by the race; verifying the read+clear contract is intact
 *      is the second half of the regression coverage.)
 *
 * No React rendering — vitest config uses environment="node" and
 * @testing-library/react is not a project dep (HARD halt-and-ask per
 * AUTOMATION_RULES). We test the pure URL-mapping + slot-lifecycle
 * helpers that drive the rendered view, which is exactly the regression
 * surface.
 */
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { parseHash } from "../AppRouter";
import {
  IMPORT_STAGED_KEY,
  clearStagedImport,
  readStagedImport,
  type StagedImport,
} from "../ImportRoute";

/** Minimal in-memory localStorage stub matching the surface ImportRoute uses. */
function _attachLocalStorageStub(): { teardown: () => void } {
  const store = new Map<string, string>();
  const stub = {
    getItem: (k: string) => (store.has(k) ? store.get(k)! : null),
    setItem: (k: string, v: string) => {
      store.set(k, v);
    },
    removeItem: (k: string) => {
      store.delete(k);
    },
    // length / key / clear not used by ImportRoute helpers but provided for
    // shape compatibility if any caller tests them.
    get length() {
      return store.size;
    },
    key: (i: number) => Array.from(store.keys())[i] ?? null,
    clear: () => {
      store.clear();
    },
  };
  // Use `globalThis.window` — vitest's node env doesn't provide one by default.
  const prevWindow = (globalThis as { window?: unknown }).window;
  (globalThis as { window?: unknown }).window = { localStorage: stub };
  return {
    teardown: () => {
      (globalThis as { window?: unknown }).window = prevWindow;
    },
  };
}

describe("parseHash — Phase 4.12.1 regression", () => {
  test('"#workspace" routes to workspace (the hotfix URL)', () => {
    expect(parseHash("#workspace")).toBe("workspace");
  });

  test('"" still routes to landing (4.12b design preserved)', () => {
    expect(parseHash("")).toBe("landing");
    expect(parseHash("#/")).toBe("landing");
  });

  test('existing workspace hashes ("#workspace-decks", "#workspace-runs") still route to workspace', () => {
    expect(parseHash("#workspace-decks")).toBe("workspace");
    expect(parseHash("#workspace-runs")).toBe("workspace");
  });

  test("other named routes unchanged", () => {
    expect(parseHash("#diagnostics")).toBe("diagnostics");
    expect(parseHash("#import")).toBe("import");
    expect(parseHash("#playtest")).toBe("playtest");
    expect(parseHash("#settings")).toBe("settings");
    expect(parseHash("#decks")).toBe("decks");
  });
});

describe("staged-import slot round-trip (4.3/4.6 bridge contract)", () => {
  let teardown: () => void = () => {};

  beforeEach(() => {
    ({ teardown } = _attachLocalStorageStub());
  });
  afterEach(() => {
    teardown();
  });

  test("pre-written staged payload reads back; clearStagedImport empties; subsequent read is null", () => {
    const fixture: StagedImport = {
      commander: "Krenko, Mob Boss",
      commander_oracle_id: "krenko-uuid",
      decklist: "1 Sol Ring\n1 Goblin Matron",
      unknowns: [],
      source: "archidekt",
      staged_at_iso: "2026-05-10T03:00:00.000Z",
    };
    // Pre-write the fixture using the canonical key from ImportRoute (no
    // re-declaration per autonomous_repair_log soft-safety #4).
    const stub = (globalThis as { window?: { localStorage?: Storage } }).window?.localStorage;
    stub?.setItem(IMPORT_STAGED_KEY, JSON.stringify(fixture));

    const read1 = readStagedImport();
    expect(read1).not.toBeNull();
    expect(read1?.commander).toBe("Krenko, Mob Boss");
    expect(read1?.decklist).toContain("Sol Ring");
    expect(read1?.source).toBe("archidekt");

    clearStagedImport();
    const read2 = readStagedImport();
    expect(read2).toBeNull();
  });

  test("readStagedImport returns null when no slot present (pre-fix race did NOT corrupt this)", () => {
    expect(readStagedImport()).toBeNull();
  });
});
