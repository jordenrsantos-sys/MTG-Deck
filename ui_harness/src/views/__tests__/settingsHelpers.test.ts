/** Vitest tests for SettingsView pure helpers (Phase 4.12b). */
import { beforeEach, describe, expect, test } from "vitest";
import { _resetStorageAdapterForTests, _setStorageAdapterForTests, type StorageAdapter } from "../../lib/storage";
import { _settingsHelpers } from "../SettingsView";

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

describe("SettingsView _settingsHelpers", () => {
  beforeEach(() => {
    _setStorageAdapterForTests(createMemoryAdapter());
  });

  test("readSetting returns DEFAULTS when nothing stored", () => {
    expect(_settingsHelpers.readSetting("darkMode")).toBe(_settingsHelpers.DEFAULTS.darkMode);
    expect(_settingsHelpers.readSetting("bracketDefault")).toBe(_settingsHelpers.DEFAULTS.bracketDefault);
    expect(_settingsHelpers.readSetting("profileDefault")).toBe(_settingsHelpers.DEFAULTS.profileDefault);
  });

  test("writeSetting + readSetting round-trip", () => {
    _settingsHelpers.writeSetting("darkMode", false);
    _settingsHelpers.writeSetting("bracketDefault", "B4");
    _settingsHelpers.writeSetting("profileDefault", "balanced");
    expect(_settingsHelpers.readSetting("darkMode")).toBe(false);
    expect(_settingsHelpers.readSetting("bracketDefault")).toBe("B4");
    expect(_settingsHelpers.readSetting("profileDefault")).toBe("balanced");
  });

  test("BRACKETS enum stable B1..B5", () => {
    expect(_settingsHelpers.BRACKETS).toEqual(["B1", "B2", "B3", "B4", "B5"]);
  });

  test("PROFILES enum hardcoded ['balanced'] for v1.2 (autonomous_repair #12)", () => {
    expect(_settingsHelpers.PROFILES).toEqual(["balanced"]);
  });

  test("teardown reset adapter for following suites", () => {
    _resetStorageAdapterForTests();
    expect(true).toBe(true);
  });
});
