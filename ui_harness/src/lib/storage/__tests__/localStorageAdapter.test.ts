/** Vitest tests for storage adapter contract (Phase 4.12a). */
import { afterEach, describe, expect, test } from "vitest";
import type { StorageAdapter } from "../adapter";

/** Memory implementation of StorageAdapter; mirrors localStorageAdapter shape. */
function createMemoryAdapter(): StorageAdapter {
  const store = new Map<string, string>();
  return {
    save<T>(key: string, value: T): void {
      try {
        store.set(key, JSON.stringify(value));
      } catch {
        /* swallow */
      }
    },
    load<T>(key: string): T | null {
      const raw = store.get(key);
      if (raw === undefined) return null;
      try {
        return JSON.parse(raw) as T;
      } catch {
        return null;
      }
    },
    list(prefix: string): string[] {
      const out: string[] = [];
      for (const k of store.keys()) if (k.startsWith(prefix)) out.push(k);
      return out;
    },
    delete(key: string): void {
      store.delete(key);
    },
    clear(prefix?: string): void {
      const target = prefix ?? "mtgdb:";
      for (const k of Array.from(store.keys())) if (k.startsWith(target)) store.delete(k);
    },
  };
}

describe("StorageAdapter contract — memory impl", () => {
  let adapter: StorageAdapter;
  afterEach(() => {
    adapter.clear("mtgdb:");
  });

  test("save + load round-trip with typed value", () => {
    adapter = createMemoryAdapter();
    adapter.save<{ x: number; y: string }>("mtgdb:test:obj", { x: 7, y: "ok" });
    const out = adapter.load<{ x: number; y: string }>("mtgdb:test:obj");
    expect(out).toEqual({ x: 7, y: "ok" });
  });

  test("load missing key returns null (no throw)", () => {
    adapter = createMemoryAdapter();
    expect(adapter.load("mtgdb:nope")).toBeNull();
  });

  test("delete removes key", () => {
    adapter = createMemoryAdapter();
    adapter.save("mtgdb:test:k", 1);
    expect(adapter.load("mtgdb:test:k")).toBe(1);
    adapter.delete("mtgdb:test:k");
    expect(adapter.load("mtgdb:test:k")).toBeNull();
  });

  test("list returns only keys with the prefix", () => {
    adapter = createMemoryAdapter();
    adapter.save("mtgdb:decks:a", 1);
    adapter.save("mtgdb:decks:b", 2);
    adapter.save("mtgdb:other:c", 3);
    const decks = adapter.list("mtgdb:decks:").sort();
    expect(decks).toEqual(["mtgdb:decks:a", "mtgdb:decks:b"]);
  });

  test("clear with prefix removes matching keys, leaves others", () => {
    adapter = createMemoryAdapter();
    adapter.save("mtgdb:decks:a", 1);
    adapter.save("mtgdb:settings:theme", "dark");
    adapter.clear("mtgdb:decks:");
    expect(adapter.load("mtgdb:decks:a")).toBeNull();
    expect(adapter.load("mtgdb:settings:theme")).toBe("dark");
  });

  test("clear with no prefix wipes mtgdb:* (defensive scope)", () => {
    adapter = createMemoryAdapter();
    adapter.save("mtgdb:decks:a", 1);
    adapter.save("mtgdb:settings:b", 2);
    adapter.clear();
    expect(adapter.load("mtgdb:decks:a")).toBeNull();
    expect(adapter.load("mtgdb:settings:b")).toBeNull();
  });
});
