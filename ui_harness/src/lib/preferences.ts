/**
 * Preferences — Phase 4.6 global-default + per-deck override.
 *
 * localStorage adapter with `mtgdb:workspace:*` prefix per autonomous_repair_log
 * soft-safety #4 (localStorage namespace conflicts → mtgdb:workspace:* prefix).
 * Per OQ-6 resolution: global default + per-deck override semantics.
 *
 * Read order: per-deck override (if deckId given AND key present) → global
 * default → caller-supplied fallback.
 *
 * Pure client-side; safe to import in tests via the `_StorageBackend` injection
 * point that defaults to `window.localStorage` but lets tests pass a Map-backed
 * stub.
 *
 * Phase 4.12a refactor (HARD safety #15): the default backend resolution now
 * delegates to `getRawStorageBackend()` from `lib/storage` so a future server
 * adapter (4.12c) can swap in without touching this module's external API.
 * The injection point + every export below is BYTE-IDENTICAL to the 4.6
 * contract — verified by existing `lib/__tests__/preferences.test.ts` passing
 * without modification.
 */
import { getRawStorageBackend } from "./storage/localStorageAdapter";

const PREFS_PREFIX = "mtgdb:workspace:";

export type StorageBackend = {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
  removeItem: (key: string) => void;
};

function _resolveBackend(injected?: StorageBackend): StorageBackend | null {
  if (injected) return injected;
  // Phase 4.12a: delegate to the shared raw-backend resolver. Behavior is
  // BYTE-IDENTICAL to the prior inline `window.localStorage` access — same
  // SSR-safe try/catch + null-return semantics. The shared resolver lets
  // 4.12c swap in a server adapter without re-touching this file.
  return getRawStorageBackend();
}

function _globalKey(name: string): string {
  return `${PREFS_PREFIX}global:${name}`;
}

function _perDeckKey(deckId: string, name: string): string {
  return `${PREFS_PREFIX}deck:${deckId}:${name}`;
}

export type ReadOptions = {
  deckId?: string | null;
  fallback?: string | null;
  backend?: StorageBackend;
};

export function readPreference(
  name: string,
  opts: ReadOptions = {},
): string | null {
  const backend = _resolveBackend(opts.backend);
  if (!backend) return opts.fallback ?? null;
  if (opts.deckId) {
    const v = backend.getItem(_perDeckKey(opts.deckId, name));
    if (v !== null && v !== "") return v;
  }
  const g = backend.getItem(_globalKey(name));
  if (g !== null && g !== "") return g;
  return opts.fallback ?? null;
}

export type WriteOptions = {
  scope?: "global" | "deck";
  deckId?: string | null;
  backend?: StorageBackend;
};

export function writePreference(
  name: string,
  value: string,
  opts: WriteOptions = {},
): void {
  const backend = _resolveBackend(opts.backend);
  if (!backend) return;
  const scope = opts.scope ?? (opts.deckId ? "deck" : "global");
  const key =
    scope === "deck" && opts.deckId
      ? _perDeckKey(opts.deckId, name)
      : _globalKey(name);
  backend.setItem(key, value);
}

export function clearPerDeckPreference(
  name: string,
  deckId: string,
  backend?: StorageBackend,
): void {
  const b = _resolveBackend(backend);
  if (!b) return;
  b.removeItem(_perDeckKey(deckId, name));
}

/** Derive a deterministic deck ID from build-response fields. */
export function deriveDeckId(input: {
  build_id?: string | null;
  build_hash_v1?: string | null;
  snapshot_id?: string | null;
  commander?: string | null;
}): string {
  if (input.build_id) return `build:${input.build_id}`;
  if (input.build_hash_v1) return `build:${input.build_hash_v1}`;
  // Fallback: hash of snapshot+commander per autonomous_repair_log soft-safety
  // #6 (deck-id derivation: build_id else hash of snapshot+commander).
  const snap = (input.snapshot_id ?? "").trim();
  const cmdr = (input.commander ?? "").trim().toLowerCase();
  if (snap === "" && cmdr === "") return "anon";
  // Tiny FNV-1a 32-bit; deterministic + dependency-free.
  let hash = 0x811c9dc5;
  const s = `${snap}::${cmdr}`;
  for (let i = 0; i < s.length; i += 1) {
    hash ^= s.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return `derived:${hash.toString(16)}`;
}

/** Helper for tests — synchronous Map-backed StorageBackend. */
export function createMemoryBackend(): StorageBackend {
  const store = new Map<string, string>();
  return {
    getItem: (k) => (store.has(k) ? store.get(k)! : null),
    setItem: (k, v) => {
      store.set(k, v);
    },
    removeItem: (k) => {
      store.delete(k);
    },
  };
}
