/**
 * localStorageAdapter — Phase 4.12a.
 *
 * Implements StorageAdapter against browser localStorage. JSON-serializes
 * values; tolerates QuotaExceededError + malformed entries silently per
 * autonomous_repair_log soft-safety #4 (compression queued as 4.12.1 if
 * the quota is hit in practice). SSR-safe: every method short-circuits
 * gracefully when `window.localStorage` is unavailable.
 *
 * NOTE: this also exposes `getRawBackend()` returning the underlying
 * Storage-shaped object (or a memory stub when SSR), used by
 * `lib/preferences.ts` to preserve its 4.6 contract semantics
 * (string-only KV; the preferences module manages its own keys/scopes
 * on top of the raw backend, NOT through the JSON adapter).
 */
import type { StorageAdapter } from "./adapter";
import { devWarn } from "../devLog";

export type RawStorageBackend = {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
  removeItem: (key: string) => void;
};

function _resolveRawBackend(): RawStorageBackend | null {
  try {
    if (typeof window !== "undefined" && window.localStorage) {
      return window.localStorage;
    }
  } catch {
    // SSR / sandbox without window — fall through.
  }
  return null;
}

/** Listing requires iterating localStorage keys; this is the raw helper. */
function _listKeysWithPrefix(prefix: string): string[] {
  const backend = _resolveRawBackend();
  if (!backend) return [];
  const out: string[] = [];
  // window.localStorage exposes a `length` / `key(i)` iteration API.
  // RawStorageBackend doesn't include it (it's a string-only KV); cast safely.
  const ls = backend as unknown as Storage;
  if (typeof ls.length !== "number" || typeof ls.key !== "function") return out;
  for (let i = 0; i < ls.length; i += 1) {
    const k = ls.key(i);
    if (typeof k === "string" && k.startsWith(prefix)) out.push(k);
  }
  return out;
}

export const localStorageAdapter: StorageAdapter = {
  save<T>(key: string, value: T): void {
    const backend = _resolveRawBackend();
    if (!backend) return;
    try {
      backend.setItem(key, JSON.stringify(value));
    } catch (e: unknown) {
      // QuotaExceededError or serialization failure — surface a dev warning
      // and return silently per autonomous_repair_log soft-safety #4.
      devWarn(`[localStorageAdapter] save failed for key=${key}:`, e);
    }
  },

  load<T>(key: string): T | null {
    const backend = _resolveRawBackend();
    if (!backend) return null;
    try {
      const raw = backend.getItem(key);
      if (raw === null) return null;
      return JSON.parse(raw) as T;
    } catch (e: unknown) {
      devWarn(`[localStorageAdapter] load failed for key=${key}:`, e);
      return null;
    }
  },

  list(prefix: string): string[] {
    return _listKeysWithPrefix(prefix);
  },

  delete(key: string): void {
    const backend = _resolveRawBackend();
    if (!backend) return;
    try {
      backend.removeItem(key);
    } catch (e: unknown) {
      devWarn(`[localStorageAdapter] delete failed for key=${key}:`, e);
    }
  },

  clear(prefix?: string): void {
    const backend = _resolveRawBackend();
    if (!backend) return;
    if (prefix === undefined) {
      // Don't actually clear ALL localStorage — that's destructive across the
      // origin. Limit to mtgdb:* always. Callers needing a full wipe should
      // call backend.clear() directly with explicit intent.
      const keys = _listKeysWithPrefix("mtgdb:");
      for (const k of keys) backend.removeItem(k);
      return;
    }
    const keys = _listKeysWithPrefix(prefix);
    for (const k of keys) backend.removeItem(k);
  },
};

/**
 * Returns the raw Storage-shaped backend (string KV). Used by
 * `lib/preferences.ts` to preserve its 4.6 contract — preferences hands
 * out string values, not JSON-encoded ones. Callers that need
 * type-safe JSON persistence should use `localStorageAdapter.save/load`.
 */
export function getRawStorageBackend(): RawStorageBackend | null {
  return _resolveRawBackend();
}
