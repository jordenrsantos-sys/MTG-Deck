/**
 * StorageAdapter — Phase 4.12a (persistence adapter, OQ-1=(c) local-first).
 *
 * Generic key-value persistence interface. The default implementation
 * (`localStorageAdapter`) wraps browser localStorage; a future server
 * adapter (4.12c, gated on user direction re: auth provider) can swap in
 * without touching consumer code.
 *
 * Type-safe: `save<T>` / `load<T>` JSON-serialize / parse with caller-
 * supplied types. Adapter implementations MUST tolerate quota / parse /
 * SSR errors gracefully — `save` returns silently on QuotaExceededError;
 * `load` returns `null` on missing or malformed entries.
 *
 * HARD safety #10: NO server-side persistence implementation in this
 * bundle. The interface ships now; a serverAdapter is a 4.12c epic.
 */

export type StorageAdapter = {
  save: <T>(key: string, value: T) => void;
  load: <T>(key: string) => T | null;
  list: (prefix: string) => string[];
  delete: (key: string) => void;
  clear: (prefix?: string) => void;
};
