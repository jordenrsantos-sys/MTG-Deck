/**
 * storage — Phase 4.12a factory entry point.
 *
 * Returns the active StorageAdapter. v1.0 ships localStorage only;
 * a future serverAdapter (4.12c) will be selected via build-target
 * env flag (`VITE_STORAGE_BACKEND`) without touching consumer code.
 *
 * HARD safety #10: NO server adapter implemented in this bundle.
 */
import type { StorageAdapter } from "./adapter";
import { localStorageAdapter } from "./localStorageAdapter";

export type { StorageAdapter } from "./adapter";
export { localStorageAdapter, getRawStorageBackend } from "./localStorageAdapter";

let _activeAdapter: StorageAdapter = localStorageAdapter;

/** Returns the currently-active StorageAdapter (defaults to localStorage). */
export function getStorageAdapter(): StorageAdapter {
  return _activeAdapter;
}

/** Test-only: swap the active adapter (e.g., to a memory stub). */
export function _setStorageAdapterForTests(adapter: StorageAdapter): void {
  _activeAdapter = adapter;
}

/** Test-only: reset to the default localStorage adapter. */
export function _resetStorageAdapterForTests(): void {
  _activeAdapter = localStorageAdapter;
}
