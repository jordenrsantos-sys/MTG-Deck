/**
 * legacyMigration — Phase 4 BUNDLE Integration (4.13) Stage 2.
 *
 * One-shot migration of pre-Phase-4.12a `mtg_saved_decks_v1` legacy entries
 * into the `mtgdb:decks:*` namespace via `saveDeck()`. Idempotent: a
 * `mtgdb:decks:_migration_v1_legacy_imported` flag in the storage adapter
 * short-circuits subsequent calls.
 *
 * Per autonomous_repair_log soft-safety #4: corrupt/malformed legacy
 * entries surface a console.warn + are skipped (no infinite-retry loop;
 * migration marked complete on first run regardless of per-deck failures).
 */
import { saveDeck } from "./savedDecks";
import { getStorageAdapter } from "../storage";
import { devWarn } from "../devLog";

const LEGACY_KEY = "mtg_saved_decks_v1";
const MIGRATION_FLAG_KEY = "mtgdb:decks:_migration_v1_legacy_imported";

export type LegacyMigrationResult = {
  alreadyDone: boolean;
  importedCount: number;
  skippedCount: number;
  errors: string[];
};

type LegacyEntry = {
  name?: string;
  commander?: string;
  deckText?: string;
  updatedAtMs?: number;
};

function _readRawLocalStorage(key: string): string | null {
  try {
    if (typeof window === "undefined" || !window.localStorage) return null;
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function _parseLegacyArray(raw: string | null): LegacyEntry[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed as LegacyEntry[];
  } catch {
    return [];
  }
}

/**
 * Run the one-shot legacy migration. Returns a summary; safe to call
 * multiple times — short-circuits when the flag is set.
 */
export function runLegacyMigrationIfNeeded(): LegacyMigrationResult {
  const adapter = getStorageAdapter();
  const flag = adapter.load<boolean>(MIGRATION_FLAG_KEY);
  if (flag === true) {
    return { alreadyDone: true, importedCount: 0, skippedCount: 0, errors: [] };
  }

  const legacyRaw = _readRawLocalStorage(LEGACY_KEY);
  const legacyEntries = _parseLegacyArray(legacyRaw);

  let imported = 0;
  let skipped = 0;
  const errors: string[] = [];

  for (const entry of legacyEntries) {
    try {
      const name = typeof entry.name === "string" ? entry.name : "";
      const decklist = typeof entry.deckText === "string" ? entry.deckText : "";
      if (name.trim() === "" || decklist.trim() === "") {
        skipped += 1;
        continue;
      }
      const nowMs = typeof entry.updatedAtMs === "number" ? entry.updatedAtMs : Date.now();
      saveDeck({ name, decklist, nowMs });
      imported += 1;
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`legacy "${entry.name ?? "<unnamed>"}": ${msg}`);
      devWarn(`[legacyMigration] failed to import entry:`, e);
      skipped += 1;
    }
  }

  // Mark complete even if individual imports failed — autonomous_repair #4
  // (no infinite-retry loop; failures queued as 4.13.1).
  try {
    adapter.save(MIGRATION_FLAG_KEY, true);
  } catch (e: unknown) {
    devWarn(`[legacyMigration] failed to write completion flag:`, e);
  }

  return { alreadyDone: false, importedCount: imported, skippedCount: skipped, errors };
}

export const _MIGRATION_FLAG_KEY_FOR_TESTS = MIGRATION_FLAG_KEY;
export const _LEGACY_KEY_FOR_TESTS = LEGACY_KEY;
