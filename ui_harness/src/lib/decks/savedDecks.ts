/**
 * savedDecks — Phase 4.12a.
 *
 * Save / load / list / delete user decks via the `getStorageAdapter()`
 * factory. Namespace: `mtgdb:decks:*`. Schema per autonomous_repair_log
 * soft-safety #11:
 *   { id, name, savedAt: ISO-8601, commander_oracle_id?, decklist, summary }
 *
 * Pure helpers — no React. Tests inject a memory adapter via
 * `_setStorageAdapterForTests`.
 */
import { getStorageAdapter } from "../storage";

const DECKS_PREFIX = "mtgdb:decks:";
const DECKS_INDEX_KEY = "mtgdb:decks:_index_v1";

export type SavedDeckSummary = {
  totalCards: number;
  bracket?: string;
};

export type SavedDeck = {
  id: string;
  name: string;
  savedAt: string;
  commander_oracle_id?: string | null;
  decklist: string;
  summary: SavedDeckSummary;
};

export type SavedDeckIndexEntry = Pick<SavedDeck, "id" | "name" | "savedAt"> & {
  summary: SavedDeckSummary;
};

function _deckKey(id: string): string {
  return `${DECKS_PREFIX}${id}`;
}

function _readIndex(): SavedDeckIndexEntry[] {
  const adapter = getStorageAdapter();
  const idx = adapter.load<SavedDeckIndexEntry[]>(DECKS_INDEX_KEY);
  return Array.isArray(idx) ? idx : [];
}

function _writeIndex(idx: SavedDeckIndexEntry[]): void {
  const adapter = getStorageAdapter();
  adapter.save(DECKS_INDEX_KEY, idx);
}

/**
 * Generate a deterministic-prefix deck id from the name + millisecond
 * timestamp so tests + UI surfaces can reason about ordering without
 * coupling to a UUID library.
 */
export function makeDeckId(name: string, nowMs: number = Date.now()): string {
  const slug = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 32);
  return `${slug || "deck"}-${nowMs}`;
}

export type SaveDeckInput = {
  name: string;
  decklist: string;
  commander_oracle_id?: string | null;
  summary?: Partial<SavedDeckSummary>;
  /** Override the generated id (e.g. when overwriting an existing save). */
  id?: string;
  /** Override Date.now (test injection). */
  nowMs?: number;
};

export function saveDeck(input: SaveDeckInput): SavedDeck {
  const adapter = getStorageAdapter();
  const nowMs = input.nowMs ?? Date.now();
  const id = input.id ?? makeDeckId(input.name, nowMs);
  const totalCards = _countDecklistLines(input.decklist);
  const deck: SavedDeck = {
    id,
    name: input.name.trim() || "Untitled deck",
    savedAt: new Date(nowMs).toISOString(),
    commander_oracle_id: input.commander_oracle_id ?? null,
    decklist: input.decklist,
    summary: {
      totalCards,
      ...(input.summary ?? {}),
    },
  };
  adapter.save(_deckKey(id), deck);
  // Refresh the index entry; deduplicate by id.
  const idx = _readIndex().filter((e) => e.id !== id);
  idx.push({ id, name: deck.name, savedAt: deck.savedAt, summary: deck.summary });
  // Newest-first ordering by savedAt for predictable list rendering.
  idx.sort((a, b) => (a.savedAt < b.savedAt ? 1 : a.savedAt > b.savedAt ? -1 : 0));
  _writeIndex(idx);
  return deck;
}

export function loadDeck(id: string): SavedDeck | null {
  const adapter = getStorageAdapter();
  return adapter.load<SavedDeck>(_deckKey(id));
}

export function listSavedDecks(): SavedDeckIndexEntry[] {
  return _readIndex();
}

export function deleteDeck(id: string): void {
  const adapter = getStorageAdapter();
  adapter.delete(_deckKey(id));
  const idx = _readIndex().filter((e) => e.id !== id);
  _writeIndex(idx);
}

function _countDecklistLines(text: string): number {
  if (typeof text !== "string" || text.trim() === "") return 0;
  let total = 0;
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#") || line.startsWith("//")) continue;
    const m = /^\s*(\d+)\s+/.exec(line);
    if (m) total += Number.parseInt(m[1], 10) || 1;
    else total += 1;
  }
  return total;
}
