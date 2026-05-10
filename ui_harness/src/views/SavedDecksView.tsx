/**
 * SavedDecksView — Phase 4.12b.
 *
 * Lists `mtgdb:decks:*` entries via lib/decks/savedDecks; per-row Load
 * (stages the deck via existing `mtgdb:workspace:import:staged_v1`
 * slot — preserves the 4.6 import bridge) and Delete affordances.
 */
import { useEffect, useState } from "react";
import Button from "../ui/primitives/Button";
import { Card, CardHeader, CardBody } from "../ui/primitives/Card";
import { deleteDeck, listSavedDecks, loadDeck, type SavedDeckIndexEntry } from "../lib/decks/savedDecks";
import { runLegacyMigrationIfNeeded } from "../lib/decks/legacyMigration";

const IMPORT_STAGED_KEY = "mtgdb:workspace:import:staged_v1";

export type SavedDecksViewProps = {
  onBack?: () => void;
};

export default function SavedDecksView({ onBack }: SavedDecksViewProps) {
  const [entries, setEntries] = useState<SavedDeckIndexEntry[]>(() => listSavedDecks());

  // Phase 4 BUNDLE Integration (4.13) Stage 2: one-shot legacy migration on
  // first mount. Idempotent — short-circuits on the
  // `mtgdb:decks:_migration_v1_legacy_imported` flag. After migration runs
  // (whether or not it imported anything), refresh the listing.
  useEffect(() => {
    const result = runLegacyMigrationIfNeeded();
    if (!result.alreadyDone && (result.importedCount > 0 || result.skippedCount > 0)) {
      setEntries(listSavedDecks());
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function refresh() {
    setEntries(listSavedDecks());
  }

  function handleLoad(id: string) {
    const deck = loadDeck(id);
    if (!deck) return;
    // Bridge into the workspace via the existing Phase 4.3/4.6 staged-import
    // localStorage slot so DeckInputPanel populates without rewriting.
    try {
      const payload = {
        commander: "",
        commander_oracle_id: deck.commander_oracle_id ?? null,
        decklist: deck.decklist,
        unknowns: [],
        source: `saved_deck:${id}`,
        staged_at_iso: new Date().toISOString(),
      };
      window.localStorage.setItem(IMPORT_STAGED_KEY, JSON.stringify(payload));
    } catch {
      /* noop — surfaces nothing destructive */
    }
    window.location.hash = "#workspace-decks";
  }

  function handleDelete(id: string) {
    deleteDeck(id);
    refresh();
  }

  return (
    <main className="min-h-screen bg-bg-base text-text-primary p-token-3 md:p-panel-pad" aria-label="Saved decks">
      <header className="flex items-center justify-between mb-token-3">
        <h1 className="text-2xl font-semibold">Saved decks</h1>
        {onBack ? (
          <Button variant="ghost" onClick={onBack} aria-label="Back">
            ← Back
          </Button>
        ) : null}
      </header>

      {entries.length === 0 ? (
        <Card>
          <CardHeader>No saved decks yet</CardHeader>
          <CardBody>
            <p className="text-sm text-text-secondary">
              Save a deck from the workspace to see it here. Decks are stored on this device.
            </p>
          </CardBody>
        </Card>
      ) : (
        <ul className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-token-3" aria-label="Saved deck list">
          {entries.map((entry) => {
            // Phase 4.14 Stage 5: per-row commander surfaced from the full
            // SavedDeck blob (the index entry only carries id/name/savedAt/
            // summary; the full record has commander_oracle_id + decklist).
            // loadDeck() is cheap (per-row localStorage read).
            const full = loadDeck(entry.id);
            const commanderLabel = (() => {
              if (!full || !full.decklist) return null;
              // Heuristic: first non-blank, non-comment line of the saved
              // decklist tends to be the commander entry; fall back to
              // commander_oracle_id ("commander resolved") when present.
              const firstLine = full.decklist.split(/\r?\n/).find((l) => l.trim() !== "" && !l.startsWith("#"));
              if (firstLine && firstLine.trim().length > 0) {
                const m = /^\s*\d*\s*(.+?)\s*$/.exec(firstLine);
                return m && m[1] ? m[1].trim() : null;
              }
              return null;
            })();
            return (
              <li key={entry.id}>
                <Card>
                  <CardHeader>{entry.name}</CardHeader>
                  <CardBody>
                    {commanderLabel ? (
                      <p className="text-sm text-text-secondary">{commanderLabel}</p>
                    ) : null}
                    <p className="text-xs text-text-muted">
                      Saved {new Date(entry.savedAt).toLocaleString()} · {entry.summary.totalCards} cards
                      {entry.summary.bracket ? ` · ${entry.summary.bracket}` : ""}
                    </p>
                    <div className="flex gap-token-2 mt-token-3">
                      <Button variant="primary" onClick={() => handleLoad(entry.id)} aria-label={`Load ${entry.name}`}>
                        Load
                      </Button>
                      <Button variant="ghost" onClick={() => handleDelete(entry.id)} aria-label={`Delete ${entry.name}`}>
                        Delete
                      </Button>
                    </div>
                  </CardBody>
                </Card>
              </li>
            );
          })}
        </ul>
      )}
    </main>
  );
}
