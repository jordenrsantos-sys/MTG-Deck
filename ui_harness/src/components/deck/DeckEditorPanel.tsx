import { useMemo, useState } from "react";
import type { MouseEvent } from "react";

import CardSuggestInput from "../CardSuggestInput";
import CardList, { type CardListItem } from "../cards/CardList";
import type { CardSuggestRow, HoverCard, ParsedDecklistRow } from "../workspaceTypes";
import { cardNameSortKey } from "../workspaceUtils";

export type DeckEditorCardHint = {
  oracleId: string;
  typeLine: string | null;
};

type DeckEditorPanelProps = {
  apiBase: string;
  snapshotId: string;
  cardsInput: string;
  parsedDeckRows: ParsedDecklistRow[];
  cardHintsByName?: Record<string, DeckEditorCardHint>;
  onCardsInputChange: (value: string) => void;
  onHoverCard: (card: HoverCard | null) => void;
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
};

type WorkingDeckRow = {
  key: string;
  name: string;
  count: number;
  sourceOrder: number;
  oracleId: string;
  typeLine: string | null;
};

function normalizeDeckKey(name: string): string {
  return cardNameSortKey(name);
}

function normalizeOracleId(raw: string | null | undefined): string {
  return typeof raw === "string" ? raw.trim() : "";
}

function normalizeTypeLine(raw: string | null | undefined): string | null {
  if (typeof raw !== "string") {
    return null;
  }
  const token = raw.trim();
  return token === "" ? null : token;
}

function collapseDeckRows(parsedRows: ParsedDecklistRow[]): WorkingDeckRow[] {
  const rowsByKey = new Map<string, WorkingDeckRow>();

  for (const parsedRow of parsedRows) {
    const key = normalizeDeckKey(parsedRow.name);
    if (key === "") {
      continue;
    }

    const safeCount = Number.isFinite(parsedRow.count) ? Math.max(1, Math.trunc(parsedRow.count)) : 1;
    const existing = rowsByKey.get(key);
    if (!existing) {
      rowsByKey.set(key, {
        key,
        name: parsedRow.name.trim(),
        count: safeCount,
        sourceOrder: parsedRow.source_order,
        oracleId: "",
        typeLine: null,
      });
      continue;
    }

    existing.count += safeCount;
    if (parsedRow.source_order < existing.sourceOrder) {
      existing.sourceOrder = parsedRow.source_order;
      existing.name = parsedRow.name.trim();
    }
  }

  return Array.from(rowsByKey.values()).sort((left: WorkingDeckRow, right: WorkingDeckRow) => {
    if (left.sourceOrder !== right.sourceOrder) {
      return left.sourceOrder - right.sourceOrder;
    }
    return left.name.localeCompare(right.name);
  });
}

function stringifyDeckRows(rows: WorkingDeckRow[]): string {
  return rows
    .filter((row: WorkingDeckRow) => row.count > 0 && row.name.trim() !== "")
    .map((row: WorkingDeckRow) => `${Math.max(1, Math.trunc(row.count))} ${row.name}`)
    .join("\n");
}

export default function DeckEditorPanel(props: DeckEditorPanelProps) {
  const {
    apiBase,
    snapshotId,
    cardsInput,
    parsedDeckRows,
    cardHintsByName,
    onCardsInputChange,
    onHoverCard,
    onOpenCard,
  } = props;

  const [addCardInput, setAddCardInput] = useState("");
  const [copyMessage, setCopyMessage] = useState<string | null>(null);
  const [suggestedHintsByName, setSuggestedHintsByName] = useState<Record<string, DeckEditorCardHint>>({});

  const mergedHintsByName = useMemo(() => {
    return {
      ...(cardHintsByName || {}),
      ...suggestedHintsByName,
    };
  }, [cardHintsByName, suggestedHintsByName]);

  const workingRows = useMemo(() => {
    const collapsedRows = collapseDeckRows(parsedDeckRows);
    return collapsedRows.map((row: WorkingDeckRow) => {
      const hint = mergedHintsByName[row.key];
      return {
        ...row,
        oracleId: normalizeOracleId(hint?.oracleId) || row.oracleId,
        typeLine: normalizeTypeLine(hint?.typeLine) || row.typeLine,
      };
    });
  }, [mergedHintsByName, parsedDeckRows]);

  const totalCount = useMemo(() => {
    return workingRows.reduce((acc: number, row: WorkingDeckRow) => acc + row.count, 0);
  }, [workingRows]);

  const rowItems = useMemo(() => {
    return workingRows.map((row: WorkingDeckRow) => {
      return {
        name: row.name,
        oracleId: row.oracleId || null,
        className: "deck-editor-list-row",
        rightMeta: (
          <div className="deck-editor-row-controls">
            <span className="workspace-chip workspace-chip-soft deck-editor-count-chip">x{row.count}</span>
            <button
              type="button"
              className="deck-editor-stepper"
              aria-label={`Decrease count for ${row.name}`}
              onMouseDown={(event: MouseEvent<HTMLButtonElement>) => {
                event.preventDefault();
                event.stopPropagation();
              }}
              onClick={(event: MouseEvent<HTMLButtonElement>) => {
                event.preventDefault();
                event.stopPropagation();
                handleAdjustCardCount(row.key, -1);
              }}
            >
              −
            </button>
            <button
              type="button"
              className="deck-editor-stepper"
              aria-label={`Increase count for ${row.name}`}
              onMouseDown={(event: MouseEvent<HTMLButtonElement>) => {
                event.preventDefault();
                event.stopPropagation();
              }}
              onClick={(event: MouseEvent<HTMLButtonElement>) => {
                event.preventDefault();
                event.stopPropagation();
                handleAdjustCardCount(row.key, 1);
              }}
            >
              +
            </button>
          </div>
        ),
      } satisfies CardListItem;
    });
  }, [workingRows]);

  function updateCardsInputFromRows(rows: WorkingDeckRow[]): void {
    onCardsInputChange(stringifyDeckRows(rows));
  }

  function learnSuggestedCard(row: CardSuggestRow): void {
    const key = normalizeDeckKey(row.name);
    if (key === "") {
      return;
    }

    const nextOracleId = normalizeOracleId(row.oracle_id);
    const nextTypeLine = normalizeTypeLine(row.type_line);
    if (nextOracleId === "" && nextTypeLine === null) {
      return;
    }

    setSuggestedHintsByName((previous: Record<string, DeckEditorCardHint>) => {
      const existing = previous[key];
      const merged: DeckEditorCardHint = {
        oracleId: normalizeOracleId(existing?.oracleId) || nextOracleId,
        typeLine: normalizeTypeLine(existing?.typeLine) || nextTypeLine,
      };

      if (existing && existing.oracleId === merged.oracleId && existing.typeLine === merged.typeLine) {
        return previous;
      }

      return {
        ...previous,
        [key]: merged,
      };
    });
  }

  function handleAddCardFromSuggest(row: CardSuggestRow): void {
    learnSuggestedCard(row);
    setAddCardInput("");

    const rowKey = normalizeDeckKey(row.name);
    if (rowKey === "") {
      return;
    }

    let found = false;
    const nextRows = workingRows.map((entry: WorkingDeckRow) => {
      if (entry.key !== rowKey) {
        return entry;
      }
      found = true;
      return {
        ...entry,
        count: entry.count + 1,
        oracleId: entry.oracleId || normalizeOracleId(row.oracle_id),
        typeLine: entry.typeLine || normalizeTypeLine(row.type_line),
      };
    });

    if (!found) {
      const maxSourceOrder = workingRows.reduce(
        (maxValue: number, rowEntry: WorkingDeckRow) => Math.max(maxValue, rowEntry.sourceOrder),
        -1,
      );
      nextRows.push({
        key: rowKey,
        name: row.name,
        count: 1,
        sourceOrder: maxSourceOrder + 1,
        oracleId: normalizeOracleId(row.oracle_id),
        typeLine: normalizeTypeLine(row.type_line),
      });
    }

    updateCardsInputFromRows(nextRows);
  }

  function handleAdjustCardCount(rowKey: string, delta: number): void {
    if (!Number.isFinite(delta) || Math.trunc(delta) === 0) {
      return;
    }

    const nextRows: WorkingDeckRow[] = [];
    for (const row of workingRows) {
      if (row.key !== rowKey) {
        nextRows.push(row);
        continue;
      }

      const nextCount = row.count + Math.trunc(delta);
      if (nextCount <= 0) {
        continue;
      }

      nextRows.push({
        ...row,
        count: nextCount,
      });
    }

    updateCardsInputFromRows(nextRows);
  }

  async function handleCopyDecklist(): Promise<void> {
    if (!navigator.clipboard || typeof navigator.clipboard.writeText !== "function") {
      setCopyMessage("Clipboard API unavailable in this browser context.");
      return;
    }

    try {
      await navigator.clipboard.writeText(cardsInput);
      setCopyMessage("Decklist copied.");
    } catch {
      setCopyMessage("Failed to copy decklist.");
    }
  }

  return (
    <section className="workspace-panel workspace-panel-content deck-editor-panel">
      <div className="deck-editor-header">
        <div className="deck-editor-title-wrap">
          <h3>Deck</h3>
          <span className="workspace-chip deck-editor-total-chip">{totalCount}</span>
        </div>

        <button
          type="button"
          className="deck-editor-copy-button"
          onClick={() => {
            void handleCopyDecklist();
          }}
        >
          Copy
        </button>
      </div>

      {copyMessage ? <p className="workspace-copy-notice">{copyMessage}</p> : null}

      <CardSuggestInput
        label="Add card"
        value={addCardInput}
        placeholder="Search cards to add"
        apiBase={apiBase}
        snapshotId={snapshotId}
        limit={20}
        onChange={setAddCardInput}
        onSelect={handleAddCardFromSuggest}
        onHoverCard={(row: CardSuggestRow | null) => {
          if (!row) {
            onHoverCard(null);
            return;
          }

          onHoverCard({
            name: row.name,
            oracle_id: row.oracle_id,
            type_line: row.type_line,
            primitive_tags: [],
            source: "suggest",
          });
        }}
      />

      <div className="deck-editor-list-wrap">
        {workingRows.length === 0 ? (
          <p className="workspace-muted">No cards in the working deck yet.</p>
        ) : (
          <CardList
            items={rowItems}
            className="deck-editor-card-list"
            ariaLabel="Working deck cards"
            onOpenCard={onOpenCard}
            onRowMouseEnter={(_, index: number) => {
              const cardRow = workingRows[index];
              if (!cardRow) {
                return;
              }

              onHoverCard({
                name: cardRow.name,
                oracle_id: cardRow.oracleId,
                type_line: cardRow.typeLine,
                primitive_tags: [],
                source: "deck",
              });
            }}
            onRowMouseLeave={() => {
              onHoverCard(null);
            }}
          />
        )}
      </div>
    </section>
  );
}
