import { useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent } from "react";

import CardSuggestInput from "../CardSuggestInput";
import CardList, { type CardListItem } from "../cards/CardList";
import type { CardSuggestRow, HoverCard, ParsedDecklistRow } from "../workspaceTypes";
import { asArray, asRecord, cardNameSortKey, extractResolveNamesMissingNames, normalizeApiBase, safeParseJson } from "../workspaceUtils";

export type DeckEditorCardHint = {
  oracleId: string;
  typeLine: string | null;
};

const RESOLVE_NAMES_DEBOUNCE_MS = 150;
const RESOLVE_NAMES_MAX_NAMES_PER_REQUEST = 200;

type DeckEditorPanelProps = {
  apiBase: string;
  snapshotId: string;
  cardsInput: string;
  parsedDeckRows: ParsedDecklistRow[];
  deckLineCount?: number;
  deckTextRevision?: number;
  cardHintsByName?: Record<string, DeckEditorCardHint>;
  onCardsInputChange: (value: string) => void;
  onHoverCard: (card: HoverCard | null) => void;
  onResolveNamesMissingChange?: (missingNames: string[]) => void;
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
  onCompleteTo100?: () => void;
  runningCompleteTo100?: boolean;
  disableCompleteActions?: boolean;
  onApplyCompletedDecklist?: () => void;
  canApplyCompletedDecklist?: boolean;
  completionStatus?: string;
  completionAddedCards?: number;
  completionLandsAdded?: number;
  completionError?: string | null;
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

function chunkNamesInOrder(names: string[], maxPerChunk: number): string[][] {
  if (maxPerChunk <= 0) {
    return [names.slice()];
  }

  const chunks: string[][] = [];
  for (let index = 0; index < names.length; index += maxPerChunk) {
    chunks.push(names.slice(index, index + maxPerChunk));
  }
  return chunks;
}

export default function DeckEditorPanel(props: DeckEditorPanelProps) {
  const {
    apiBase,
    snapshotId,
    cardsInput,
    parsedDeckRows,
    deckLineCount,
    deckTextRevision,
    cardHintsByName,
    onCardsInputChange,
    onHoverCard,
    onResolveNamesMissingChange,
    onOpenCard,
    onCompleteTo100,
    runningCompleteTo100 = false,
    disableCompleteActions = false,
    onApplyCompletedDecklist,
    canApplyCompletedDecklist = false,
    completionStatus,
    completionAddedCards,
    completionLandsAdded,
    completionError,
  } = props;

  const [addCardInput, setAddCardInput] = useState("");
  const [copyMessage, setCopyMessage] = useState<string | null>(null);
  const [suggestedHintsByName, setSuggestedHintsByName] = useState<Record<string, DeckEditorCardHint>>({});
  const resolveRequestIdRef = useRef(0);
  const resolveNamesRequestCountRef = useRef(0);
  const resolveNamesResolvedCountRef = useRef(0);

  function logResolveNamesMetrics(extra: Record<string, unknown>): void {
    if (!import.meta.env.DEV) {
      return;
    }

    console.log("[DeckEditorPanel] resolve_names_metrics", {
      resolve_names_request_count: resolveNamesRequestCountRef.current,
      resolve_names_resolved_count: resolveNamesResolvedCountRef.current,
      ...extra,
    });
  }

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

  const unresolvedNames = useMemo(() => {
    const names: string[] = [];
    const seenKeys = new Set<string>();

    for (const row of workingRows) {
      if (normalizeOracleId(row.oracleId) !== "") {
        continue;
      }
      if (Object.prototype.hasOwnProperty.call(mergedHintsByName, row.key)) {
        continue;
      }

      const name = row.name.trim();
      const key = normalizeDeckKey(name);
      if (name === "" || key === "" || seenKeys.has(key)) {
        continue;
      }

      seenKeys.add(key);
      names.push(name);
    }

    return names;
  }, [mergedHintsByName, workingRows]);

  useEffect(() => {
    if (unresolvedNames.length === 0) {
      onResolveNamesMissingChange?.([]);
      return;
    }

    const requestId = resolveRequestIdRef.current + 1;
    resolveRequestIdRef.current = requestId;

    const controllers: AbortController[] = [];
    const normalizedApiBase = normalizeApiBase(apiBase);
    const snapshotToken = snapshotId.trim();
    const nameChunks = chunkNamesInOrder(unresolvedNames, RESOLVE_NAMES_MAX_NAMES_PER_REQUEST);
    const missingNames: string[] = [];
    const missingSeen = new Set<string>();
    let disposed = false;

    logResolveNamesMetrics({
      event: "scheduled",
      requestId,
      unresolved_names_count: unresolvedNames.length,
      chunk_count: nameChunks.length,
    });

    const timerId = window.setTimeout(() => {
      void (async () => {
        const resolvedHintsByKey: Record<string, DeckEditorCardHint> = {};

        for (const [chunkIndex, chunk] of nameChunks.entries()) {
          if (disposed || requestId !== resolveRequestIdRef.current) {
            return;
          }

          const controller = new AbortController();
          controllers.push(controller);

          resolveNamesRequestCountRef.current += 1;
          logResolveNamesMetrics({
            event: "request",
            requestId,
            chunk_index: chunkIndex,
            chunk_size: chunk.length,
          });

          try {
            const response = await fetch(`${normalizedApiBase}/cards/resolve_names`, {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                snapshot_id: snapshotToken,
                names: chunk,
              }),
              signal: controller.signal,
            });

            const text = await response.text();
            const parsed = safeParseJson(text);
            if (!response.ok) {
              logResolveNamesMetrics({
                event: "request_failed",
                requestId,
                chunk_index: chunkIndex,
                status: response.status,
              });
              continue;
            }

            const payload = asRecord(parsed);
            const chunkMissingNames = extractResolveNamesMissingNames(payload);
            for (const missingName of chunkMissingNames) {
              const key = normalizeDeckKey(missingName);
              if (key === "" || missingSeen.has(key)) {
                continue;
              }
              missingSeen.add(key);
              missingNames.push(missingName);
            }
            const resultRows = asArray(payload?.results);
            for (const rawRow of resultRows) {
              const row = asRecord(rawRow);
              if (!row) {
                continue;
              }

              const nameRaw = typeof row.name === "string" && row.name.trim() !== "" ? row.name : row.input;
              const name = typeof nameRaw === "string" ? nameRaw.trim() : "";
              const key = normalizeDeckKey(name);
              const oracleRaw = typeof row.oracle_id === "string" ? row.oracle_id : null;
              const typeLineRaw = typeof row.type_line === "string" ? row.type_line : null;
              const oracleId = normalizeOracleId(oracleRaw);
              if (key === "" || oracleId === "") {
                continue;
              }

              resolvedHintsByKey[key] = {
                oracleId,
                typeLine: normalizeTypeLine(typeLineRaw),
              };
            }
          } catch {
            if (controller.signal.aborted || disposed || requestId !== resolveRequestIdRef.current) {
              return;
            }

            logResolveNamesMetrics({
              event: "request_error",
              requestId,
              chunk_index: chunkIndex,
            });
          }
        }

        if (disposed || requestId !== resolveRequestIdRef.current) {
          return;
        }

        const resolvedCount = Object.keys(resolvedHintsByKey).length;
        resolveNamesResolvedCountRef.current += resolvedCount;
        logResolveNamesMetrics({
          event: "resolved",
          requestId,
          resolved_count: resolvedCount,
          missing_count: missingNames.length,
        });

        onResolveNamesMissingChange?.(missingNames);

        for (const [key, resolvedHint] of Object.entries(resolvedHintsByKey)) {
          setSuggestedHintsByName((previous: Record<string, DeckEditorCardHint>) => {
            const existing = previous[key];
            const merged: DeckEditorCardHint = {
              oracleId: normalizeOracleId(existing?.oracleId) || normalizeOracleId(resolvedHint.oracleId),
              typeLine: normalizeTypeLine(existing?.typeLine) || normalizeTypeLine(resolvedHint.typeLine),
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
      })();
    }, RESOLVE_NAMES_DEBOUNCE_MS);

    return () => {
      disposed = true;
      window.clearTimeout(timerId);
      for (const controller of controllers) {
        controller.abort();
      }
    };
  }, [apiBase, onResolveNamesMissingChange, parsedDeckRows, snapshotId, unresolvedNames]);

  const totalCount = useMemo(() => {
    return workingRows.reduce((acc: number, row: WorkingDeckRow) => acc + row.count, 0);
  }, [workingRows]);

  const normalizedCompletionStatus = useMemo(() => {
    return typeof completionStatus === "string" ? completionStatus.trim() : "";
  }, [completionStatus]);

  const normalizedCompletionAddedCards = useMemo(() => {
    if (typeof completionAddedCards !== "number" || !Number.isFinite(completionAddedCards)) {
      return null;
    }
    return Math.max(0, Math.trunc(completionAddedCards));
  }, [completionAddedCards]);

  const normalizedCompletionLandsAdded = useMemo(() => {
    if (typeof completionLandsAdded !== "number" || !Number.isFinite(completionLandsAdded)) {
      return null;
    }
    return Math.max(0, Math.trunc(completionLandsAdded));
  }, [completionLandsAdded]);

  const normalizedDeckLineCount = useMemo(() => {
    if (typeof deckLineCount !== "number" || !Number.isFinite(deckLineCount)) {
      return parsedDeckRows.length;
    }
    return Math.max(0, Math.trunc(deckLineCount));
  }, [deckLineCount, parsedDeckRows.length]);

  const normalizedDeckTextRevision = useMemo(() => {
    if (typeof deckTextRevision !== "number" || !Number.isFinite(deckTextRevision)) {
      return 0;
    }
    return Math.max(0, Math.trunc(deckTextRevision));
  }, [deckTextRevision]);

  const resolvedArtReadyCount = useMemo(() => {
    let resolvedCount = 0;
    for (const row of workingRows) {
      if (normalizeOracleId(row.oracleId) !== "") {
        resolvedCount += 1;
      }
    }
    return resolvedCount;
  }, [workingRows]);

  const totalResolvableCards = workingRows.length;

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

        <div className="deck-editor-actions">
          <button
            type="button"
            className="deck-editor-copy-button"
            onClick={() => {
              void handleCopyDecklist();
            }}
          >
            Copy
          </button>

          {onCompleteTo100 ? (
            <button
              type="button"
              className="deck-editor-complete-button"
              onClick={onCompleteTo100}
              disabled={runningCompleteTo100 || disableCompleteActions}
            >
              {runningCompleteTo100 ? "Completing..." : "Complete to 100"}
            </button>
          ) : null}

          {onApplyCompletedDecklist ? (
            <button
              type="button"
              className="deck-editor-apply-complete-button"
              onClick={onApplyCompletedDecklist}
              disabled={!canApplyCompletedDecklist || disableCompleteActions}
            >
              Apply Complete
            </button>
          ) : null}
        </div>
      </div>

      {copyMessage ? <p className="workspace-copy-notice">{copyMessage}</p> : null}

      {completionError ? (
        <div className="workspace-error-inline">
          <p>{completionError}</p>
        </div>
      ) : null}

      {normalizedCompletionStatus !== "" || normalizedCompletionAddedCards !== null || normalizedCompletionLandsAdded !== null ? (
        <div className="deck-editor-complete-summary workspace-chip-row">
          {normalizedCompletionStatus !== "" ? (
            <span className="workspace-chip">Complete status: {normalizedCompletionStatus}</span>
          ) : null}
          {normalizedCompletionAddedCards !== null ? (
            <span className="workspace-chip">Added cards: {normalizedCompletionAddedCards}</span>
          ) : null}
          {normalizedCompletionLandsAdded !== null ? (
            <span className="workspace-chip">Added lands: {normalizedCompletionLandsAdded}</span>
          ) : null}
        </div>
      ) : null}

      <p className="workspace-muted">Deck lines: {normalizedDeckLineCount} (rev {normalizedDeckTextRevision})</p>
      <p className="workspace-muted">Resolved: {resolvedArtReadyCount}/{totalResolvableCards} cards (art ready)</p>

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

              const fallbackHint = mergedHintsByName[cardRow.key];
              const resolvedOracleId = normalizeOracleId(cardRow.oracleId) || normalizeOracleId(fallbackHint?.oracleId);
              const resolvedTypeLine = cardRow.typeLine || normalizeTypeLine(fallbackHint?.typeLine);

              if (import.meta.env.DEV) {
                console.log("[DeckEditorPanel] deck_hover_oracle", {
                  name: cardRow.name,
                  has_oracle_id: resolvedOracleId !== "",
                  oracle_id: resolvedOracleId,
                });
              }

              onHoverCard({
                name: cardRow.name,
                oracle_id: resolvedOracleId,
                type_line: resolvedTypeLine,
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
