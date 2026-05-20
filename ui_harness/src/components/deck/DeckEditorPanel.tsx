import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { MouseEvent } from "react";

import CardSuggestInput from "../CardSuggestInput";
import CardList, { type CardListItem } from "../cards/CardList";
import Badge from "../../ui/primitives/Badge";
import { GAME_CHANGER_TOOLTIP_TEXT } from "../stats/AddedCardRow";
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
  commanderName?: string;
  commanderOracleId?: string | null;
  cardsInput: string;
  parsedDeckRows: ParsedDecklistRow[];
  deckLineCount?: number;
  deckTextRevision?: number;
  cardHintsByName?: Record<string, DeckEditorCardHint>;
  onCardsInputChange: (value: string) => void;
  onHoverCard: (card: HoverCard | null) => void;
  onResolveNamesMissingChange?: (missingNames: string[]) => void;
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
  onCommanderChange?: (commanderName: string) => void;
  onCompleteTo100?: () => void;
  runningCompleteTo100?: boolean;
  disableCompleteActions?: boolean;
  onApplyCompletedDecklist?: () => void;
  canApplyCompletedDecklist?: boolean;
  completionStatus?: string;
  completionAddedCards?: number;
  completionLandsAdded?: number;
  completionError?: string | null;
  savedDeckNames?: string[];
  selectedSavedDeckName?: string;
  onSelectedSavedDeckNameChange?: (deckName: string) => void;
  onSaveDeck?: () => void;
  onLoadSavedDeck?: (deckName: string) => void;
  onRenameSavedDeck?: (deckName: string) => void;
  onDeleteSavedDeck?: (deckName: string) => void;
  /** Game-Changer name set sourced from /deck/complete_v1's game_changers_v1.
   *  When provided, deck-list rows whose card name appears in the set render
   *  an amber "GC" badge next to the name. Defaults to no badges when
   *  omitted (back-compat for direct callers in tests/other surfaces). */
  gameChangers?: ReadonlySet<string>;
};

type WorkingDeckRow = {
  key: string;
  name: string;
  count: number;
  sourceOrder: number;
  oracleId: string;
  typeLine: string | null;
};

type DeckEditorTypeGroup =
  | "Creature"
  | "Instant"
  | "Sorcery"
  | "Artifact"
  | "Enchantment"
  | "Planeswalker"
  | "Battle"
  | "Land"
  | "Other";

type DeckEditorColumn = "core" | "spells" | "lands";

type DeckEditorGroupSection = {
  group: DeckEditorTypeGroup;
  title: string;
  rows: WorkingDeckRow[];
  totalCount: number;
};

const DECK_EDITOR_GROUP_ORDER: DeckEditorTypeGroup[] = [
  "Creature",
  "Instant",
  "Sorcery",
  "Artifact",
  "Enchantment",
  "Planeswalker",
  "Battle",
  "Land",
  "Other",
];

const DECK_EDITOR_TYPE_TOKEN_TO_GROUP: Record<string, DeckEditorTypeGroup> = {
  creature: "Creature",
  instant: "Instant",
  sorcery: "Sorcery",
  artifact: "Artifact",
  enchantment: "Enchantment",
  planeswalker: "Planeswalker",
  battle: "Battle",
  land: "Land",
};

function resolveDeckEditorGroup(typeLine: string | null | undefined): DeckEditorTypeGroup {
  const normalizedTypeLine = normalizeTypeLine(typeLine);
  if (normalizedTypeLine === null) {
    return "Other";
  }

  const leftOfDash = normalizedTypeLine.split(/[—-]/)[0]?.trim() || "";
  if (leftOfDash === "") {
    return "Other";
  }

  const typeTokens = leftOfDash
    .toLowerCase()
    .split(/\s+/)
    .map((token: string) => token.trim())
    .filter(Boolean);

  for (const token of typeTokens) {
    const resolvedGroup = DECK_EDITOR_TYPE_TOKEN_TO_GROUP[token];
    if (resolvedGroup) {
      return resolvedGroup;
    }
  }

  return "Other";
}

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
    commanderName,
    commanderOracleId,
    cardsInput,
    parsedDeckRows,
    deckLineCount,
    deckTextRevision,
    cardHintsByName,
    onCardsInputChange,
    onHoverCard,
    onResolveNamesMissingChange,
    onOpenCard,
    onCommanderChange,
    onCompleteTo100,
    runningCompleteTo100 = false,
    disableCompleteActions = false,
    onApplyCompletedDecklist,
    canApplyCompletedDecklist = false,
    completionStatus,
    completionAddedCards,
    completionLandsAdded,
    completionError,
    savedDeckNames = [],
    selectedSavedDeckName = "",
    onSelectedSavedDeckNameChange,
    onSaveDeck,
    onLoadSavedDeck,
    onRenameSavedDeck,
    onDeleteSavedDeck,
    gameChangers,
  } = props;

  const [addCardInput, setAddCardInput] = useState("");
  const [commanderInput, setCommanderInput] = useState(() => (typeof commanderName === "string" ? commanderName.trim() : ""));
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

  const normalizedCommanderName = useMemo(() => {
    return typeof commanderName === "string" ? commanderName.trim() : "";
  }, [commanderName]);

  useEffect(() => {
    setCommanderInput(normalizedCommanderName);
  }, [normalizedCommanderName]);

  const normalizedCommanderOracleId = useMemo(() => normalizeOracleId(commanderOracleId), [commanderOracleId]);

  const normalizedCommanderKey = useMemo(() => normalizeDeckKey(normalizedCommanderName), [normalizedCommanderName]);

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

    if (normalizedCommanderName !== "" && normalizedCommanderKey !== "") {
      seenKeys.add(normalizedCommanderKey);
      const commanderHint = mergedHintsByName[normalizedCommanderKey];
      const commanderHasOracleId = normalizedCommanderOracleId !== "" || normalizeOracleId(commanderHint?.oracleId) !== "";
      if (!commanderHasOracleId) {
        names.push(normalizedCommanderName);
      }
    }

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
  }, [mergedHintsByName, normalizedCommanderKey, normalizedCommanderName, normalizedCommanderOracleId, workingRows]);

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

  const commanderHint = useMemo(() => {
    if (normalizedCommanderKey === "") {
      return undefined;
    }
    return mergedHintsByName[normalizedCommanderKey];
  }, [mergedHintsByName, normalizedCommanderKey]);

  const resolvedCommanderOracleId = useMemo(() => {
    return normalizedCommanderOracleId || normalizeOracleId(commanderHint?.oracleId);
  }, [commanderHint, normalizedCommanderOracleId]);

  const resolvedCommanderTypeLine = useMemo(() => {
    return normalizeTypeLine(commanderHint?.typeLine);
  }, [commanderHint]);

  const commanderItems = useMemo(() => {
    if (normalizedCommanderName === "") {
      return [] as CardListItem[];
    }

    return [
      {
        name: normalizedCommanderName,
        oracleId: resolvedCommanderOracleId || null,
        className: "deck-commander-row",
      } satisfies CardListItem,
    ];
  }, [normalizedCommanderName, resolvedCommanderOracleId]);

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
  const savedDeckStatusId = useId();

  const normalizedSavedDeckName = useMemo(() => selectedSavedDeckName.trim(), [selectedSavedDeckName]);

  const hasSavedDeckOptions = savedDeckNames.length > 0;

  const hasMatchingSavedDeck = useMemo(() => {
    if (normalizedSavedDeckName === "") {
      return false;
    }

    const normalizedSelection = normalizedSavedDeckName.toLowerCase();
    return savedDeckNames.some((deckName: string) => deckName.trim().toLowerCase() === normalizedSelection);
  }, [normalizedSavedDeckName, savedDeckNames]);

  const canOpenSavedDeck = hasMatchingSavedDeck && Boolean(onLoadSavedDeck);
  const canRenameSavedDeck = hasMatchingSavedDeck && Boolean(onRenameSavedDeck);
  const canDeleteSavedDeck = hasMatchingSavedDeck && Boolean(onDeleteSavedDeck);
  const hasMissingSavedDeckSelection = normalizedSavedDeckName !== "" && !hasMatchingSavedDeck;
  const canSelectSavedDeck = hasSavedDeckOptions || hasMissingSavedDeckSelection;

  const matchedSavedDeckName = useMemo(() => {
    if (!hasMatchingSavedDeck) {
      return "";
    }

    const normalizedSelection = normalizedSavedDeckName.toLowerCase();
    const matchedDeck =
      savedDeckNames.find((deckName: string) => deckName.trim().toLowerCase() === normalizedSelection)?.trim() || "";
    return matchedDeck || normalizedSavedDeckName;
  }, [hasMatchingSavedDeck, normalizedSavedDeckName, savedDeckNames]);

  const savedDeckStatusMessage = useMemo(() => {
    if (hasMissingSavedDeckSelection) {
      return `Saved deck "${normalizedSavedDeckName}" is no longer available. Select another saved deck or clear the selection.`;
    }
    if (!hasSavedDeckOptions) {
      return "No saved decks yet. Save the current deck to create one.";
    }
    if (matchedSavedDeckName === "") {
      const deckCount = savedDeckNames.length;
      return `${deckCount} saved deck${deckCount === 1 ? "" : "s"} available.`;
    }
    return `Selected saved deck: "${matchedSavedDeckName}".`;
  }, [hasMissingSavedDeckSelection, hasSavedDeckOptions, matchedSavedDeckName, normalizedSavedDeckName, savedDeckNames.length]);

  const savedDeckSelectValue = useMemo(() => {
    if (hasMatchingSavedDeck) {
      return matchedSavedDeckName;
    }
    if (hasMissingSavedDeckSelection) {
      return normalizedSavedDeckName;
    }
    return "";
  }, [hasMatchingSavedDeck, hasMissingSavedDeckSelection, matchedSavedDeckName, normalizedSavedDeckName]);

  const groupedSections = useMemo(() => {
    const rowsByGroup = new Map<DeckEditorTypeGroup, WorkingDeckRow[]>();
    for (const group of DECK_EDITOR_GROUP_ORDER) {
      rowsByGroup.set(group, []);
    }

    for (const row of workingRows) {
      const group = resolveDeckEditorGroup(row.typeLine);
      const bucket = rowsByGroup.get(group);
      if (bucket) {
        bucket.push(row);
      }
    }

    const sections: DeckEditorGroupSection[] = [];
    for (const group of DECK_EDITOR_GROUP_ORDER) {
      const rows = rowsByGroup.get(group) || [];
      if (rows.length === 0) {
        continue;
      }

      const sortedRows = [...rows].sort((left: WorkingDeckRow, right: WorkingDeckRow) => left.name.localeCompare(right.name));
      const totalSectionCount = sortedRows.reduce((acc: number, row: WorkingDeckRow) => acc + Math.max(1, Math.trunc(row.count)), 0);

      sections.push({
        group,
        title: group,
        rows: sortedRows,
        totalCount: totalSectionCount,
      });
    }

    return sections;
  }, [workingRows]);

  const sectionsByColumn = useMemo(() => {
    const columns: Record<DeckEditorColumn, DeckEditorGroupSection[]> = {
      core: [],
      spells: [],
      lands: [],
    };

    const columnWeights: Record<DeckEditorColumn, number> = {
      core: commanderItems.length > 0 ? 2 : 0,
      spells: 0,
      lands: 0,
    };

    const sectionWeight = (section: DeckEditorGroupSection): number => {
      return section.totalCount + section.rows.length * 0.35;
    };

    for (const section of groupedSections) {
      if (section.group === "Land") {
        columns.lands.push(section);
        columnWeights.lands += sectionWeight(section);
        continue;
      }

      const targetColumn: DeckEditorColumn = columnWeights.core <= columnWeights.spells ? "core" : "spells";
      columns[targetColumn].push(section);
      columnWeights[targetColumn] += sectionWeight(section);
    }

    return columns;
  }, [commanderItems.length, groupedSections]);

  const deckContextOracleIds = useMemo(() => {
    const seen = new Set<string>();
    const oracleIds: string[] = [];

    for (const row of workingRows) {
      const fallbackHint = mergedHintsByName[row.key];
      const resolvedOracleId = normalizeOracleId(row.oracleId) || normalizeOracleId(fallbackHint?.oracleId);
      if (resolvedOracleId === "" || seen.has(resolvedOracleId)) {
        continue;
      }
      seen.add(resolvedOracleId);
      oracleIds.push(resolvedOracleId);
    }

    return oracleIds;
  }, [mergedHintsByName, workingRows]);

  const handleOpenDeckEditorCard = useMemo(() => {
    if (!onOpenCard) {
      return undefined;
    }

    return (oracleId: string) => {
      onOpenCard(oracleId, deckContextOracleIds);
    };
  }, [deckContextOracleIds, onOpenCard]);

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

  function handleHoverDeckRow(cardRow: WorkingDeckRow | undefined): void {
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
  }

  function buildDeckEditorRowItem(row: WorkingDeckRow): CardListItem {
    const displayCount = Math.max(1, Math.trunc(row.count));
    const trimmedName = row.name.trim();
    const isGameChanger =
      gameChangers !== undefined && trimmedName !== "" && gameChangers.has(trimmedName);

    return {
      name: `${displayCount} ${row.name}`,
      oracleId: row.oracleId || null,
      className: "deck-editor-list-row",
      nameBadge: isGameChanger ? (
        <span
          data-game-changer="true"
          title={GAME_CHANGER_TOOLTIP_TEXT}
          aria-label={GAME_CHANGER_TOOLTIP_TEXT}
          className="inline-flex"
        >
          <Badge variant="warn">GC</Badge>
        </span>
      ) : null,
      rightMeta: (
        <div className="deck-editor-row-controls">
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
  }

  function renderDeckEditorSection(section: DeckEditorGroupSection) {
    const sectionItems = section.rows.map((row: WorkingDeckRow) => buildDeckEditorRowItem(row));

    return (
      <section className="deck-editor-section" key={`deck-editor-group-${section.group}`}>
        <div className="deck-editor-section-header">
          <h4 className="deck-editor-section-title">{section.title}</h4>
          <span className="deck-editor-section-count">{section.totalCount}</span>
        </div>

        <CardList
          items={sectionItems}
          className="deck-editor-card-list deck-editor-section-card-list"
          ariaLabel={`${section.title} cards`}
          onOpenCard={handleOpenDeckEditorCard}
          onRowMouseEnter={(_, index: number) => {
            const cardRow = section.rows[index];
            handleHoverDeckRow(cardRow);
          }}
          onRowMouseLeave={() => {
            onHoverCard(null);
          }}
        />
      </section>
    );
  }

  function commitCommanderChange(nextCommanderRaw: string): void {
    if (!onCommanderChange) {
      return;
    }

    const nextCommanderName = nextCommanderRaw.trim();
    if (nextCommanderName === "" || nextCommanderName === normalizedCommanderName) {
      return;
    }

    onCommanderChange(nextCommanderName);
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
          {onSaveDeck || onLoadSavedDeck || onRenameSavedDeck || onDeleteSavedDeck ? (
            <div className="deck-editor-saved-actions-group">
              <div className="deck-editor-saved-actions">
                <button
                  type="button"
                  className="deck-editor-save-button"
                  onClick={onSaveDeck}
                  disabled={!onSaveDeck}
                >
                  Save Deck
                </button>

                <label className="deck-editor-saved-select-wrap">
                  <span className="sr-only">Saved decks</span>
                  <select
                    className={`deck-editor-saved-select${hasMissingSavedDeckSelection ? " deck-editor-saved-select-warning" : ""}`}
                    aria-describedby={savedDeckStatusId}
                    aria-invalid={hasMissingSavedDeckSelection}
                    aria-errormessage={hasMissingSavedDeckSelection ? savedDeckStatusId : undefined}
                    value={savedDeckSelectValue}
                    onChange={(event) => {
                      onSelectedSavedDeckNameChange?.(event.target.value);
                    }}
                    disabled={!canSelectSavedDeck}
                  >
                    <option value="">Saved decks...</option>
                    {hasMissingSavedDeckSelection ? <option value={normalizedSavedDeckName}>[Missing] {normalizedSavedDeckName}</option> : null}
                    {savedDeckNames.map((deckName: string) => (
                      <option key={`saved-deck-option-${deckName}`} value={deckName}>
                        {deckName}
                      </option>
                    ))}
                  </select>
                </label>

                <button
                  type="button"
                  className="deck-editor-open-button"
                  onClick={() => {
                    if (!canOpenSavedDeck || !onLoadSavedDeck) {
                      return;
                    }
                    onLoadSavedDeck(normalizedSavedDeckName);
                  }}
                  disabled={!canOpenSavedDeck}
                >
                  Open
                </button>

                <button
                  type="button"
                  className="deck-editor-rename-button"
                  onClick={() => {
                    if (!canRenameSavedDeck || !onRenameSavedDeck) {
                      return;
                    }
                    onRenameSavedDeck(normalizedSavedDeckName);
                  }}
                  disabled={!canRenameSavedDeck}
                >
                  Rename
                </button>

                <button
                  type="button"
                  className="deck-editor-delete-button"
                  onClick={() => {
                    if (!canDeleteSavedDeck || !onDeleteSavedDeck) {
                      return;
                    }
                    onDeleteSavedDeck(normalizedSavedDeckName);
                  }}
                  disabled={!canDeleteSavedDeck}
                >
                  Delete
                </button>

                {hasMissingSavedDeckSelection && onSelectedSavedDeckNameChange ? (
                  <button
                    type="button"
                    className="deck-editor-clear-selection-button"
                    onClick={() => {
                      onSelectedSavedDeckNameChange("");
                    }}
                  >
                    Clear
                  </button>
                ) : null}
              </div>

              <p
                id={savedDeckStatusId}
                className={`workspace-muted deck-editor-saved-status${hasMissingSavedDeckSelection ? " deck-editor-saved-status-warning" : ""}`}
                aria-live="polite"
                aria-atomic="true"
              >
                {savedDeckStatusMessage}
              </p>
            </div>
          ) : null}

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

      <CardSuggestInput
        label="Add card"
        value={addCardInput}
        placeholder="Add a card by name..."
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

      {normalizedCompletionStatus !== "" || normalizedCompletionAddedCards !== null || normalizedCompletionLandsAdded !== null ? (
        <div className="deck-editor-complete-summary workspace-chip-row">
          {normalizedCompletionStatus !== "" ? <span className="workspace-chip">Complete status: {normalizedCompletionStatus}</span> : null}
          {normalizedCompletionAddedCards !== null ? <span className="workspace-chip">Added cards: {normalizedCompletionAddedCards}</span> : null}
          {normalizedCompletionLandsAdded !== null ? <span className="workspace-chip">Added lands: {normalizedCompletionLandsAdded}</span> : null}
        </div>
      ) : null}

      <div className="workspace-chip-row deck-editor-stats-row">
        <span className="workspace-chip">Deck lines: {normalizedDeckLineCount} (rev {normalizedDeckTextRevision})</span>
        <span className="workspace-chip">Resolved art: {resolvedArtReadyCount}/{totalResolvableCards}</span>
      </div>

      <div className="deck-editor-list-wrap">
        <div className="deck-editor-board">
          <div className="deck-editor-column deck-editor-column-core">
            <section className="deck-editor-section deck-commander-block deck-editor-commander-wrap">
              <div className="deck-editor-section-header">
                <h4 className="deck-editor-section-title">Commander</h4>
                <span className="deck-editor-section-count">{commanderItems.length > 0 ? "1" : "0"}</span>
              </div>

              {commanderItems.length > 0 ? (
                <CardList
                  items={commanderItems}
                  className="deck-editor-card-list deck-editor-section-card-list"
                  ariaLabel="Deck commander"
                  onOpenCard={onOpenCard}
                  onRowMouseEnter={() => {
                    onHoverCard({
                      name: normalizedCommanderName,
                      oracle_id: resolvedCommanderOracleId,
                      type_line: resolvedCommanderTypeLine,
                      primitive_tags: [],
                      source: "deck",
                    });
                  }}
                  onRowMouseLeave={() => {
                    onHoverCard(null);
                  }}
                />
              ) : (
                <p className="workspace-muted">No commander selected.</p>
              )}

              <div className="deck-editor-commander-controls">
                <CardSuggestInput
                  label="Change commander"
                  value={commanderInput}
                  placeholder="Search commander"
                  apiBase={apiBase}
                  snapshotId={snapshotId}
                  limit={12}
                  commanderOnly
                  onChange={setCommanderInput}
                  onSelect={(row: CardSuggestRow) => {
                    learnSuggestedCard(row);
                    commitCommanderChange(row.name);
                  }}
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

                <div className="workspace-action-row deck-editor-commander-set-row">
                  <button
                    type="button"
                    className="workspace-link-button"
                    onClick={() => {
                      commitCommanderChange(commanderInput);
                    }}
                    disabled={!onCommanderChange || commanderInput.trim() === "" || commanderInput.trim() === normalizedCommanderName}
                  >
                    Set Commander
                  </button>
                </div>
              </div>
            </section>

            {sectionsByColumn.core.map((section: DeckEditorGroupSection) => renderDeckEditorSection(section))}

            {groupedSections.length === 0 ? <p className="workspace-muted deck-editor-empty-message">No cards in the working deck yet.</p> : null}
          </div>

          <div className="deck-editor-column deck-editor-column-spells">
            {sectionsByColumn.spells.map((section: DeckEditorGroupSection) => renderDeckEditorSection(section))}
          </div>

          <div className="deck-editor-column deck-editor-column-lands">
            {sectionsByColumn.lands.map((section: DeckEditorGroupSection) => renderDeckEditorSection(section))}
          </div>
        </div>
      </div>
    </section>
  );
}
