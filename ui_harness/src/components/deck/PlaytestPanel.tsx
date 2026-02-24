import { useEffect, useMemo, useState } from "react";

import CardList, { type CardListItem } from "../cards/CardList";
import type { HoverCard, ParsedDecklistRow } from "../workspaceTypes";
import { cardNameSortKey } from "../workspaceUtils";
import type { DeckEditorCardHint } from "./DeckEditorPanel";

type PlaytestPanelProps = {
  parsedDeckRows: ParsedDecklistRow[];
  cardHintsByName?: Record<string, DeckEditorCardHint>;
  buildHashV1?: string | null;
  onHoverCard: (card: HoverCard | null) => void;
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
};

type PlaytestCardInstance = {
  instanceId: string;
  name: string;
  oracleId: string;
  typeLine: string | null;
};

type DeckSeedEntry = {
  name: string;
  count: number;
};

function normalizeOracleId(value: string | null | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeTypeLine(value: string | null | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const token = value.trim();
  return token === "" ? null : token;
}

function normalizeDeckName(value: string | null | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeDeckCount(value: number): number {
  return Number.isFinite(value) ? Math.max(1, Math.trunc(value)) : 1;
}

function stableHash32(value: string): number {
  let hash = 2166136261 >>> 0;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash >>> 0;
}

function makeMulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let token = Math.imul(state ^ (state >>> 15), 1 | state);
    token ^= token + Math.imul(token ^ (token >>> 7), 61 | token);
    return ((token ^ (token >>> 14)) >>> 0) / 4294967296;
  };
}

function buildCardPool(
  parsedDeckRows: ParsedDecklistRow[],
  cardHintsByName: Record<string, DeckEditorCardHint> | undefined,
): PlaytestCardInstance[] {
  const pool: PlaytestCardInstance[] = [];

  for (const row of parsedDeckRows) {
    const name = normalizeDeckName(row.name);
    if (name === "") {
      continue;
    }

    const key = cardNameSortKey(name);
    if (key === "") {
      continue;
    }

    const hint = cardHintsByName?.[key];
    const oracleId = normalizeOracleId(hint?.oracleId);
    const typeLine = normalizeTypeLine(hint?.typeLine);
    const copies = normalizeDeckCount(row.count);

    for (let instance = 0; instance < copies; instance += 1) {
      pool.push({
        instanceId: `${row.source_order}-${key}-${instance}`,
        name,
        oracleId,
        typeLine,
      });
    }
  }

  return pool;
}

function buildStableDeckSeedToken(parsedDeckRows: ParsedDecklistRow[]): string {
  const entriesByKey = new Map<string, DeckSeedEntry>();

  for (const row of parsedDeckRows) {
    const name = normalizeDeckName(row.name);
    if (name === "") {
      continue;
    }

    const key = cardNameSortKey(name);
    if (key === "") {
      continue;
    }

    const copies = normalizeDeckCount(row.count);
    const existing = entriesByKey.get(key);
    if (!existing) {
      entriesByKey.set(key, {
        name,
        count: copies,
      });
      continue;
    }

    existing.count += copies;
    if (name.localeCompare(existing.name) < 0) {
      existing.name = name;
    }
  }

  return Array.from(entriesByKey.entries())
    .sort((left: [string, DeckSeedEntry], right: [string, DeckSeedEntry]) => left[0].localeCompare(right[0]))
    .map(([key, entry]: [string, DeckSeedEntry]) => `${key}|${entry.count}|${entry.name}`)
    .join("\n");
}

function seededShuffle(cards: PlaytestCardInstance[], seedNumber: number): PlaytestCardInstance[] {
  const out = cards.slice();
  const random = makeMulberry32(seedNumber);
  for (let index = out.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(random() * (index + 1));
    const tmp = out[index];
    out[index] = out[swapIndex];
    out[swapIndex] = tmp;
  }
  return out;
}

function drawOpeningHand(pool: PlaytestCardInstance[]): { hand: PlaytestCardInstance[]; library: PlaytestCardInstance[] } {
  const drawCount = Math.min(7, pool.length);
  return {
    hand: pool.slice(0, drawCount),
    library: pool.slice(drawCount),
  };
}

export default function PlaytestPanel(props: PlaytestPanelProps) {
  const { parsedDeckRows, cardHintsByName, buildHashV1, onHoverCard, onOpenCard } = props;

  const [shuffleIndex, setShuffleIndex] = useState(0);
  const [handCards, setHandCards] = useState<PlaytestCardInstance[]>([]);
  const [libraryCards, setLibraryCards] = useState<PlaytestCardInstance[]>([]);

  const cardPool = useMemo(() => {
    return buildCardPool(parsedDeckRows, cardHintsByName);
  }, [cardHintsByName, parsedDeckRows]);

  const fallbackDeckSeedToken = useMemo(() => {
    return buildStableDeckSeedToken(parsedDeckRows);
  }, [parsedDeckRows]);

  const fallbackDeckSeedHash = useMemo(() => {
    return stableHash32(fallbackDeckSeedToken).toString(16).padStart(8, "0");
  }, [fallbackDeckSeedToken]);

  const baseSeedToken = useMemo(() => {
    const buildHash = typeof buildHashV1 === "string" ? buildHashV1.trim() : "";
    return buildHash !== "" ? buildHash : fallbackDeckSeedHash;
  }, [buildHashV1, fallbackDeckSeedHash]);

  const shuffledPool = useMemo(() => {
    const seedNumber = stableHash32(`${baseSeedToken}#${shuffleIndex}`);
    return seededShuffle(cardPool, seedNumber);
  }, [baseSeedToken, cardPool, shuffleIndex]);

  useEffect(() => {
    setShuffleIndex(0);
  }, [baseSeedToken]);

  useEffect(() => {
    const opening = drawOpeningHand(shuffledPool);
    setHandCards(opening.hand);
    setLibraryCards(opening.library);
  }, [shuffledPool]);

  const handItems = useMemo(() => {
    return handCards.map((card: PlaytestCardInstance, index: number) => {
      return {
        name: card.name,
        oracleId: card.oracleId || null,
        className: "playtest-hand-row",
        rightMeta: <span className="workspace-chip workspace-chip-soft">#{index + 1}</span>,
      } satisfies CardListItem;
    });
  }, [handCards]);

  function handleDrawSeven(): void {
    const opening = drawOpeningHand(shuffledPool);
    setHandCards(opening.hand);
    setLibraryCards(opening.library);
  }

  function handleDrawOne(): void {
    if (libraryCards.length === 0) {
      return;
    }
    const nextCard = libraryCards[0];
    if (!nextCard) {
      return;
    }

    setLibraryCards((previous: PlaytestCardInstance[]) => previous.slice(1));
    setHandCards((previous: PlaytestCardInstance[]) => [...previous, nextCard]);
  }

  function handleNewHand(): void {
    setShuffleIndex((previous: number) => previous + 1);
  }

  return (
    <section
      className="workspace-panel workspace-panel-content playtest-panel"
      onMouseLeave={() => {
        onHoverCard(null);
      }}
    >
      <div className="playtest-header">
        <h3>Playtest</h3>
        <div className="workspace-chip-row">
          <span className="workspace-chip">Pool: {cardPool.length}</span>
          <span className="workspace-chip">Library: {libraryCards.length}</span>
          <span className="workspace-chip">Hand: {handCards.length}</span>
        </div>
      </div>

      <div className="playtest-actions workspace-action-row">
        <button type="button" onClick={handleDrawSeven} disabled={cardPool.length === 0}>
          Draw 7
        </button>
        <button type="button" onClick={handleDrawOne} disabled={libraryCards.length === 0}>
          Draw 1
        </button>
        <button type="button" onClick={handleNewHand} disabled={cardPool.length === 0}>
          New Hand
        </button>
      </div>

      <p className="workspace-muted playtest-seed">
        seed: {baseSeedToken === "" ? "(empty deck)" : `${baseSeedToken}#${shuffleIndex}`}
      </p>

      <div className="playtest-hand-wrap">
        {handCards.length === 0 ? (
          <p className="workspace-muted">No cards drawn yet.</p>
        ) : (
          <CardList
            items={handItems}
            className="playtest-hand-list"
            ariaLabel="Playtest hand"
            onOpenCard={onOpenCard}
            onRowMouseEnter={(_, index: number) => {
              const card = handCards[index];
              if (!card) {
                return;
              }

              onHoverCard({
                name: card.name,
                oracle_id: card.oracleId,
                type_line: card.typeLine,
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
