/**
 * GroupedDeckList — Phase 4.6.
 *
 * Top-level composition: GroupBySelector + sectioned CardListSection
 * rendering driven by the grouping pure functions + per-deck-override
 * preference. Self-contained — embeddable in WorkspaceView OR any future
 * 4.7 dashboard surface.
 */
import { useEffect, useMemo, useState } from "react";
import GroupBySelector from "./GroupBySelector";
import CardListSection from "./CardListSection";
import {
  type GroupAxis,
  type GroupableCard,
  type CustomTagMap,
  groupCards,
} from "../../lib/grouping";
import { readPreference, writePreference } from "../../lib/preferences";
import type { CardListItem } from "../cards/CardList";

export type GroupedDeckListProps = {
  cards: GroupableCard[];
  deckId?: string | null;
  defaultAxis?: GroupAxis;
  customTags?: CustomTagMap;
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
  className?: string;
  toItem?: (card: GroupableCard) => CardListItem;
};

const PREF_NAME = "groupBy";

function _defaultToItem(card: GroupableCard): CardListItem {
  return {
    name: card.name,
    oracleId: card.oracle_id ?? null,
  };
}

export default function GroupedDeckList(props: GroupedDeckListProps) {
  const {
    cards,
    deckId = null,
    defaultAxis = "type",
    customTags,
    onOpenCard,
    className,
    toItem = _defaultToItem,
  } = props;

  const [axis, setAxis] = useState<GroupAxis>(() => {
    const stored = readPreference(PREF_NAME, { deckId, fallback: defaultAxis });
    return (stored as GroupAxis) || defaultAxis;
  });

  // When `deckId` changes (different deck loaded), re-read preferences with
  // the new override scope.
  useEffect(() => {
    const stored = readPreference(PREF_NAME, { deckId, fallback: defaultAxis });
    if (stored && stored !== axis) {
      setAxis(stored as GroupAxis);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deckId]);

  const sections = useMemo(
    () => groupCards(cards, axis, { customTags }),
    [cards, axis, customTags],
  );

  function handleAxisChange(next: GroupAxis) {
    setAxis(next);
    // Per OQ-6 resolution: write per-deck if a deckId is available, else
    // write the global default.
    writePreference(PREF_NAME, next, {
      scope: deckId ? "deck" : "global",
      deckId,
    });
  }

  return (
    <section className={["flex flex-col gap-token-2", className].filter(Boolean).join(" ")}>
      <div className="flex items-center justify-between gap-token-2">
        <GroupBySelector value={axis} onChange={handleAxisChange} label="Group by" />
        <span className="text-xs text-text-muted">
          {sections.length} group{sections.length === 1 ? "" : "s"} · {cards.length} card
          {cards.length === 1 ? "" : "s"}
        </span>
      </div>
      <div className="flex flex-col gap-token-2">
        {sections.map((section) => (
          <CardListSection
            key={section.key}
            label={section.label}
            count={section.cards.length}
            items={section.cards.map(toItem)}
            onOpenCard={onOpenCard}
          />
        ))}
      </div>
    </section>
  );
}
