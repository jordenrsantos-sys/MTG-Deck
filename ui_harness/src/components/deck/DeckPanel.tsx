import { useMemo, useState } from "react";

import CardList, { type CardListItem } from "../cards/CardList";
import GlassPanel from "../../ui/primitives/GlassPanel";
import IconButton from "../../ui/primitives/IconButton";

export type DeckPanelCard = {
  name: string;
  oracleId?: string | null;
  typeLine?: string | null;
  cmc?: number | null;
};

export type DeckPanelCommander = {
  name: string;
  oracleId?: string | null;
};

type DeckPanelProps = {
  deckCards: DeckPanelCard[];
  commander?: DeckPanelCommander | null;
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
  onHoverCard?: (card: DeckPanelCard | null) => void;
  unknownsCount?: number | null;
  deckSizeTotal?: number | null;
  cardsNeeded?: number | null;
  deckStatus?: string | null;
  buildStatus?: string | null;
  unknownsPanelId?: string;
};

type DeckTypeGroup = "Creature" | "Instant" | "Sorcery" | "Artifact" | "Enchantment" | "Planeswalker" | "Land" | "Other";

type DeckTypeGroupSection = {
  group: DeckTypeGroup;
  cards: DeckPanelCard[];
};

type DeckTypeGroupSectionView = DeckTypeGroupSection & {
  isCollapsed: boolean;
};

type DeckCountStripItem = {
  group: DeckTypeGroup;
  label: string;
};

const GROUP_ORDER: DeckTypeGroup[] = [
  "Creature",
  "Instant",
  "Sorcery",
  "Artifact",
  "Enchantment",
  "Planeswalker",
  "Land",
  "Other",
];

const TYPE_TOKEN_TO_GROUP: Record<string, DeckTypeGroup> = {
  creature: "Creature",
  instant: "Instant",
  sorcery: "Sorcery",
  artifact: "Artifact",
  enchantment: "Enchantment",
  planeswalker: "Planeswalker",
  land: "Land",
};

const CURVE_LABELS = ["0", "1", "2", "3", "4", "5", "6", "7+"];
const TARGET_DECK_SIZE = 100;
const DEFAULT_UNKNOWNS_PANEL_ID = "workspace-unknowns-panel";

const COUNT_STRIP_ITEMS: DeckCountStripItem[] = [
  { group: "Land", label: "Lands" },
  { group: "Creature", label: "Creatures" },
  { group: "Instant", label: "Instants" },
  { group: "Sorcery", label: "Sorceries" },
  { group: "Artifact", label: "Artifacts" },
  { group: "Enchantment", label: "Enchantments" },
  { group: "Planeswalker", label: "Planeswalkers" },
  { group: "Other", label: "Other" },
];

const EMPTY_GROUP_COUNTS: Record<DeckTypeGroup, number> = {
  Creature: 0,
  Instant: 0,
  Sorcery: 0,
  Artifact: 0,
  Enchantment: 0,
  Planeswalker: 0,
  Land: 0,
  Other: 0,
};

const DEFAULT_COLLAPSED_BY_GROUP: Record<DeckTypeGroup, boolean> = {
  Creature: false,
  Instant: false,
  Sorcery: false,
  Artifact: false,
  Enchantment: false,
  Planeswalker: false,
  Land: false,
  Other: false,
};

function _nonempty_str(value: unknown): string {
  if (typeof value !== "string") {
    return "";
  }
  const token = value.trim();
  return token === "" ? "" : token;
}

function _normalize_cmc(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function _deck_group_from_type_line(typeLine: string | null | undefined): DeckTypeGroup {
  const normalizedTypeLine = _nonempty_str(typeLine || "");
  if (normalizedTypeLine === "") {
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
    const group = TYPE_TOKEN_TO_GROUP[token];
    if (group) {
      return group;
    }
  }

  return "Other";
}

function _to_curve_bucket_index(cmc: number): number {
  if (!Number.isFinite(cmc)) {
    return -1;
  }
  const rounded = Math.floor(Math.max(cmc, 0));
  return Math.min(rounded, 7);
}

function _format_cmc_label(cmc: number | null): string | null {
  if (cmc === null) {
    return null;
  }
  if (Math.abs(cmc - Math.round(cmc)) < 0.001) {
    return String(Math.round(cmc));
  }
  return cmc.toFixed(1);
}

function _type_abbrev_for_group(group: DeckTypeGroup): string {
  if (group === "Creature") {
    return "CRE";
  }
  if (group === "Instant") {
    return "INS";
  }
  if (group === "Sorcery") {
    return "SOR";
  }
  if (group === "Artifact") {
    return "ART";
  }
  if (group === "Enchantment") {
    return "ENC";
  }
  if (group === "Planeswalker") {
    return "PW";
  }
  if (group === "Land") {
    return "LAND";
  }
  return "OTH";
}

function _matches_query(card: DeckPanelCard, normalizedQuery: string): boolean {
  if (normalizedQuery === "") {
    return true;
  }

  const cardName = card.name.toLowerCase();
  if (cardName.includes(normalizedQuery)) {
    return true;
  }

  const typeLine = _nonempty_str(card.typeLine || "").toLowerCase();
  if (typeLine !== "" && typeLine.includes(normalizedQuery)) {
    return true;
  }

  return false;
}

function _normalize_int(value: number | null | undefined): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  return Math.trunc(value);
}

function _normalize_status(value: string | null | undefined): string {
  const token = _nonempty_str(value || "");
  return token.toUpperCase();
}

export default function DeckPanel(props: DeckPanelProps) {
  const {
    deckCards,
    commander,
    onOpenCard,
    onHoverCard,
    unknownsCount,
    deckSizeTotal,
    cardsNeeded,
    deckStatus,
    buildStatus,
    unknownsPanelId,
  } = props;

  const [query, setQuery] = useState("");
  const [activeTypeFilter, setActiveTypeFilter] = useState<DeckTypeGroup | null>(null);
  const [collapsedByGroup, setCollapsedByGroup] = useState<Record<DeckTypeGroup, boolean>>(() => ({
    ...DEFAULT_COLLAPSED_BY_GROUP,
  }));

  const normalizedQuery = query.trim().toLowerCase();
  const queryActive = normalizedQuery !== "";

  const resolvedUnknownsPanelId = _nonempty_str(unknownsPanelId || "") || DEFAULT_UNKNOWNS_PANEL_ID;
  const normalizedUnknownsCount = _normalize_int(unknownsCount);
  const hasUnknownsSignal = normalizedUnknownsCount !== null;

  const normalizedDeckSizeTotal = _normalize_int(deckSizeTotal);
  const normalizedCardsNeeded = _normalize_int(cardsNeeded);
  const resolvedDeckSize =
    normalizedDeckSizeTotal !== null
      ? normalizedDeckSizeTotal
      : normalizedCardsNeeded !== null
      ? TARGET_DECK_SIZE - normalizedCardsNeeded
      : null;
  const hasCompletenessSignal = resolvedDeckSize !== null;

  const normalizedDeckStatus = _normalize_status(deckStatus);
  const deckMarkedUnder100 = normalizedDeckStatus === "UNDER_100";
  const deckUnderTarget = resolvedDeckSize !== null && resolvedDeckSize < TARGET_DECK_SIZE;
  const deckAtTarget = resolvedDeckSize !== null && resolvedDeckSize === TARGET_DECK_SIZE;
  const completenessBadgeClassName =
    resolvedDeckSize === null
      ? "deck-signal-badge"
      : deckMarkedUnder100 || deckUnderTarget
      ? "deck-signal-badge deck-signal-badge-warning"
      : deckAtTarget
      ? "deck-signal-badge deck-signal-badge-success"
      : "deck-signal-badge deck-signal-badge-error";
  const completenessTitle =
    deckMarkedUnder100 || deckUnderTarget
      ? "Deck incomplete"
      : deckAtTarget
      ? "Deck complete"
      : undefined;

  const normalizedBuildStatus = _normalize_status(buildStatus);
  const hasBuildStatusSignal = normalizedBuildStatus !== "";
  const buildStatusClassName =
    normalizedBuildStatus === "OK"
      ? "deck-signal-badge deck-signal-badge-success"
      : "deck-signal-badge deck-signal-badge-error";

  const commanderItem = useMemo(() => {
    const commanderName = _nonempty_str(commander?.name || "");
    if (commanderName === "") {
      return [] as CardListItem[];
    }

    return [
      {
        name: commanderName,
        oracleId: _nonempty_str(commander?.oracleId || "") || null,
        className: "deck-commander-row",
      } satisfies CardListItem,
    ];
  }, [commander]);

  const groupedSections = useMemo(() => {
    const rowsByGroup = new Map<DeckTypeGroup, DeckPanelCard[]>();
    for (const group of GROUP_ORDER) {
      rowsByGroup.set(group, []);
    }

    for (const card of deckCards) {
      const group = _deck_group_from_type_line(card.typeLine);
      const bucket = rowsByGroup.get(group);
      if (bucket) {
        bucket.push(card);
      }
    }

    const sections: DeckTypeGroupSection[] = [];
    for (const group of GROUP_ORDER) {
      const cardsForGroup = rowsByGroup.get(group) || [];
      if (cardsForGroup.length === 0) {
        continue;
      }
      sections.push({ group, cards: cardsForGroup });
    }
    return sections;
  }, [deckCards]);

  const fullCountsByGroup = useMemo(() => {
    const counts: Record<DeckTypeGroup, number> = {
      ...EMPTY_GROUP_COUNTS,
    };
    for (const section of groupedSections) {
      counts[section.group] = section.cards.length;
    }
    return counts;
  }, [groupedSections]);

  const filteredSections = useMemo(() => {
    const rows: DeckTypeGroupSection[] = [];
    for (const section of groupedSections) {
      if (activeTypeFilter !== null && section.group !== activeTypeFilter) {
        continue;
      }

      const matchingCards = queryActive
        ? section.cards.filter((card: DeckPanelCard) => _matches_query(card, normalizedQuery))
        : section.cards;
      if (matchingCards.length === 0) {
        continue;
      }
      rows.push({
        group: section.group,
        cards: matchingCards,
      });
    }

    return rows;
  }, [activeTypeFilter, groupedSections, normalizedQuery, queryActive]);

  const sectionsForRender = useMemo(() => {
    return filteredSections.map((section: DeckTypeGroupSection) => ({
      ...section,
      isCollapsed: queryActive ? false : Boolean(collapsedByGroup[section.group]),
    } satisfies DeckTypeGroupSectionView));
  }, [collapsedByGroup, filteredSections, queryActive]);

  const visibleCards = useMemo(() => {
    const rows: DeckPanelCard[] = [];
    for (const section of sectionsForRender) {
      if (section.isCollapsed) {
        continue;
      }
      rows.push(...section.cards);
    }
    return rows;
  }, [sectionsForRender]);

  const visibleContextOracleIds = useMemo(() => {
    const seen = new Set<string>();
    const orderedOracleIds: string[] = [];

    for (const card of visibleCards) {
      const oracleId = _nonempty_str(card.oracleId || "");
      if (oracleId === "" || seen.has(oracleId)) {
        continue;
      }
      seen.add(oracleId);
      orderedOracleIds.push(oracleId);
    }

    return orderedOracleIds;
  }, [visibleCards]);

  const curveSourceCards = useMemo(() => {
    const rows: DeckPanelCard[] = [];
    for (const section of filteredSections) {
      rows.push(...section.cards);
    }
    return rows;
  }, [filteredSections]);

  const curveCounts = useMemo(() => {
    const counts = [0, 0, 0, 0, 0, 0, 0, 0];
    for (const card of curveSourceCards) {
      const cmc = _normalize_cmc(card.cmc);
      if (cmc === null) {
        continue;
      }
      const bucket = _to_curve_bucket_index(cmc);
      if (bucket >= 0) {
        counts[bucket] += 1;
      }
    }
    return counts;
  }, [curveSourceCards]);

  const curveMax = useMemo(() => {
    return curveCounts.reduce((acc: number, value: number) => Math.max(acc, value), 0);
  }, [curveCounts]);

  const sectionItemsByName = useMemo(() => {
    const result = new Map<DeckTypeGroup, CardListItem[]>();

    for (const section of filteredSections) {
      const listItems: CardListItem[] = section.cards.map((card: DeckPanelCard) => {
        const cmc = _normalize_cmc(card.cmc);
        const cmcLabel = _format_cmc_label(cmc);
        const typeLine = _nonempty_str(card.typeLine || "");
        const typeAbbrev = _type_abbrev_for_group(section.group);
        const typeChipTitle = typeLine !== "" ? typeLine : section.group;

        return {
          name: card.name,
          oracleId: _nonempty_str(card.oracleId || "") || null,
          className: "deck-card-row",
          rightMeta: (
            <div className="deck-row-meta">
              {cmcLabel !== null ? <span className="workspace-chip workspace-chip-soft deck-meta-chip">CMC {cmcLabel}</span> : null}
              <span className="workspace-chip workspace-chip-soft deck-meta-chip" title={typeChipTitle}>
                {typeAbbrev}
              </span>
            </div>
          ),
        } satisfies CardListItem;
      });

      result.set(section.group, listItems);
    }

    return result;
  }, [filteredSections]);

  const handleOpenVisibleCard = useMemo(() => {
    if (!onOpenCard) {
      return undefined;
    }

    return (oracleId: string, _contextOracleIds?: string[]) => {
      onOpenCard(oracleId, visibleContextOracleIds);
    };
  }, [onOpenCard, visibleContextOracleIds]);

  function handleToggleGroup(group: DeckTypeGroup): void {
    if (queryActive) {
      return;
    }

    setCollapsedByGroup((previous: Record<DeckTypeGroup, boolean>) => ({
      ...previous,
      [group]: !Boolean(previous[group]),
    }));
  }

  function clearQuery(): void {
    setQuery("");
  }

  function handleToggleTypeFilter(group: DeckTypeGroup): void {
    setActiveTypeFilter((previous: DeckTypeGroup | null) => {
      if (previous === group) {
        return null;
      }
      return group;
    });
  }

  function handleUnknownsBadgeClick(): void {
    if (!hasUnknownsSignal) {
      return;
    }

    const target = document.getElementById(resolvedUnknownsPanelId);
    if (target) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  return (
    <section
      className="deck-panel-root workspace-panel-content"
      onMouseLeave={() => {
        onHoverCard?.(null);
      }}
    >
      <div className="deck-panel-sticky">
        <div className="deck-panel-subheader">
          <div className="deck-panel-summary">
            <h3 className="deck-section-title">Deck</h3>
            <span className="deck-section-count">{deckCards.length}</span>

            <div className="deck-signal-strip" role="group" aria-label="Deck engine signals">
              {hasUnknownsSignal ? (
                <button
                  type="button"
                  className={`deck-signal-badge${normalizedUnknownsCount !== null && normalizedUnknownsCount > 0 ? " deck-signal-badge-warning" : ""}`}
                  title={
                    normalizedUnknownsCount !== null && normalizedUnknownsCount > 0
                      ? "Unknowns detected. Scroll to Unknowns panel."
                      : "No unknowns. Scroll to Unknowns panel."
                  }
                  onClick={handleUnknownsBadgeClick}
                >
                  Unknowns {normalizedUnknownsCount ?? 0}
                </button>
              ) : null}

              {hasCompletenessSignal ? (
                <span className={completenessBadgeClassName} title={completenessTitle}>
                  {resolvedDeckSize ?? 0} / {TARGET_DECK_SIZE}
                </span>
              ) : null}

              {hasBuildStatusSignal ? <span className={buildStatusClassName}>{normalizedBuildStatus}</span> : null}
            </div>
          </div>
        </div>

        <label className="deck-panel-search" aria-label="Search deck cards">
          <input
            className="deck-panel-search-input"
            type="search"
            value={query}
            placeholder="Search deck..."
            onChange={(event) => {
              setQuery(event.target.value);
            }}
          />

          {queryActive ? (
            <IconButton className="deck-panel-search-clear" aria-label="Clear deck search" onClick={clearQuery}>
              ×
            </IconButton>
          ) : null}
        </label>

        <div className="deck-section-header">
          <h4 className="deck-section-title">Mana Curve</h4>
          <span className="deck-section-count">{curveCounts.reduce((acc: number, value: number) => acc + value, 0)}</span>
        </div>
        <div className="deck-curve-row" role="img" aria-label="Mana curve buckets from 0 through 7 plus">
          {curveCounts.map((count: number, index: number) => {
            const heightPct = curveMax > 0 ? Math.max(10, Math.round((count / curveMax) * 100)) : 10;
            const label = CURVE_LABELS[index] || String(index);
            return (
              <div className="deck-curve-cell" key={`curve-${label}`}>
                <div className="deck-curve-track">
                  <div className="deck-curve-bar" style={{ height: `${heightPct}%` }} title={`CMC ${label}: ${count}`} />
                </div>
                <span className="deck-curve-label">{label}</span>
              </div>
            );
          })}
        </div>

        <div className="deck-counts-strip" role="toolbar" aria-label="Quick type filters">
          {COUNT_STRIP_ITEMS.map((item: DeckCountStripItem) => {
            const isActive = activeTypeFilter === item.group;
            const count = fullCountsByGroup[item.group] || 0;
            return (
              <button
                key={`count-${item.group}`}
                type="button"
                title={`Filter to ${item.label}`}
                className={`deck-count-chip${isActive ? " deck-count-chip-active" : ""}`}
                aria-pressed={isActive}
                onClick={() => {
                  handleToggleTypeFilter(item.group);
                }}
              >
                <span>{item.label}</span>
                <span>{count}</span>
              </button>
            );
          })}
        </div>
      </div>

      <div className="deck-panel-scroll">
        {commanderItem.length > 0 ? (
          <GlassPanel className="deck-section deck-commander-block">
            <div className="deck-section-header">
              <h4 className="deck-section-title">Commander</h4>
              <span className="deck-section-count">1</span>
            </div>
            <CardList
              items={commanderItem}
              onOpenCard={onOpenCard}
              className="deck-card-list"
              ariaLabel="Commander"
              onRowMouseEnter={() => {
                if (!commander) {
                  return;
                }
                onHoverCard?.({
                  name: commander.name,
                  oracleId: commander.oracleId,
                  typeLine: null,
                });
              }}
              onRowMouseLeave={() => {
                onHoverCard?.(null);
              }}
            />
          </GlassPanel>
        ) : null}

        {deckCards.length === 0 ? (
          <p className="workspace-muted">No deck cards available in the current build result.</p>
        ) : sectionsForRender.length === 0 ? (
          <p className="workspace-muted">No cards match your current deck search.</p>
        ) : (
          sectionsForRender.map((section: DeckTypeGroupSectionView) => {
            const items = sectionItemsByName.get(section.group) || [];
            const sectionCollapsed = section.isCollapsed;
            const sectionId = `deck-section-${section.group.toLowerCase()}`;

            return (
              <section className="deck-section" key={`group-${section.group}`}>
                <div className="deck-section-header">
                  <div className="deck-section-heading">
                    <button
                      type="button"
                      className="deck-section-toggle"
                      aria-expanded={!sectionCollapsed}
                      aria-controls={sectionId}
                      disabled={queryActive}
                      onClick={() => {
                        handleToggleGroup(section.group);
                      }}
                    >
                      {sectionCollapsed ? "▸" : "▾"}
                    </button>
                    <h4 className="deck-section-title">{section.group}</h4>
                  </div>
                  <span className="deck-section-count">{section.cards.length}</span>
                </div>

                {!sectionCollapsed ? (
                  <>
                    <div className="deck-section-divider" aria-hidden="true" />
                    <div id={sectionId}>
                      <CardList
                        items={items}
                        onOpenCard={handleOpenVisibleCard}
                        className="deck-card-list"
                        ariaLabel={`${section.group} cards`}
                        onRowMouseEnter={(_, index: number) => {
                          const card = section.cards[index];
                          if (!card) {
                            return;
                          }
                          onHoverCard?.(card);
                        }}
                        onRowMouseLeave={() => {
                          onHoverCard?.(null);
                        }}
                      />
                    </div>
                  </>
                ) : null}
              </section>
            );
          })
        )}
      </div>
    </section>
  );
}
