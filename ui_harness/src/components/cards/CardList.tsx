import type { MouseEvent, ReactNode } from "react";
import { useMemo } from "react";

import CardRow from "./CardRow";

export type CardListItem = {
  name: string;
  oracleId?: string | null;
  rightMeta?: ReactNode;
  className?: string;
  tabIndex?: number;
};

type CardListProps = {
  items: CardListItem[];
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
  className?: string;
  role?: string;
  ariaLabel?: string;
  onRowMouseEnter?: (item: CardListItem, index: number) => void;
  onRowMouseLeave?: (item: CardListItem, index: number) => void;
  onRowMouseDown?: (item: CardListItem, index: number, event: MouseEvent<HTMLLIElement>) => void;
};

function buildContextOracleIds(items: CardListItem[]): string[] {
  const contextOracleIds: string[] = [];
  const seen = new Set<string>();

  for (const item of items) {
    const oracleId = (item.oracleId || "").trim();
    if (oracleId === "" || seen.has(oracleId)) {
      continue;
    }

    seen.add(oracleId);
    contextOracleIds.push(oracleId);
  }

  return contextOracleIds;
}

export default function CardList(props: CardListProps) {
  const {
    items,
    onOpenCard,
    className,
    role,
    ariaLabel,
    onRowMouseEnter,
    onRowMouseLeave,
    onRowMouseDown,
  } = props;

  const normalizedItems = useMemo(() => items, [items]);

  const contextOracleIds = useMemo(() => buildContextOracleIds(normalizedItems), [normalizedItems]);
  const classes = ["workspace-card-list", className].filter(Boolean).join(" ");

  return (
    <ul className={classes} role={role} aria-label={ariaLabel}>
      {normalizedItems.map((item: CardListItem, index: number) => (
        <CardRow
          key={`${item.oracleId || "no-oracle"}-${item.name}-${index}`}
          name={item.name}
          oracleId={item.oracleId}
          rightMeta={item.rightMeta}
          className={item.className}
          tabIndex={item.tabIndex}
          onOpen={
            onOpenCard
              ? (oracleId: string) => {
                  onOpenCard(oracleId, contextOracleIds);
                }
              : undefined
          }
          onMouseEnter={() => {
            onRowMouseEnter?.(item, index);
          }}
          onMouseLeave={() => {
            onRowMouseLeave?.(item, index);
          }}
          onMouseDown={(event) => {
            onRowMouseDown?.(item, index, event);
          }}
        />
      ))}
    </ul>
  );
}
