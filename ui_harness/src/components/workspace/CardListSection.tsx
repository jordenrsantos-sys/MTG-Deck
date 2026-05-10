/**
 * CardListSection — Phase 4.6.
 *
 * Wraps the existing flat `<CardList>` (autonomous_repair_log soft-safety #1:
 * preserve flat API) inside a sectioned/expandable header. The wrapper does
 * NOT modify CardList's props or behavior; it just adds a `<details>`
 * surface around it with section label + count badge.
 */
import type { ReactNode } from "react";
import CardList, { type CardListItem } from "../cards/CardList";
import Badge from "../../ui/primitives/Badge";

export type CardListSectionProps = {
  label: string;
  count?: number;
  items: CardListItem[];
  onOpenCard?: (oracleId: string, contextOracleIds?: string[]) => void;
  defaultOpen?: boolean;
  rightSlot?: ReactNode;
  className?: string;
};

export default function CardListSection(props: CardListSectionProps) {
  const {
    label,
    count,
    items,
    onOpenCard,
    defaultOpen = true,
    rightSlot,
    className,
  } = props;
  const resolvedCount = typeof count === "number" ? count : items.length;
  const classes = ["workspace-card-list-section", className].filter(Boolean).join(" ");
  return (
    <details open={defaultOpen} className={classes}>
      <summary className="flex items-center gap-token-2 cursor-pointer select-none py-token-1">
        <span className="text-text-primary font-medium">{label}</span>
        <Badge variant="neutral">{resolvedCount}</Badge>
        {rightSlot ? <span className="ml-auto">{rightSlot}</span> : null}
      </summary>
      <CardList items={items} onOpenCard={onOpenCard} />
    </details>
  );
}
