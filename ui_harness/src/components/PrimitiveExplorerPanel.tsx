import { useEffect, useMemo, useState } from "react";

import type {
  BuildResponsePayload,
  HoverCard,
  PrimitiveExplorerCardRow,
  PrimitiveExplorerGroup,
} from "./workspaceTypes";
import {
  asArray,
  asRecord,
  asStringArray,
  cardNameSortKey,
  extractPrimitiveIds,
  firstNonEmptyString,
  normalizeSlotIds,
  uniqueSortedStrings,
} from "./workspaceUtils";

type PrimitiveExplorerPanelProps = {
  buildResponse: BuildResponsePayload | null;
  onHoverCard: (card: HoverCard | null) => void;
};

export default function PrimitiveExplorerPanel(props: PrimitiveExplorerPanelProps) {
  const { buildResponse, onHoverCard } = props;

  const [selectedPrimitiveId, setSelectedPrimitiveId] = useState<string | null>(null);

  const result = asRecord(buildResponse?.result);
  const primitiveIndexBySlot = asRecord(result?.primitive_index_by_slot);
  const slotIdsByPrimitive = asRecord(result?.slot_ids_by_primitive);
  const primitiveIndexTotals = asRecord(result?.primitive_index_totals);

  const hasPrimitiveIndexBySlotField =
    result !== null && Object.prototype.hasOwnProperty.call(result, "primitive_index_by_slot");
  const hasSlotIdsByPrimitiveField = result !== null && Object.prototype.hasOwnProperty.call(result, "slot_ids_by_primitive");

  const slotPrimitiveTagsById = useMemo(() => {
    const rowsBySlotId = new Map<string, string[]>();
    if (!primitiveIndexBySlot) {
      return rowsBySlotId;
    }

    for (const slotId of Object.keys(primitiveIndexBySlot).sort((a, b) => a.localeCompare(b))) {
      const primitiveTags = extractPrimitiveIds(primitiveIndexBySlot[slotId]);
      if (primitiveTags.length > 0) {
        rowsBySlotId.set(slotId, primitiveTags);
      }
    }

    return rowsBySlotId;
  }, [primitiveIndexBySlot]);

  const slotCardRowsById = useMemo(() => {
    const rowsBySlotId = new Map<string, PrimitiveExplorerCardRow>();

    function upsertFromRaw(raw: unknown): void {
      const row = asRecord(raw);
      if (!row) {
        return;
      }

      const slotId = firstNonEmptyString(row.slot_id, row.id);
      if (slotId === null) {
        return;
      }

      const nextName =
        firstNonEmptyString(row.resolved_name, row.name, row.input, row.card_name, row.slot_name, slotId) || slotId;
      const nextOracleId = firstNonEmptyString(row.resolved_oracle_id, row.oracle_id) || "";
      const nextTypeLine = firstNonEmptyString(row.type_line, row.resolved_type_line, row.card_type_line);
      const nextPrimitiveTags = uniqueSortedStrings([
        ...extractPrimitiveIds(row.primitives),
        ...extractPrimitiveIds(row.primitive_ids),
        ...extractPrimitiveIds(row.primitive_tags),
        ...asStringArray(row.tags),
      ]);

      const previous = rowsBySlotId.get(slotId);
      if (!previous) {
        rowsBySlotId.set(slotId, {
          slot_id: slotId,
          name: nextName,
          oracle_id: nextOracleId,
          type_line: nextTypeLine,
          primitive_tags: nextPrimitiveTags,
        });
        return;
      }

      rowsBySlotId.set(slotId, {
        slot_id: slotId,
        name: cardNameSortKey(previous.name) !== cardNameSortKey(slotId) ? previous.name : nextName,
        oracle_id: previous.oracle_id || nextOracleId,
        type_line: previous.type_line || nextTypeLine,
        primitive_tags: uniqueSortedStrings([...previous.primitive_tags, ...nextPrimitiveTags]),
      });
    }

    upsertFromRaw(result?.commander_canonical_slot);
    for (const row of asArray(result?.canonical_slots_all)) {
      upsertFromRaw(row);
    }
    for (const row of asArray(result?.graph_nodes)) {
      upsertFromRaw(row);
    }
    for (const row of asArray(result?.deck_cards_playable)) {
      upsertFromRaw(row);
    }
    for (const row of asArray(result?.deck_cards_nonplayable)) {
      upsertFromRaw(row);
    }

    return rowsBySlotId;
  }, [result]);

  const primitiveExplorer = useMemo(() => {
    const primitiveToSlotIds = new Map<string, Set<string>>();
    let source: "primitive_index_by_slot" | "slot_ids_by_primitive" | "missing" = "missing";

    function addPrimitiveSlotPair(primitiveIdRaw: string, slotIdRaw: string): void {
      const primitiveId = primitiveIdRaw.trim();
      const slotId = slotIdRaw.trim();
      if (primitiveId === "" || slotId === "") {
        return;
      }
      const slotIds = primitiveToSlotIds.get(primitiveId) || new Set<string>();
      slotIds.add(slotId);
      primitiveToSlotIds.set(primitiveId, slotIds);
    }

    if (hasPrimitiveIndexBySlotField) {
      source = "primitive_index_by_slot";

      if (primitiveIndexBySlot) {
        for (const slotId of Object.keys(primitiveIndexBySlot).sort((a, b) => a.localeCompare(b))) {
          const primitiveIds = extractPrimitiveIds(primitiveIndexBySlot[slotId]);
          for (const primitiveId of primitiveIds) {
            addPrimitiveSlotPair(primitiveId, slotId);
          }
        }
      }

      if (primitiveToSlotIds.size === 0 && slotIdsByPrimitive) {
        for (const primitiveId of Object.keys(slotIdsByPrimitive).sort((a, b) => a.localeCompare(b))) {
          const slotIds = normalizeSlotIds(slotIdsByPrimitive[primitiveId]);
          for (const slotId of slotIds) {
            addPrimitiveSlotPair(primitiveId, slotId);
          }
        }
      }
    } else if (hasSlotIdsByPrimitiveField) {
      source = "slot_ids_by_primitive";
      if (slotIdsByPrimitive) {
        for (const primitiveId of Object.keys(slotIdsByPrimitive).sort((a, b) => a.localeCompare(b))) {
          const slotIds = normalizeSlotIds(slotIdsByPrimitive[primitiveId]);
          for (const slotId of slotIds) {
            addPrimitiveSlotPair(primitiveId, slotId);
          }
        }
      }
    }

    const groups: PrimitiveExplorerGroup[] = Array.from(primitiveToSlotIds.entries()).map(([primitiveId, slotIdSet]) => {
      const slotIds = Array.from(slotIdSet).sort((a, b) => a.localeCompare(b));
      const cards: PrimitiveExplorerCardRow[] = slotIds
        .map((slotId: string) => {
          const cardRow = slotCardRowsById.get(slotId);
          const mergedTags = uniqueSortedStrings([
            ...(slotPrimitiveTagsById.get(slotId) || []),
            ...(cardRow ? cardRow.primitive_tags : []),
          ]);

          return {
            slot_id: slotId,
            name: cardRow?.name || slotId,
            oracle_id: cardRow?.oracle_id || "",
            type_line: cardRow?.type_line || null,
            primitive_tags: mergedTags,
          };
        })
        .sort((a, b) => {
          const byName = cardNameSortKey(a.name).localeCompare(cardNameSortKey(b.name));
          if (byName !== 0) {
            return byName;
          }
          return a.slot_id.localeCompare(b.slot_id);
        });

      return {
        primitive_id: primitiveId,
        count: slotIds.length,
        slot_ids: slotIds,
        cards,
      };
    });

    groups.sort((a, b) => {
      if (a.count !== b.count) {
        return b.count - a.count;
      }
      return a.primitive_id.localeCompare(b.primitive_id);
    });

    return {
      source,
      groups,
    };
  }, [
    hasPrimitiveIndexBySlotField,
    hasSlotIdsByPrimitiveField,
    primitiveIndexBySlot,
    slotCardRowsById,
    slotIdsByPrimitive,
    slotPrimitiveTagsById,
  ]);

  useEffect(() => {
    if (primitiveExplorer.groups.length === 0) {
      if (selectedPrimitiveId !== null) {
        setSelectedPrimitiveId(null);
      }
      return;
    }

    const hasSelected =
      selectedPrimitiveId !== null &&
      primitiveExplorer.groups.some((row: PrimitiveExplorerGroup) => row.primitive_id === selectedPrimitiveId);

    if (!hasSelected) {
      setSelectedPrimitiveId(primitiveExplorer.groups[0].primitive_id);
    }
  }, [primitiveExplorer.groups, selectedPrimitiveId]);

  const selectedPrimitiveGroup = useMemo(() => {
    if (primitiveExplorer.groups.length === 0) {
      return null;
    }
    if (selectedPrimitiveId === null) {
      return primitiveExplorer.groups[0];
    }
    return primitiveExplorer.groups.find((row: PrimitiveExplorerGroup) => row.primitive_id === selectedPrimitiveId) || primitiveExplorer.groups[0];
  }, [primitiveExplorer.groups, selectedPrimitiveId]);

  const sourceLabel =
    primitiveExplorer.source === "primitive_index_by_slot"
      ? "result.primitive_index_by_slot"
      : primitiveExplorer.source === "slot_ids_by_primitive"
      ? "result.slot_ids_by_primitive"
      : "not present";

  return (
    <section className="workspace-panel" onMouseLeave={() => onHoverCard(null)}>
      <details open className="workspace-collapsible">
        <summary>Primitive Explorer</summary>

        <div className="workspace-chip-row">
          <span className="workspace-chip">source: {sourceLabel}</span>
          <span className="workspace-chip">
            total slots: {primitiveIndexTotals && typeof primitiveIndexTotals.total_slots === "number" ? primitiveIndexTotals.total_slots : "-"}
          </span>
          <span className="workspace-chip">groups: {primitiveExplorer.groups.length}</span>
        </div>

        {primitiveExplorer.source === "missing" ? (
          <p className="workspace-muted">Primitive explorer inputs are not present in this build output.</p>
        ) : (
          <div className="workspace-panel-split workspace-primitive-grid">
            <div>
              <h4>Primitives</h4>
              <ul className="workspace-primitive-list">
                {primitiveExplorer.groups.map((group: PrimitiveExplorerGroup) => (
                  <li key={group.primitive_id}>
                    <button
                      type="button"
                      className={selectedPrimitiveGroup?.primitive_id === group.primitive_id ? "is-active" : ""}
                      onClick={() => {
                        setSelectedPrimitiveId(group.primitive_id);
                      }}
                    >
                      <span>{group.primitive_id}</span>
                      <span>{group.count}</span>
                    </button>
                  </li>
                ))}
              </ul>
            </div>

            <div>
              <h4>Cards in primitive</h4>
              {!selectedPrimitiveGroup ? (
                <p className="workspace-muted">Select a primitive.</p>
              ) : (
                <ul className="workspace-primitive-cards">
                  {selectedPrimitiveGroup.cards.map((card: PrimitiveExplorerCardRow) => (
                    <li
                      key={`${selectedPrimitiveGroup.primitive_id}-${card.slot_id}`}
                      onMouseEnter={() => {
                        onHoverCard({
                          name: card.name,
                          oracle_id: card.oracle_id,
                          type_line: card.type_line,
                          primitive_tags: card.primitive_tags,
                          source: "primitive",
                        });
                      }}
                    >
                      <div className="workspace-history-title-row">
                        <strong>{card.name}</strong>
                        <span className="workspace-muted">{card.slot_id}</span>
                      </div>
                      <div className="workspace-muted">{card.type_line || "type unavailable"}</div>
                      {card.primitive_tags.length > 0 ? (
                        <div className="workspace-inline-tags">
                          {card.primitive_tags.map((tag: string) => (
                            <span key={`${card.slot_id}-${tag}`} className="workspace-chip workspace-chip-soft">
                              {tag}
                            </span>
                          ))}
                        </div>
                      ) : null}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        )}
      </details>
    </section>
  );
}
