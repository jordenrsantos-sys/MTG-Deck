import { Fragment, useMemo, useState } from "react";

import type { BuildResponsePayload } from "./workspaceTypes";
import {
  asArray,
  asNumber,
  asRecord,
  firstNonEmptyString,
  uniqueSortedStrings,
} from "./workspaceUtils";

type CanonicalSlotRow = {
  slotId: string;
  status: string;
  required: number;
  present: number;
  under: number;
  over: number;
  cardsInvolved: string[];
};

type CanonicalSlotsPanelProps = {
  buildResponse: BuildResponsePayload | null;
};

export default function CanonicalSlotsPanel(props: CanonicalSlotsPanelProps) {
  const { buildResponse } = props;
  const result = asRecord(buildResponse?.result);

  const rows = useMemo(() => {
    const rawSlots = asArray(result?.canonical_slots_all)
      .map((raw: unknown) => asRecord(raw))
      .filter((row): row is Record<string, unknown> => row !== null);

    if (rawSlots.length === 0) {
      return [] as CanonicalSlotRow[];
    }

    const cardsBySlot = new Map<string, string[]>();

    const collect = (raw: unknown) => {
      const row = asRecord(raw);
      if (!row) {
        return;
      }
      const slotId = firstNonEmptyString(row.slot_id, row.id);
      if (!slotId) {
        return;
      }

      const cardName = firstNonEmptyString(row.resolved_name, row.name, row.input, row.card_name);
      if (!cardName) {
        return;
      }

      const existing = cardsBySlot.get(slotId) || [];
      existing.push(cardName);
      cardsBySlot.set(slotId, existing);
    };

    for (const raw of asArray(result?.deck_cards_playable)) {
      collect(raw);
    }
    for (const raw of asArray(result?.deck_cards_nonplayable)) {
      collect(raw);
    }
    for (const raw of asArray(result?.canonical_slots_all)) {
      collect(raw);
    }

    return rawSlots
      .map((row): CanonicalSlotRow => {
        const slotId = firstNonEmptyString(row.slot_id, row.id) || "<slot_id>";
        const required = asNumber(row.required_count) ?? asNumber(row.required) ?? 1;

        let present = asNumber(row.present_count) ?? asNumber(row.present);
        if (present === null) {
          const resolvedName = firstNonEmptyString(row.resolved_name, row.name);
          const resolvedOracle = firstNonEmptyString(row.resolved_oracle_id, row.oracle_id);
          present = resolvedName || resolvedOracle ? 1 : 0;
        }

        const safeRequired = Math.max(0, Math.trunc(required));
        const safePresent = Math.max(0, Math.trunc(present));

        return {
          slotId,
          status: firstNonEmptyString(row.status, row.state) || "-",
          required: safeRequired,
          present: safePresent,
          under: Math.max(0, safeRequired - safePresent),
          over: Math.max(0, safePresent - safeRequired),
          cardsInvolved: uniqueSortedStrings(cardsBySlot.get(slotId) || []),
        };
      })
      .sort((a: CanonicalSlotRow, b: CanonicalSlotRow) => a.slotId.localeCompare(b.slotId));
  }, [result]);

  const [expandedSlotIds, setExpandedSlotIds] = useState<Set<string>>(new Set<string>());

  function toggle(slotId: string) {
    setExpandedSlotIds((previous: Set<string>) => {
      const next = new Set(previous);
      if (next.has(slotId)) {
        next.delete(slotId);
      } else {
        next.add(slotId);
      }
      return next;
    });
  }

  return (
    <section className="workspace-panel-content">
      <details open className="workspace-collapsible">
        <summary>Canonical Slots Drilldown</summary>

        {rows.length === 0 ? (
          <p className="workspace-muted">canonical_slots_all not present in this build output.</p>
        ) : (
          <div className="workspace-slot-table-wrap">
            <table className="workspace-slot-table">
              <thead>
                <tr>
                  <th>Slot</th>
                  <th>Status</th>
                  <th>Required</th>
                  <th>Present</th>
                  <th>Under</th>
                  <th>Over</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row: CanonicalSlotRow) => {
                  const isExpanded = expandedSlotIds.has(row.slotId);
                  return (
                    <Fragment key={row.slotId}>
                      <tr key={row.slotId}>
                        <td>{row.slotId}</td>
                        <td>{row.status}</td>
                        <td>{row.required}</td>
                        <td>{row.present}</td>
                        <td>{row.under}</td>
                        <td>{row.over}</td>
                        <td>
                          <button
                            type="button"
                            className="workspace-link-button"
                            onClick={() => {
                              toggle(row.slotId);
                            }}
                          >
                            {isExpanded ? "Hide" : "Show"}
                          </button>
                        </td>
                      </tr>
                      {isExpanded ? (
                        <tr>
                          <td colSpan={7}>
                            <div className="workspace-slot-details">
                              <strong>Cards involved:</strong>
                              {row.cardsInvolved.length === 0 ? (
                                <span className="workspace-muted"> none reported in this payload.</span>
                              ) : (
                                <ul className="workspace-compact-list">
                                  {row.cardsInvolved.map((cardName: string) => (
                                    <li key={`${row.slotId}-${cardName}`}>{cardName}</li>
                                  ))}
                                </ul>
                              )}
                            </div>
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </details>
    </section>
  );
}
