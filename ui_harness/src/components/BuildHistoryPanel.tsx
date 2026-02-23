import { useMemo } from "react";

import type { BuildHistoryEntry } from "./workspaceTypes";
import { firstNumber, getPath } from "./workspaceUtils";

type BuildHistoryPanelProps = {
  entries: BuildHistoryEntry[];
  selectedEntryId: string | null;
  onSelectEntry: (entryId: string) => void;
};

type CardCountRow = {
  key: string;
  name: string;
  count: number;
};

function toCardCountRows(cards: string[]): CardCountRow[] {
  const counters = new Map<string, CardCountRow>();
  for (const rawName of cards) {
    const name = rawName.trim();
    if (name === "") {
      continue;
    }
    const key = name.toLowerCase();
    const row = counters.get(key);
    if (!row) {
      counters.set(key, {
        key,
        name,
        count: 1,
      });
      continue;
    }
    row.count += 1;
  }

  return Array.from(counters.values()).sort((a, b) => a.name.localeCompare(b.name));
}

function formatDelta(value: number | null): string {
  if (value === null) {
    return "n/a";
  }
  if (value > 0) {
    return `+${value.toFixed(3)}`;
  }
  return value.toFixed(3);
}

function extractMetric(entry: BuildHistoryEntry | null, metric: "coherence" | "vulnerability" | "redundancy"): number | null {
  if (!entry) {
    return null;
  }

  const payload = entry.response_body;
  if (metric === "coherence") {
    return firstNumber(
      getPath(payload, ["result", "engine_coherence_v2", "score"]),
      getPath(payload, ["result", "engine_coherence_v2", "coherence_score"]),
      getPath(payload, ["result", "engine_coherence_v1", "summary", "overlap_score"]),
      getPath(payload, ["result", "engine_coherence_v1", "summary", "coherence_score"]),
      getPath(payload, ["result", "scoring_summary_v1", "coherence"]),
    );
  }

  if (metric === "vulnerability") {
    return firstNumber(
      getPath(payload, ["result", "vulnerability_v2", "score"]),
      getPath(payload, ["result", "vulnerability_v1", "score"]),
      getPath(payload, ["result", "scoring_summary_v1", "vulnerability"]),
      getPath(payload, ["result", "risk_summary_v1", "vulnerability"]),
    );
  }

  return firstNumber(
    getPath(payload, ["result", "redundancy_v2", "score"]),
    getPath(payload, ["result", "redundancy_v1", "score"]),
    getPath(payload, ["result", "scoring_summary_v1", "redundancy"]),
  );
}

export default function BuildHistoryPanel(props: BuildHistoryPanelProps) {
  const { entries, selectedEntryId, onSelectEntry } = props;

  const selectedIndex = entries.findIndex((entry: BuildHistoryEntry) => entry.id === selectedEntryId);
  const effectiveSelectedIndex = selectedIndex >= 0 ? selectedIndex : 0;
  const selectedEntry = entries.length > 0 ? entries[effectiveSelectedIndex] : null;
  const previousEntry = selectedEntry && effectiveSelectedIndex + 1 < entries.length ? entries[effectiveSelectedIndex + 1] : null;

  const diffRows = useMemo(() => {
    if (!selectedEntry || !previousEntry) {
      return {
        added: [] as string[],
        removed: [] as string[],
      };
    }

    const currentRows = toCardCountRows(selectedEntry.request_payload.cards);
    const previousRows = toCardCountRows(previousEntry.request_payload.cards);

    const previousCounts = new Map<string, CardCountRow>();
    previousRows.forEach((row) => previousCounts.set(row.key, row));

    const added: string[] = [];
    const removed: string[] = [];

    for (const row of currentRows) {
      const before = previousCounts.get(row.key);
      const delta = row.count - (before ? before.count : 0);
      if (delta > 0) {
        added.push(`${delta} ${row.name}`);
      }
    }

    const currentCounts = new Map<string, CardCountRow>();
    currentRows.forEach((row) => currentCounts.set(row.key, row));

    for (const row of previousRows) {
      const now = currentCounts.get(row.key);
      const delta = row.count - (now ? now.count : 0);
      if (delta > 0) {
        removed.push(`${delta} ${row.name}`);
      }
    }

    return {
      added,
      removed,
    };
  }, [previousEntry, selectedEntry]);

  const coherenceNow = extractMetric(selectedEntry, "coherence");
  const coherencePrev = extractMetric(previousEntry, "coherence");
  const vulnerabilityNow = extractMetric(selectedEntry, "vulnerability");
  const vulnerabilityPrev = extractMetric(previousEntry, "vulnerability");
  const redundancyNow = extractMetric(selectedEntry, "redundancy");
  const redundancyPrev = extractMetric(previousEntry, "redundancy");

  return (
    <section className="workspace-panel">
      <details open className="workspace-collapsible">
        <summary>Build History</summary>

        {entries.length === 0 ? (
          <p className="workspace-muted">No successful builds yet.</p>
        ) : (
          <ul className="workspace-history-list">
            {entries.map((entry: BuildHistoryEntry) => {
              const isActive = selectedEntry?.id === entry.id;
              return (
                <li key={entry.id}>
                  <button
                    type="button"
                    className={`workspace-history-item ${isActive ? "is-active" : ""}`}
                    onClick={() => {
                      onSelectEntry(entry.id);
                    }}
                  >
                    <div className="workspace-history-title-row">
                      <strong>{entry.deck_name}</strong>
                      <span className={`workspace-status-dot status-${entry.status.toLowerCase()}`}>{entry.status}</span>
                    </div>
                    <div>{entry.timestamp_label}</div>
                    <div>{entry.db_snapshot_id}</div>
                    <div>
                      {entry.profile_id} / {entry.bracket_id}
                    </div>
                  </button>
                </li>
              );
            })}
          </ul>
        )}

        <div className="workspace-history-diff">
          <h4>Diff vs previous</h4>
          {!selectedEntry ? (
            <p className="workspace-muted">Select a history entry to inspect diffs.</p>
          ) : !previousEntry ? (
            <p className="workspace-muted">No previous entry to compare against.</p>
          ) : (
            <>
              <div className="workspace-panel-split">
                <div>
                  <h5>Added cards</h5>
                  {diffRows.added.length === 0 ? (
                    <p className="workspace-muted">No card additions.</p>
                  ) : (
                    <ul className="workspace-compact-list">
                      {diffRows.added.map((row: string) => (
                        <li key={`add-${row}`}>{row}</li>
                      ))}
                    </ul>
                  )}
                </div>

                <div>
                  <h5>Removed cards</h5>
                  {diffRows.removed.length === 0 ? (
                    <p className="workspace-muted">No card removals.</p>
                  ) : (
                    <ul className="workspace-compact-list">
                      {diffRows.removed.map((row: string) => (
                        <li key={`remove-${row}`}>{row}</li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>

              <div className="workspace-metric-grid">
                <div className="workspace-glass-tile">
                  <h5>Coherence</h5>
                  <p>{coherenceNow === null ? "n/a" : coherenceNow.toFixed(3)}</p>
                  <small>Δ {formatDelta(coherenceNow !== null && coherencePrev !== null ? coherenceNow - coherencePrev : null)}</small>
                </div>
                <div className="workspace-glass-tile">
                  <h5>Vulnerability</h5>
                  <p>{vulnerabilityNow === null ? "n/a" : vulnerabilityNow.toFixed(3)}</p>
                  <small>
                    Δ {formatDelta(vulnerabilityNow !== null && vulnerabilityPrev !== null ? vulnerabilityNow - vulnerabilityPrev : null)}
                  </small>
                </div>
                <div className="workspace-glass-tile">
                  <h5>Redundancy</h5>
                  <p>{redundancyNow === null ? "n/a" : redundancyNow.toFixed(3)}</p>
                  <small>Δ {formatDelta(redundancyNow !== null && redundancyPrev !== null ? redundancyNow - redundancyPrev : null)}</small>
                </div>
              </div>
            </>
          )}
        </div>
      </details>
    </section>
  );
}
