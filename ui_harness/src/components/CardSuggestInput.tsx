import { useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";

import type { CardSuggestRow } from "./workspaceTypes";
import CardList, { type CardListItem } from "./cards/CardList";
import {
  clampSuggestLimit,
  normalizeApiBase,
  parseCardSuggestRows,
  safeParseJson,
  toSingleLineSnippet,
} from "./workspaceUtils";

type CardSuggestInputProps = {
  label: string;
  value: string;
  placeholder?: string;
  apiBase: string;
  snapshotId: string;
  limit?: number;
  disabled?: boolean;
  onChange: (value: string) => void;
  onSelect?: (row: CardSuggestRow) => void;
  onHoverCard?: (row: CardSuggestRow | null) => void;
};

export default function CardSuggestInput(props: CardSuggestInputProps) {
  const {
    label,
    value,
    placeholder,
    apiBase,
    snapshotId,
    limit = 12,
    disabled = false,
    onChange,
    onSelect,
    onHoverCard,
  } = props;

  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<CardSuggestRow[]>([]);
  const [activeIndex, setActiveIndex] = useState(-1);

  const requestIdRef = useRef(0);

  async function fetchSuggestRows(
    requestUrl: string,
    requestId: number,
    controller: AbortController,
  ): Promise<{ rows: CardSuggestRow[]; status: number; text: string }> {
    if (import.meta.env.DEV) {
      console.log("[CardSuggestInput] request", {
        label,
        requestId,
        requestUrl,
      });
    }

    const response = await fetch(requestUrl, {
      method: "GET",
      signal: controller.signal,
    });
    const text = await response.text();
    const parsed = safeParseJson(text);
    const parsedRows = parseCardSuggestRows(parsed);

    if (import.meta.env.DEV) {
      console.log("[CardSuggestInput] response", {
        label,
        requestId,
        requestUrl,
        status: response.status,
        resultCount: parsedRows.length,
      });
    }

    if (!response.ok) {
      throw new Error(`Failed to fetch /cards/suggest (HTTP ${response.status}) | response=${toSingleLineSnippet(text) || "(empty)"}`);
    }

    return {
      rows: parsedRows,
      status: response.status,
      text,
    };
  }

  function selectRow(row: CardSuggestRow) {
    onChange(row.name);
    onSelect?.(row);
    setOpen(false);
    setActiveIndex(-1);
    onHoverCard?.(null);
  }

  function handleInputKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Escape") {
      setOpen(false);
      setActiveIndex(-1);
      onHoverCard?.(null);
      return;
    }

    if (!open || rows.length === 0) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((prev) => (prev < 0 ? 0 : (prev + 1) % rows.length));
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((prev) => (prev < 0 ? rows.length - 1 : (prev - 1 + rows.length) % rows.length));
      return;
    }

    if (event.key === "Tab" || event.key === "Enter") {
      const selected = rows[activeIndex >= 0 ? activeIndex : 0];
      if (!selected) {
        return;
      }
      event.preventDefault();
      selectRow(selected);
    }
  }

  useEffect(() => {
    if (disabled) {
      setOpen(false);
      setLoading(false);
      setError(null);
      setRows([]);
      setActiveIndex(-1);
      return;
    }

    const query = value.trim();
    if (query.length < 2) {
      setOpen(false);
      setLoading(false);
      setError(null);
      setRows([]);
      setActiveIndex(-1);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    const controller = new AbortController();
    const base = normalizeApiBase(apiBase);
    const safeLimit = clampSuggestLimit(limit);
    const snapshotToken = snapshotId.trim();
    const snapshotPart = snapshotToken !== "" ? `&snapshot_id=${encodeURIComponent(snapshotToken)}` : "";
    const primaryRequestUrl = `${base}/cards/suggest?q=${encodeURIComponent(query)}${snapshotPart}&limit=${safeLimit}`;
    const fallbackRequestUrl = `${base}/cards/suggest?q=${encodeURIComponent(query)}&limit=${safeLimit}`;

    setLoading(true);
    setError(null);

    const timerId = window.setTimeout(async () => {
      try {
        const primary = await fetchSuggestRows(primaryRequestUrl, requestId, controller);

        if (requestId !== requestIdRef.current) {
          return;
        }

        let resolvedRows = primary.rows;

        if (resolvedRows.length === 0 && snapshotToken !== "") {
          const fallback = await fetchSuggestRows(fallbackRequestUrl, requestId, controller);
          if (requestId !== requestIdRef.current) {
            return;
          }

          resolvedRows = fallback.rows;

          if (import.meta.env.DEV) {
            console.log("[CardSuggestInput] fallback-without-snapshot", {
              label,
              requestId,
              primaryRequestUrl,
              fallbackRequestUrl,
              resultCount: resolvedRows.length,
            });
          }
        }

        setRows(resolvedRows);
        setOpen(resolvedRows.length > 0);
        setActiveIndex(resolvedRows.length > 0 ? 0 : -1);
      } catch (requestError) {
        if (controller.signal.aborted || requestId !== requestIdRef.current) {
          return;
        }

        const message = requestError instanceof Error ? requestError.message : "Unknown /cards/suggest error";
        setRows([]);
        setOpen(false);
        setActiveIndex(-1);
        setError(message);
      } finally {
        if (requestId === requestIdRef.current) {
          setLoading(false);
        }
      }
    }, 80);

    return () => {
      controller.abort();
      window.clearTimeout(timerId);
    };
  }, [apiBase, disabled, limit, snapshotId, value]);

  const suggestItems = useMemo(() => {
    return rows.map((row: CardSuggestRow, index: number) => {
      return {
        name: row.name,
        oracleId: row.oracle_id,
        className: index === activeIndex ? "is-active" : "",
        rightMeta: (
          <div className="workspace-suggest-meta-row">
            <span>{row.mana_cost || "-"}</span>
            <span>{row.type_line || "-"}</span>
          </div>
        ),
      } satisfies CardListItem;
    });
  }, [activeIndex, rows]);

  return (
    <label className="workspace-field workspace-suggest-field">
      <span>{label}</span>
      <input
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        onChange={(event) => {
          onChange(event.target.value);
          setOpen(true);
        }}
        onKeyDown={handleInputKeyDown}
        onFocus={() => {
          if (rows.length > 0) {
            setOpen(true);
          }
        }}
      />

      <div className="workspace-suggest-meta">
        <span>{loading ? "searching..." : `${rows.length} result(s)`}</span>
        {error ? <span className="workspace-error-inline">{error}</span> : null}
      </div>

      {open && rows.length > 0 ? (
        <CardList
          items={suggestItems}
          className="workspace-suggest-list"
          role="listbox"
          ariaLabel={`${label} suggestions`}
          onRowMouseEnter={(_, index: number) => {
            const row = rows[index];
            if (!row) {
              return;
            }
            setActiveIndex(index);
            onHoverCard?.(row);
          }}
          onRowMouseLeave={() => {
            onHoverCard?.(null);
          }}
          onRowMouseDown={(_, index: number, event) => {
            const row = rows[index];
            if (!row) {
              return;
            }
            event.preventDefault();
            selectRow(row);
          }}
        />
      ) : null}
    </label>
  );
}
