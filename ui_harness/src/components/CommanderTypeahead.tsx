/**
 * CommanderTypeahead — Mega-task v7 Phase 2.
 *
 * Debounced typeahead for the AIBuildView commander field. Replaces the
 * plain <Input> that shipped in iter-7, which let typos like "Edgar Makrov"
 * cascade into total deck failure (BRIEF_NO_CORPUS_ENTRIES_FOR_COMMANDER →
 * empty color identity → no candidates → 99 Wastes).
 *
 * Behavior:
 *   - 250ms debounce on keystroke → GET /cards/suggest?commander_only=true
 *   - Renders top 5-10 results in a listbox dropdown with name + type_line +
 *     mana cost.
 *   - Arrow Up/Down navigates, Enter selects, Esc dismisses, click selects.
 *   - When the exact query has no prefix/substring match, fetches with
 *     fuzzy=true and surfaces "Did you mean: <closest>?" with a click-to-
 *     replace affordance. Cutoff is the backend's difflib ratio 0.6 — good
 *     enough for 1-2 character edit-distance typos without spurious matches.
 *
 * The component is intentionally self-contained: it does NOT pull in the
 * workspace-specific CardList primitive (different design system). The
 * dropdown is a plain <ul> with utility classes.
 */
import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent } from "react";

const DEBOUNCE_MS = 250;
const MAX_RESULTS = 10;

type SuggestRow = {
  name: string;
  oracle_id?: string | null;
  type_line?: string | null;
  mana_cost?: string | null;
  image_uri?: string | null;
  fuzzy_match?: boolean | null;
};

type SuggestResponse = {
  query: string;
  snapshot_id: string;
  limit: number;
  results: SuggestRow[];
  fuzzy_active?: boolean;
};

export type CommanderTypeaheadProps = {
  value: string;
  onChange: (next: string) => void;
  apiBase: string;
  snapshotId: string;
  disabled?: boolean;
  placeholder?: string;
  ariaLabel?: string;
};

export default function CommanderTypeahead(props: CommanderTypeaheadProps) {
  const {
    value,
    onChange,
    apiBase,
    snapshotId,
    disabled = false,
    placeholder = "e.g., Edgar Markov",
    ariaLabel = "Commander name",
  } = props;

  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [rows, setRows] = useState<SuggestRow[]>([]);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [fuzzySuggestion, setFuzzySuggestion] = useState<SuggestRow | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const requestIdRef = useRef(0);
  const lastSelectedRef = useRef<string>("");

  function resetDropdown() {
    setOpen(false);
    setRows([]);
    setActiveIndex(-1);
    setFuzzySuggestion(null);
  }

  function selectRow(row: SuggestRow) {
    lastSelectedRef.current = row.name;
    onChange(row.name);
    resetDropdown();
    // Keep focus on input so the user can tab away or edit.
    window.requestAnimationFrame(() => {
      if (inputRef.current && !disabled) inputRef.current.focus();
    });
  }

  function handleKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Escape") {
      resetDropdown();
      return;
    }
    if (!open || rows.length === 0) {
      if (event.key === "Enter" && fuzzySuggestion) {
        event.preventDefault();
        selectRow(fuzzySuggestion);
      }
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      const next = activeIndex < 0 ? 0 : (activeIndex + 1) % rows.length;
      setActiveIndex(next);
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      const next = activeIndex < 0 ? rows.length - 1 : (activeIndex - 1 + rows.length) % rows.length;
      setActiveIndex(next);
      return;
    }
    if (event.key === "Enter" || event.key === "Tab") {
      const idx = activeIndex >= 0 ? activeIndex : 0;
      const sel = rows[idx];
      if (sel) {
        event.preventDefault();
        selectRow(sel);
      }
    }
  }

  useEffect(() => {
    const query = value.trim();
    // Skip refetch when the value matches a row we just selected — avoids
    // a re-fetch flicker on selection.
    if (lastSelectedRef.current && query === lastSelectedRef.current) {
      return;
    }
    if (disabled || query.length < 2) {
      resetDropdown();
      setLoading(false);
      setErrorMessage(null);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    const controller = new AbortController();
    const base = (apiBase || "").replace(/\/$/, "");
    const snap = snapshotId.trim();
    const snapPart = snap ? `&snapshot_id=${encodeURIComponent(snap)}` : "";

    setLoading(true);
    setErrorMessage(null);

    const timer = window.setTimeout(async () => {
      const exactUrl = `${base}/cards/suggest?q=${encodeURIComponent(query)}${snapPart}&commander_only=true&limit=${MAX_RESULTS}`;
      const fuzzyUrl = `${base}/cards/suggest?q=${encodeURIComponent(query)}${snapPart}&commander_only=true&fuzzy=true&limit=${MAX_RESULTS}`;
      try {
        const exactResp = await fetch(exactUrl, { method: "GET", signal: controller.signal });
        if (requestId !== requestIdRef.current) return;
        if (!exactResp.ok) {
          throw new Error(`/cards/suggest returned HTTP ${exactResp.status}`);
        }
        const exactJson = (await exactResp.json()) as SuggestResponse;
        const exactRows = (exactJson.results || []).slice(0, MAX_RESULTS);
        if (exactRows.length > 0) {
          setRows(exactRows);
          setOpen(true);
          setActiveIndex(0);
          setFuzzySuggestion(null);
          return;
        }
        // No exact matches — try fuzzy.
        const fuzzyResp = await fetch(fuzzyUrl, { method: "GET", signal: controller.signal });
        if (requestId !== requestIdRef.current) return;
        if (!fuzzyResp.ok) {
          setRows([]);
          setOpen(false);
          setActiveIndex(-1);
          setFuzzySuggestion(null);
          return;
        }
        const fuzzyJson = (await fuzzyResp.json()) as SuggestResponse;
        const fuzzyRows = (fuzzyJson.results || []).filter((r) => r.fuzzy_match);
        if (fuzzyRows.length > 0) {
          // Surface the top fuzzy match as "Did you mean: ..." rather than
          // opening the full dropdown — the user typed a name expecting a
          // commander; fuzzy means we don't have direct evidence they want
          // any of these. Confirmation via click is the gentler UX.
          setFuzzySuggestion(fuzzyRows[0]);
          setRows([]);
          setOpen(false);
          setActiveIndex(-1);
        } else {
          setRows([]);
          setOpen(false);
          setActiveIndex(-1);
          setFuzzySuggestion(null);
        }
      } catch (err) {
        if (controller.signal.aborted || requestId !== requestIdRef.current) return;
        const msg = err instanceof Error ? err.message : "Unknown error";
        setErrorMessage(msg);
        setRows([]);
        setOpen(false);
        setActiveIndex(-1);
        setFuzzySuggestion(null);
      } finally {
        if (requestId === requestIdRef.current) setLoading(false);
      }
    }, DEBOUNCE_MS);

    return () => {
      controller.abort();
      window.clearTimeout(timer);
    };
  }, [apiBase, disabled, snapshotId, value]);

  return (
    <div className="commander-typeahead" style={{ position: "relative" }}>
      <input
        ref={inputRef}
        type="text"
        value={value}
        disabled={disabled}
        placeholder={placeholder}
        aria-label={ariaLabel}
        aria-autocomplete="list"
        aria-expanded={open}
        autoComplete="off"
        spellCheck={false}
        onChange={(e) => {
          // User typed → clear the "last selected" sentinel so we resume
          // searching on the new query.
          if (e.target.value !== lastSelectedRef.current) {
            lastSelectedRef.current = "";
          }
          onChange(e.target.value);
        }}
        onKeyDown={handleKeyDown}
        onBlur={() => {
          // Defer close so click-to-select on the dropdown can fire first.
          window.setTimeout(() => {
            // Only close if focus left the typeahead entirely.
            const active = document.activeElement;
            if (active !== inputRef.current && !(active && active.closest(".commander-typeahead"))) {
              setOpen(false);
            }
          }, 100);
        }}
        className="w-full px-token-2 py-token-1 border border-border rounded-token-1 bg-surface text-text-base"
      />
      {loading ? (
        <span
          className="text-xs text-text-muted"
          style={{ position: "absolute", right: "8px", top: "8px" }}
        >
          searching…
        </span>
      ) : null}
      {errorMessage ? (
        <div className="text-xs text-error mt-token-1" role="alert">
          {errorMessage}
        </div>
      ) : null}
      {fuzzySuggestion && !open ? (
        <div className="text-xs mt-token-1" data-testid="commander-typeahead-fuzzy">
          Did you mean:{" "}
          <button
            type="button"
            className="text-link underline hover:text-link-hover"
            onMouseDown={(e) => {
              e.preventDefault();
              selectRow(fuzzySuggestion);
            }}
          >
            {fuzzySuggestion.name}
          </button>
          ?
        </div>
      ) : null}
      {open && rows.length > 0 ? (
        <ul
          role="listbox"
          aria-label="Commander suggestions"
          data-testid="commander-typeahead-dropdown"
          style={{
            position: "absolute", top: "100%", left: 0, right: 0,
            zIndex: 50,
            background: "var(--color-surface, #fff)",
            border: "1px solid var(--color-border, #ccc)",
            maxHeight: "320px", overflowY: "auto",
            margin: 0, padding: 0, listStyle: "none",
            boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
          }}
        >
          {rows.map((row, idx) => (
            <li
              key={row.oracle_id || row.name}
              role="option"
              aria-selected={idx === activeIndex}
              data-testid={`commander-typeahead-option-${idx}`}
              onMouseDown={(e) => {
                e.preventDefault();
                selectRow(row);
              }}
              onMouseEnter={() => setActiveIndex(idx)}
              style={{
                padding: "8px 12px",
                cursor: "pointer",
                background: idx === activeIndex ? "var(--color-surface-hover, #eef)" : "transparent",
                display: "flex", flexDirection: "column", gap: "2px",
              }}
            >
              <span style={{ fontWeight: 500 }}>{row.name}</span>
              <span style={{ fontSize: "11px", color: "var(--color-text-muted, #666)" }}>
                {row.type_line || ""}{row.mana_cost ? `  ·  ${row.mana_cost}` : ""}
              </span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
