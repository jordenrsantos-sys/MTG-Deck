/**
 * usePartialBuild — Phase 4 BUNDLE Stage 4 (sub-task 4.4).
 *
 * React hook for debounced calls to Engine-4A's `POST /build/partial?layers=`
 * endpoint. Used by SeedBuilderPanel to refresh seed_synergy_detection_v1
 * + commander_recommendation_v1 on each keystroke without hammering the
 * full /build pipeline.
 *
 * Per autonomous_repair_log soft-safety #4 (debounce tier selection: test
 * 100/200/250/500ms, ship lowest non-thrashing): default 200ms — fast
 * enough to feel real-time, slow enough to coalesce typing bursts.
 *
 * Per HARD safety: NO direct engine reads (calls Engine-4A endpoint only),
 * NO calibration data, deterministic given (request, layers, debounceMs).
 */
import { useCallback, useEffect, useRef, useState } from "react";

export const DEFAULT_DEBOUNCE_MS = 200;

export type PartialBuildRequest = {
  db_snapshot_id: string;
  profile_id: string;
  bracket_id: string;
  format?: string;
  commander?: string;
  cards: ReadonlyArray<string>;
};

export type PartialBuildState = {
  loading: boolean;
  error: string | null;
  result: Record<string, unknown> | null;
  resolution: Record<string, unknown> | null;
  lastFetchedAt: number | null;
};

export type UsePartialBuildOptions = {
  apiBase?: string;
  layers: ReadonlyArray<string>;
  debounceMs?: number;
  fetcher?: typeof fetch;
  enabled?: boolean;
};

const INITIAL_STATE: PartialBuildState = {
  loading: false,
  error: null,
  result: null,
  resolution: null,
  lastFetchedAt: null,
};

/**
 * Returns the latest partial-build state + a `trigger(req)` function. The
 * hook debounces consecutive trigger calls — only the last call inside a
 * `debounceMs` window actually fetches.
 */
export function usePartialBuild(opts: UsePartialBuildOptions): {
  state: PartialBuildState;
  trigger: (req: PartialBuildRequest) => void;
  cancel: () => void;
} {
  const {
    apiBase = "",
    layers,
    debounceMs = DEFAULT_DEBOUNCE_MS,
    fetcher = fetch,
    enabled = true,
  } = opts;
  const [state, setState] = useState<PartialBuildState>(INITIAL_STATE);
  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inflightAbort = useRef<AbortController | null>(null);
  const layersParam = layers.join(",");

  const cancel = useCallback(() => {
    if (debounceTimer.current !== null) {
      clearTimeout(debounceTimer.current);
      debounceTimer.current = null;
    }
    if (inflightAbort.current) {
      inflightAbort.current.abort();
      inflightAbort.current = null;
    }
  }, []);

  const trigger = useCallback(
    (req: PartialBuildRequest) => {
      if (!enabled) return;
      cancel();
      debounceTimer.current = setTimeout(() => {
        const controller = new AbortController();
        inflightAbort.current = controller;
        setState((prev) => ({ ...prev, loading: true, error: null }));
        const body = {
          db_snapshot_id: req.db_snapshot_id,
          profile_id: req.profile_id,
          bracket_id: req.bracket_id,
          format: req.format ?? "commander",
          commander: req.commander ?? "",
          cards: [...req.cards],
          engine_patches_v0: [],
        };
        fetcher(
          `${apiBase}/build/partial?layers=${encodeURIComponent(layersParam)}`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
            signal: controller.signal,
          },
        )
          .then(async (resp) => {
            if (!resp.ok) {
              const txt = await resp.text().catch(() => "");
              throw new Error(`HTTP ${resp.status}: ${txt.slice(0, 120)}`);
            }
            return resp.json();
          })
          .then((data) => {
            const dataObj = (data ?? {}) as Record<string, unknown>;
            setState({
              loading: false,
              error: null,
              result: (dataObj.result as Record<string, unknown>) ?? null,
              resolution: (dataObj.resolution as Record<string, unknown>) ?? null,
              lastFetchedAt: Date.now(),
            });
          })
          .catch((e: unknown) => {
            if (e instanceof Error && e.name === "AbortError") return;
            setState((prev) => ({
              ...prev,
              loading: false,
              error: e instanceof Error ? e.message : "unknown error",
            }));
          });
      }, debounceMs);
    },
    [apiBase, layersParam, debounceMs, fetcher, enabled, cancel],
  );

  useEffect(() => () => cancel(), [cancel]);

  return { state, trigger, cancel };
}
