/**
 * useBuildStreaming — Mega-task v5 Phase 3 hook.
 *
 * Connects to POST /agent/build_deck_v1/stream and exposes the current
 * phase, elapsed time, cumulative LLM cost, and the final deck response.
 *
 * EventSource is GET-only and cannot send a request body, so we use fetch
 * + ReadableStream + a manual SSE parser. Cancellation is provided via
 * AbortController; the controller is returned so callers can wire it to
 * a Cancel button (Phase 4).
 */
import { useCallback, useEffect, useRef, useState } from "react";

export type ProgressPhase =
  | "intent_interpreter"
  | "candidate_pool"
  | "select_deck"
  | "c21_c22_parallel"
  | "validate_swap"
  | "final_critic"
  | "mana_base"
  | "card_advantage"
  | "structural_safety_net"
  | "complete";

export type ProgressEvent = {
  phase: ProgressPhase | string;
  status: "started" | "completed";
  elapsed_s: number;
  cost_usd: number;
  calls_so_far: number;
  response?: unknown;
};

export type BuildStreamingRequest = {
  db_snapshot_id: string;
  commander: string;
  bracket: string;
  theme_hints: string[];
  must_include_cards: string[];
};

export type BuildStreamingState = {
  /** Are we currently streaming a build? */
  isStreaming: boolean;
  /** Most-recent phase + status emitted by the server. */
  currentPhase: ProgressPhase | string | null;
  currentStatus: "started" | "completed" | null;
  /** Elapsed seconds since stream start (mirrors the last event's elapsed_s). */
  elapsedSeconds: number;
  /** Cumulative LLM cost so far (mirrors the last event's cost_usd). */
  cumulativeCostUsd: number;
  /** Ordered log of every progress event received. */
  events: ProgressEvent[];
  /** Set when a complete event arrives. */
  finalResponse: unknown | null;
  /** Set when the stream aborts / errors. */
  errorMessage: string | null;
};

const INITIAL_STATE: BuildStreamingState = {
  isStreaming: false,
  currentPhase: null,
  currentStatus: null,
  elapsedSeconds: 0,
  cumulativeCostUsd: 0,
  events: [],
  finalResponse: null,
  errorMessage: null,
};

export type UseBuildStreamingOptions = {
  apiBaseUrl?: string;
};

export type UseBuildStreamingResult = BuildStreamingState & {
  /** Start a streaming build. Aborts any in-flight stream. */
  start: (req: BuildStreamingRequest) => Promise<void>;
  /** Abort the in-flight stream. Final response is not produced. */
  cancel: () => void;
  /** Reset to initial state (e.g., when the user closes the deck view). */
  reset: () => void;
};

/**
 * Parses the SSE wire format from a raw text buffer. Returns the parsed
 * events + the remaining (unterminated) buffer so the caller can hold it
 * across chunks. SSE frames end at "\n\n" or "\r\n\r\n".
 */
function _parseSseBuffer(
  buffer: string
): { events: Array<{ event: string; data: string }>; remaining: string } {
  const events: Array<{ event: string; data: string }> = [];
  const normalized = buffer.replace(/\r\n/g, "\n");
  const parts = normalized.split("\n\n");
  // The last part may be incomplete (no terminating \n\n) — keep it.
  const remaining = parts.pop() ?? "";
  for (const block of parts) {
    let eventType = "message";
    const dataLines: string[] = [];
    for (const line of block.split("\n")) {
      if (!line) continue;
      if (line.startsWith(":")) continue; // SSE comment / keep-alive
      if (line.startsWith("event:")) {
        eventType = line.slice("event:".length).trim();
      } else if (line.startsWith("data:")) {
        dataLines.push(line.slice("data:".length).trim());
      }
    }
    if (dataLines.length > 0) {
      events.push({ event: eventType, data: dataLines.join("\n") });
    }
  }
  return { events, remaining };
}

export function useBuildStreaming(
  options: UseBuildStreamingOptions = {}
): UseBuildStreamingResult {
  const { apiBaseUrl } = options;
  const [state, setState] = useState<BuildStreamingState>(INITIAL_STATE);
  const abortRef = useRef<AbortController | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    return () => {
      mountedRef.current = false;
      abortRef.current?.abort();
    };
  }, []);

  const reset = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setState(INITIAL_STATE);
  }, []);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setState((prev) => ({ ...prev, isStreaming: false }));
  }, []);

  const start = useCallback(
    async (req: BuildStreamingRequest) => {
      // Abort any in-flight stream first.
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setState({ ...INITIAL_STATE, isStreaming: true });

      const base =
        apiBaseUrl ??
        ((import.meta as ImportMeta).env?.VITE_API_BASE_URL as string | undefined) ??
        "http://localhost:8000";

      let response: Response;
      try {
        response = await fetch(`${base}/agent/build_deck_v1/stream`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "text/event-stream",
          },
          body: JSON.stringify(req),
          signal: controller.signal,
        });
      } catch (exc) {
        if (!mountedRef.current) return;
        const message =
          (exc as Error).name === "AbortError"
            ? "Build cancelled."
            : `Failed to open stream: ${(exc as Error).message ?? String(exc)}`;
        setState((prev) => ({
          ...prev,
          isStreaming: false,
          errorMessage: message,
        }));
        return;
      }

      if (!response.ok) {
        if (!mountedRef.current) return;
        setState((prev) => ({
          ...prev,
          isStreaming: false,
          errorMessage: `Stream failed: HTTP ${response.status} ${response.statusText}`,
        }));
        return;
      }

      const reader = response.body?.getReader();
      if (!reader) {
        setState((prev) => ({
          ...prev,
          isStreaming: false,
          errorMessage: "Stream failed: response had no body reader.",
        }));
        return;
      }

      const decoder = new TextDecoder("utf-8");
      let buffer = "";

      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const { events, remaining } = _parseSseBuffer(buffer);
          buffer = remaining;
          if (events.length === 0) continue;
          for (const ev of events) {
            let parsed: ProgressEvent | null = null;
            try {
              parsed = JSON.parse(ev.data) as ProgressEvent;
            } catch {
              parsed = null;
            }
            if (!parsed) continue;
            if (ev.event === "error") {
              if (!mountedRef.current) return;
              setState((prev) => ({
                ...prev,
                isStreaming: false,
                errorMessage:
                  (parsed as { error?: string }).error ??
                  "Stream emitted an error event.",
              }));
              return;
            }
            if (!mountedRef.current) return;
            setState((prev) => {
              const nextEvents = [...prev.events, parsed!];
              const isComplete =
                parsed!.phase === "complete" && parsed!.status === "completed";
              return {
                ...prev,
                events: nextEvents,
                currentPhase: parsed!.phase,
                currentStatus: parsed!.status,
                elapsedSeconds: parsed!.elapsed_s,
                cumulativeCostUsd: parsed!.cost_usd,
                finalResponse: isComplete ? parsed!.response ?? null : prev.finalResponse,
                isStreaming: !isComplete,
              };
            });
          }
        }
      } catch (exc) {
        if (!mountedRef.current) return;
        // AbortError on cancel — treat as user-initiated cancel.
        const isAbort = (exc as Error).name === "AbortError";
        setState((prev) => ({
          ...prev,
          isStreaming: false,
          errorMessage: isAbort
            ? null
            : `Stream read error: ${(exc as Error).message ?? String(exc)}`,
        }));
      }
    },
    [apiBaseUrl]
  );

  return { ...state, start, cancel, reset };
}

// Exported for tests.
export const __testing = { _parseSseBuffer };
