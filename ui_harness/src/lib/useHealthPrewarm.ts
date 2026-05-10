/**
 * useHealthPrewarm — Phase 4 BUNDLE Stage 4.10 (polish).
 *
 * Fires a one-shot `GET /health` on App mount to warm Engine-4A's
 * import paths and registry, avoiding the 1.4-second cold-start the
 * first /build pays after server boot. Results are intentionally
 * discarded — this is a side-channel warmup only.
 *
 * Per HARD safety: NO state/output changes user-visible — this hook
 * does not block render, does not show errors, does not retry; failure
 * is silent (the next real /build will surface its own errors).
 */
import { useEffect } from "react";
import { devLog, devWarn } from "./devLog";

export function useHealthPrewarm(apiBase: string, fetcher: typeof fetch = fetch): void {
  useEffect(() => {
    let aborted = false;
    const ctrl = new AbortController();
    const t0 = performance.now();
    fetcher(`${apiBase}/health`, { method: "GET", signal: ctrl.signal })
      .then((resp) => {
        if (aborted) return;
        const ms = Math.round(performance.now() - t0);
        devLog(`[health-prewarm] ${resp.status} in ${ms}ms`);
      })
      .catch((e: unknown) => {
        if (aborted) return;
        if (e instanceof Error && e.name === "AbortError") return;
        devWarn(`[health-prewarm] failed (silent):`, e);
      });
    return () => {
      aborted = true;
      ctrl.abort();
    };
  }, [apiBase, fetcher]);
}
