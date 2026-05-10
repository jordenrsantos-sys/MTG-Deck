/**
 * Archidekt URL parser — Phase 4.3.
 *
 * Calls Engine-4A's `POST /import/url` endpoint via the Vite proxy
 * (vite.config.ts forwards `/import` → `${VITE_API_TARGET}`). Server-side
 * dispatch identifies Archidekt vs Moxfield vs EDHREC and returns the
 * canonical ImportResult shape per `repo/api/import_url_v1.py`.
 *
 * Moxfield + EDHREC URLs hit the SAME endpoint; Engine-4A returns
 * `status=DEFERRED` for those sources per its registry. ImportView surfaces
 * the deferred_reason to the user as "not available — file paste only"
 * affordance per spec.
 */
import { makeImportResult, type ImportResult } from "./types";

export type ImportFetcher = (url: string, init?: RequestInit) => Promise<Response>;

export type ImportFromUrlOptions = {
  fetcher?: ImportFetcher;
  apiBase?: string;
};

const DEFAULT_API_BASE = ""; // Vite proxy handles same-origin /import/url

export async function importFromUrl(
  url: string,
  opts: ImportFromUrlOptions = {},
): Promise<ImportResult> {
  if (typeof url !== "string" || url.trim() === "") {
    return makeImportResult({
      status: "INVALID_REQUEST",
      source: "unknown",
      reason_code: "empty_url",
    });
  }
  const fetcher = opts.fetcher ?? fetch;
  const apiBase = opts.apiBase ?? DEFAULT_API_BASE;
  let resp: Response;
  try {
    resp = await fetcher(`${apiBase}/import/url`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: url.trim() }),
    });
  } catch (e) {
    return makeImportResult({
      status: "FETCH_ERROR",
      source: "unknown",
      reason_code: "network_error",
      deferred_reason: e instanceof Error ? e.message.slice(0, 200) : "fetch threw",
    });
  }
  let body: unknown;
  try {
    body = await resp.json();
  } catch {
    return makeImportResult({
      status: "FETCH_ERROR",
      source: "unknown",
      reason_code: `non_json_response_status_${resp.status}`,
    });
  }
  // Engine-4A's response is already in canonical ImportResult shape;
  // pass through but defensively re-wrap to guarantee field presence.
  if (typeof body !== "object" || body === null) {
    return makeImportResult({
      status: "FETCH_ERROR",
      source: "unknown",
      reason_code: "response_not_object",
    });
  }
  const r = body as Record<string, unknown>;
  return makeImportResult({
    status: (r.status as ImportResult["status"]) ?? "FETCH_ERROR",
    source: (r.source as ImportResult["source"]) ?? "unknown",
    deck: (r.deck as ImportResult["deck"]) ?? null,
    unknowns: Array.isArray(r.unknowns)
      ? (r.unknowns as ImportResult["unknowns"])
      : [],
    reason_code: (r.reason_code as string | null | undefined) ?? null,
    deferred_reason: (r.deferred_reason as string | null | undefined) ?? null,
  });
}
