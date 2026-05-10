/**
 * File-upload dispatcher — Phase 4.3.
 *
 * Dispatches a File's text contents to the right text parser based on
 * filename + content heuristics. Browser File API only — no Node fs reads.
 *
 * Supported extensions: .txt / .dec / .json / (no-extension).
 *
 * Heuristics:
 * - .json with Archidekt-shape `{cards: [...]}` → parse via _parse_archidekt-
 *   compatible JSON-text fallback (best effort; Engine-4A's URL importer is
 *   preferred for live Archidekt). Phase 4.3 ships a stub that surfaces
 *   "json_offline_import_deferred" and falls back to plain-text view.
 * - .dec → parseMtgoText (MTGO is the canonical .dec source).
 * - .txt or content with `(SET) NUM` patterns → parseArenaText.
 * - else parsePlainText.
 *
 * Per autonomous_repair_log soft-safety #4 (rare formats queued as 4.3.1):
 * .cod / .o8d / .ydk / .ygopro / etc. fall back to parsePlainText with
 * unknowns[] surfacing per-line failures.
 */
import { parseArenaText } from "./arenaText";
import { parseMtgoText } from "./mtgoText";
import { _parsePlainText } from "./plainText";
import type { ImportResult } from "./types";
import { makeImportResult } from "./types";

const ARENA_PATTERN = /\([A-Za-z0-9]+\)\s+\d+\s*$/m;

export function detectFormatFromText(text: string, filename?: string): "arena_text" | "mtgo_text" | "plain_text" | "json" {
  const ext = (filename ?? "").toLowerCase().split(".").pop() ?? "";
  if (ext === "json") return "json";
  if (ext === "dec") return "mtgo_text";
  if (ARENA_PATTERN.test(text)) return "arena_text";
  return "plain_text";
}

export function parseTextWithDispatch(text: string, filename?: string): ImportResult {
  const format = detectFormatFromText(text, filename);
  if (format === "arena_text") return parseArenaText(text);
  if (format === "mtgo_text") return parseMtgoText(text);
  if (format === "json") {
    return makeImportResult({
      status: "DEFERRED",
      source: "file_upload",
      reason_code: "json_offline_import_deferred",
      deferred_reason:
        "JSON file imports defer to the URL importer flow; .json offline imports queued as 4.3.1.",
    });
  }
  // Default plain-text dispatch but tag as file_upload for analytics.
  return _parsePlainText(text, "file_upload");
}

/**
 * Read a File via FileReader and dispatch. Returns a Promise to keep the
 * caller's effect signature consistent with URL imports.
 */
export async function parseFile(file: File): Promise<ImportResult> {
  const text = await file.text();
  return parseTextWithDispatch(text, file.name);
}
