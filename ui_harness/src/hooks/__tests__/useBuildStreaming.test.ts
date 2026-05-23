/**
 * useBuildStreaming — Mega-task v5 Phase 3 hook tests.
 *
 * The hook itself relies on React + fetch + ReadableStream which require a
 * DOM/JSDOM environment. The project uses environment="node" by default,
 * so we focus on:
 *
 *   - The exported SSE-buffer parser (pure function — covered with unit
 *     tests across edge cases the wire format throws at us).
 *   - Source-grep checks on the hook so a future refactor that breaks the
 *     critical contract (start/cancel/reset, AbortController wiring, the
 *     complete-event short-circuit) gets caught.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { __testing } from "../useBuildStreaming";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const SOURCE_PATH = resolve(__dirname, "../useBuildStreaming.ts");
const SOURCE = readFileSync(SOURCE_PATH, "utf8");

const { _parseSseBuffer } = __testing;

describe("useBuildStreaming — SSE parser", () => {
  test("parses a single complete event", () => {
    const buffer =
      'event: progress\ndata: {"phase":"intent_interpreter","status":"started","elapsed_s":0.0,"cost_usd":0.0,"calls_so_far":0}\n\n';
    const { events, remaining } = _parseSseBuffer(buffer);
    expect(remaining).toBe("");
    expect(events).toHaveLength(1);
    expect(events[0].event).toBe("progress");
    const parsed = JSON.parse(events[0].data);
    expect(parsed.phase).toBe("intent_interpreter");
  });

  test("parses multiple events back-to-back", () => {
    const buffer =
      'event: progress\ndata: {"phase":"a","status":"started","elapsed_s":0.0,"cost_usd":0.0,"calls_so_far":0}\n\n' +
      'event: progress\ndata: {"phase":"b","status":"completed","elapsed_s":1.0,"cost_usd":0.01,"calls_so_far":1}\n\n';
    const { events, remaining } = _parseSseBuffer(buffer);
    expect(remaining).toBe("");
    expect(events).toHaveLength(2);
    expect(JSON.parse(events[0].data).phase).toBe("a");
    expect(JSON.parse(events[1].data).phase).toBe("b");
  });

  test("normalizes \\r\\n line endings (sse-starlette format)", () => {
    const buffer =
      'event: progress\r\ndata: {"phase":"x","status":"completed","elapsed_s":0,"cost_usd":0,"calls_so_far":0}\r\n\r\n';
    const { events, remaining } = _parseSseBuffer(buffer);
    expect(remaining).toBe("");
    expect(events).toHaveLength(1);
    expect(JSON.parse(events[0].data).phase).toBe("x");
  });

  test("returns incomplete trailing block as remaining", () => {
    const buffer =
      'event: progress\ndata: {"phase":"a","status":"started","elapsed_s":0,"cost_usd":0,"calls_so_far":0}\n\n' +
      "event: progress\ndata: {\"phase\":\"b\""; // unterminated
    const { events, remaining } = _parseSseBuffer(buffer);
    expect(events).toHaveLength(1);
    expect(remaining).toContain('{"phase":"b"');
  });

  test("skips SSE comment / keep-alive lines starting with :", () => {
    const buffer =
      ": ping - 2026-05-21\n\n" +
      'event: progress\ndata: {"phase":"x","status":"completed","elapsed_s":0,"cost_usd":0,"calls_so_far":0}\n\n';
    const { events, remaining } = _parseSseBuffer(buffer);
    expect(remaining).toBe("");
    expect(events).toHaveLength(1); // The comment block contributes no event.
    expect(events[0].event).toBe("progress");
  });

  test("returns empty array on empty buffer", () => {
    const { events, remaining } = _parseSseBuffer("");
    expect(events).toEqual([]);
    expect(remaining).toBe("");
  });

  test("defaults event type to 'message' when no event: line", () => {
    const buffer = 'data: {"hello": "world"}\n\n';
    const { events } = _parseSseBuffer(buffer);
    expect(events[0].event).toBe("message");
  });
});

describe("useBuildStreaming — source contract checks", () => {
  test("exports the hook + types", () => {
    expect(SOURCE).toMatch(/export function useBuildStreaming/);
    expect(SOURCE).toMatch(/export type ProgressEvent/);
    expect(SOURCE).toMatch(/export type BuildStreamingState/);
  });

  test("uses AbortController for cancellation", () => {
    expect(SOURCE).toMatch(/new AbortController\(\)/);
    expect(SOURCE).toMatch(/abortRef\.current\?\.abort\(\)/);
  });

  test("returns start, cancel, reset action callbacks", () => {
    expect(SOURCE).toMatch(/const start = useCallback/);
    expect(SOURCE).toMatch(/const cancel = useCallback/);
    expect(SOURCE).toMatch(/const reset = useCallback/);
  });

  test("posts to /agent/build_deck_v1/stream with text/event-stream Accept", () => {
    expect(SOURCE).toMatch(/agent\/build_deck_v1\/stream/);
    expect(SOURCE).toMatch(/Accept:\s*"text\/event-stream"/);
  });

  test("short-circuits isStreaming when the complete event arrives", () => {
    expect(SOURCE).toMatch(/phase\s*===\s*"complete"/);
    expect(SOURCE).toMatch(/isStreaming:\s*!isComplete/);
  });

  test("aborts the in-flight stream on unmount", () => {
    expect(SOURCE).toMatch(/return\s*\(\)\s*=>\s*\{[\s\S]*?abortRef\.current\?\.abort\(\)/m);
  });

  test("(v6 P1 regression) mount effect resets mountedRef=true on every mount", () => {
    // React 18 StrictMode mounts useEffect, immediately fires cleanup
    // (mountedRef=false), then remounts. If the body of the effect does
    // not re-set mountedRef=true, the ref stays false forever, every
    // setState inside the stream loop is gated out, and the UI shows
    // INITIAL_STATE the entire build until the 480s timeout fires.
    //
    // This grep asserts the explicit reset is in place. Removing it
    // reproduces the v6 Phase 1 bug exactly.
    const effectBody = SOURCE.match(
      /useEffect\(\(\)\s*=>\s*\{([\s\S]*?)\},\s*\[\]\);/
    );
    expect(effectBody, "useEffect with [] deps must exist").not.toBeNull();
    expect(effectBody![1]).toMatch(/mountedRef\.current\s*=\s*true/);
  });
});
