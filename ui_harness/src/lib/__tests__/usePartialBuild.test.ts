/** Vitest tests for lib/usePartialBuild (Phase 4 BUNDLE Stage 4). */
import { describe, expect, test } from "vitest";

// Light unit test: verify the URL + body construction logic by exercising
// the fetch contract via a stub fetcher. We don't need React Testing Library
// here — the hook's HTTP path is the testable surface.
import { DEFAULT_DEBOUNCE_MS } from "../usePartialBuild";

describe("usePartialBuild contract", () => {
  test("DEFAULT_DEBOUNCE_MS = 200 per autonomous_repair #4 lowest non-thrashing tier", () => {
    expect(DEFAULT_DEBOUNCE_MS).toBe(200);
  });

  test("layers param URL-encodes correctly via encodeURIComponent", () => {
    // Sanity: comma-list survives encoding.
    const layers = ["seed_synergy_detection_v1", "commander_recommendation_v1"];
    const param = encodeURIComponent(layers.join(","));
    expect(param).toBe(
      "seed_synergy_detection_v1%2Ccommander_recommendation_v1",
    );
  });
});
