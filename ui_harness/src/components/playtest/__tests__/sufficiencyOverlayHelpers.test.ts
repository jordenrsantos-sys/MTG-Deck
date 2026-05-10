/** Vitest tests for SufficiencyOverlay pure helpers (Phase 4.9 Stage 3). */
import { describe, expect, test } from "vitest";
import {
  findCheckpointForTurn,
  statusVariantFromString,
  type ProbabilityCheckpointLayer,
} from "../SufficiencyOverlay";

describe("statusVariantFromString", () => {
  test("PASS → success, WARN → warn, FAIL → error", () => {
    expect(statusVariantFromString("PASS")).toBe("success");
    expect(statusVariantFromString("WARN")).toBe("warn");
    expect(statusVariantFromString("FAIL")).toBe("error");
  });

  test("unknown / undefined → neutral (no fabrication)", () => {
    expect(statusVariantFromString("WHATEVER")).toBe("neutral");
    expect(statusVariantFromString(undefined)).toBe("neutral");
    expect(statusVariantFromString("")).toBe("neutral");
  });
});

describe("findCheckpointForTurn — shape tolerance", () => {
  test("null / undefined layer → null (graceful, never throws)", () => {
    expect(findCheckpointForTurn(null, 1)).toBeNull();
    expect(findCheckpointForTurn(undefined, 5)).toBeNull();
  });

  test("missing checkpoints field → null", () => {
    const layer: ProbabilityCheckpointLayer = { version: "v1" };
    expect(findCheckpointForTurn(layer, 1)).toBeNull();
  });

  test("array shape — match by entry.turn field", () => {
    const layer: ProbabilityCheckpointLayer = {
      checkpoints: [
        { turn: 1, status: "PASS", probability: 0.9 },
        { turn: 2, status: "WARN" },
        { turn: 3, status: "FAIL" },
      ],
    };
    expect(findCheckpointForTurn(layer, 2)?.status).toBe("WARN");
  });

  test("array shape — fallback to (turn-1) index when no turn field present", () => {
    const layer: ProbabilityCheckpointLayer = {
      checkpoints: [{ status: "PASS" }, { status: "WARN" }, { status: "FAIL" }],
    };
    expect(findCheckpointForTurn(layer, 1)?.status).toBe("PASS");
    expect(findCheckpointForTurn(layer, 3)?.status).toBe("FAIL");
  });

  test("array shape — out-of-range returns null", () => {
    const layer: ProbabilityCheckpointLayer = {
      checkpoints: [{ turn: 1, status: "PASS" }],
    };
    expect(findCheckpointForTurn(layer, 5)).toBeNull();
  });

  test("record shape — keyed by string turn number", () => {
    const layer: ProbabilityCheckpointLayer = {
      checkpoints: { "3": { status: "FAIL", probability: 0.31 } },
    };
    expect(findCheckpointForTurn(layer, 3)?.status).toBe("FAIL");
    expect(findCheckpointForTurn(layer, 4)).toBeNull();
  });
});
