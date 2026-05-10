/** Vitest tests for lib/devLog (Phase 4 BUNDLE Stage 4.10). */
import { describe, expect, test, vi, afterEach } from "vitest";
import { devError, devLog, devWarn, __IS_DEV_FOR_TESTS } from "../devLog";

describe("devLog production gating", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  test("module exports the three callable wrappers", () => {
    expect(typeof devLog).toBe("function");
    expect(typeof devWarn).toBe("function");
    expect(typeof devError).toBe("function");
  });

  test("calling wrappers does not throw regardless of env", () => {
    expect(() => devLog("a", 1)).not.toThrow();
    expect(() => devWarn("b")).not.toThrow();
    expect(() => devError("c", new Error("x"))).not.toThrow();
  });

  test("DEV detection resolves to a boolean (vitest runs in DEV)", () => {
    expect(typeof __IS_DEV_FOR_TESTS).toBe("boolean");
  });
});
