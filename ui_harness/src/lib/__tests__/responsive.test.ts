/** Vitest tests for lib/responsive (Phase 4.11 Stage 1). */
import { describe, expect, test } from "vitest";
import {
  BREAKPOINT_PX,
  TOUCH_TARGET_PX,
  getBreakpoint,
  isMobileLayout,
  shouldCollapseLeftRail,
} from "../responsive";

describe("breakpoint constants", () => {
  test("Tailwind defaults intact", () => {
    expect(BREAKPOINT_PX.sm).toBe(640);
    expect(BREAKPOINT_PX.md).toBe(768);
    expect(BREAKPOINT_PX.lg).toBe(1024);
    expect(BREAKPOINT_PX.xl).toBe(1280);
  });
  test("touch target = iOS HIG 44px", () => {
    expect(TOUCH_TARGET_PX).toBe(44);
  });
});

describe("getBreakpoint", () => {
  test("phone widths < 768px", () => {
    expect(getBreakpoint(320)).toBe("phone");
    expect(getBreakpoint(414)).toBe("phone");
    expect(getBreakpoint(640)).toBe("phone");
    expect(getBreakpoint(767)).toBe("phone");
  });
  test("tablet 768–1023px", () => {
    expect(getBreakpoint(768)).toBe("tablet");
    expect(getBreakpoint(900)).toBe("tablet");
    expect(getBreakpoint(1023)).toBe("tablet");
  });
  test("desktop 1024–1279px", () => {
    expect(getBreakpoint(1024)).toBe("desktop");
    expect(getBreakpoint(1279)).toBe("desktop");
  });
  test("wide ≥ 1280px", () => {
    expect(getBreakpoint(1280)).toBe("wide");
    expect(getBreakpoint(1920)).toBe("wide");
  });
  test("non-finite / negative → phone (defensive)", () => {
    expect(getBreakpoint(NaN)).toBe("phone");
    expect(getBreakpoint(-1)).toBe("phone");
  });
});

describe("isMobileLayout / shouldCollapseLeftRail", () => {
  test("isMobileLayout true only when phone", () => {
    expect(isMobileLayout(414)).toBe(true);
    expect(isMobileLayout(768)).toBe(false);
    expect(isMobileLayout(1024)).toBe(false);
  });
  test("shouldCollapseLeftRail mirrors phone-or-narrower", () => {
    expect(shouldCollapseLeftRail(700)).toBe(true);
    expect(shouldCollapseLeftRail(768)).toBe(false);
  });
});
