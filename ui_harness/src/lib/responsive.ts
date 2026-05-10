/**
 * responsive — Phase 4.11 mobile pass.
 *
 * Centralizes Tailwind breakpoint metadata + a small `getBreakpoint(width)`
 * helper used by tests + a few components needing JS-side reactivity (the
 * majority of responsive layout is pure Tailwind utilities).
 *
 * Per autonomous_repair_log soft-safety #7 (tablet vs phone heuristic):
 *   - sm = 640px  → phone landscape & up
 *   - md = 768px  → tablet portrait & up        (LeftRail collapse boundary)
 *   - lg = 1024px → tablet landscape / desktop  (3-pane layout boundary)
 *   - xl = 1280px → wide desktop
 *
 * Per autonomous_repair_log soft-safety #8 (touch-target size insufficient):
 *   - TOUCH_TARGET_PX = 44 (iOS HIG minimum); enforced as `min-h-[44px]`
 *     on the Button primitive.
 */

export const BREAKPOINT_PX = {
  sm: 640,
  md: 768,
  lg: 1024,
  xl: 1280,
} as const;

export type Breakpoint = "phone" | "tablet" | "desktop" | "wide";

export const TOUCH_TARGET_PX = 44;

/** Map a viewport width to a coarse-grained breakpoint label. */
export function getBreakpoint(width: number): Breakpoint {
  if (!Number.isFinite(width) || width < BREAKPOINT_PX.md) return "phone";
  if (width < BREAKPOINT_PX.lg) return "tablet";
  if (width < BREAKPOINT_PX.xl) return "desktop";
  return "wide";
}

/** True when the viewport is tablet-portrait or narrower. */
export function isMobileLayout(width: number): boolean {
  return getBreakpoint(width) === "phone";
}

/** True when the LeftRail should collapse into a hamburger Dialog. */
export function shouldCollapseLeftRail(width: number): boolean {
  return width < BREAKPOINT_PX.md;
}
