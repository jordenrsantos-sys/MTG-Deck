/**
 * Vitest tests for v1.6.2 Stage 1 — app-shell header bar.
 *
 * v1.6.1 hoisted LeftRail to AppRouter (good); but the hamburger button
 * was rendered as a fixed-position overlay (`fixed top-2 left-2 z-modal`)
 * which clipped page content on routes like LandingView / SavedDecksView
 * where there's no top padding to make room for the overlay.
 *
 * v1.6.2 Stage 1 wraps AppRouter's return in an outer app-shell `<div>`
 * containing a thin 48px sticky header `<header>` element with the
 * HamburgerButton rendered INLINE inside it. View content sits in a
 * separate `<div>` below the header in natural document flow — no more
 * fixed-position clipping.
 *
 * Implementation: LeftRail gains optional controlled-mode props
 * (`open` / `onOpenChange`) + a `renderHamburger` mode toggle (default
 * "fixed" for v1.6/v1.6.1 back-compat); AppRouter passes
 * `renderHamburger="none"` + the controlled open state so AppRouter is
 * the single source of truth.
 *
 * Tests assert the new app-shell structure + that HamburgerButton lives
 * inside the header element (not as a sibling overlay) + that content
 * is in its own container below.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import AppRouter from "../AppRouter";
import LeftRail from "../../components/layout/LeftRail";

const APPROUTER_SRC_RAW = readFileSync(
  resolve(__dirname, "../AppRouter.tsx"),
  "utf-8",
);
const LEFTRAIL_SRC_RAW = readFileSync(
  resolve(__dirname, "../../components/layout/LeftRail.tsx"),
  "utf-8",
);
// Strip JS comments so source-level greps don't false-positive on docs.
function _stripJs(src: string): string {
  return src
    .replace(/\{\s*\/\*[\s\S]*?\*\/\s*\}/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/.*$/gm, "");
}
const APPROUTER_SRC = _stripJs(APPROUTER_SRC_RAW);
const LEFTRAIL_SRC = _stripJs(LEFTRAIL_SRC_RAW);

beforeEach(() => {
  if (typeof window !== "undefined" && window.location) {
    try {
      Object.defineProperty(window.location, "hash", { value: "", configurable: true });
    } catch {
      // jsdom env may not be available; renderToString doesn't need it.
    }
  }
});

function renderRouterHtml(): string {
  return renderToString(React.createElement(AppRouter)).replace(/<!-- -->/g, "");
}

describe("v1.6.2 Stage 1 — app-shell header bar structure", () => {
  test("AppRouter source has v1.6.2 app-shell + header-bar data-attribute markers", () => {
    expect(APPROUTER_SRC).toContain('data-v162-stage="app-shell"');
    expect(APPROUTER_SRC).toContain('data-v162-stage="app-shell-header"');
    expect(APPROUTER_SRC).toContain('data-v162-stage="app-shell-content"');
  });

  test("header element is a sticky 48px bar (h-12 sticky top-0)", () => {
    // Tailwind: h-12 = 3rem = 48px. sticky top-0 keeps it pinned.
    expect(APPROUTER_SRC).toMatch(/<header[\s\S]*?sticky[\s\S]*?top-0[\s\S]*?h-12/);
  });

  test("rendered HTML contains a <header> with role=banner + aria-label='App shell header'", () => {
    const html = renderRouterHtml();
    expect(html).toMatch(/<header[^>]*role="banner"[^>]*aria-label="App shell header"/);
  });

  test("HamburgerButton renders INLINE inside the header element (not a fixed-position sibling)", () => {
    const html = renderRouterHtml();
    // Locate the header's open-close span. Hamburger aria-label appears
    // INSIDE the header element.
    const headerMatch = html.match(/<header[^>]*>([\s\S]*?)<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerInner = headerMatch?.[1] ?? "";
    const hasHamburger =
      headerInner.includes('aria-label="Open navigation menu"') ||
      headerInner.includes('aria-label="Close navigation menu"');
    expect(hasHamburger).toBe(true);
  });

  test("view content sits below the header in its own container (natural flow)", () => {
    const html = renderRouterHtml();
    // The app-shell-content data-attribute marks the content wrapper.
    expect(html).toContain('data-v162-stage="app-shell-content"');
    // Content wrapper appears AFTER the header element in document order.
    const headerEndIdx = html.indexOf("</header>");
    const contentIdx = html.indexOf('data-v162-stage="app-shell-content"');
    expect(headerEndIdx).toBeGreaterThan(-1);
    expect(contentIdx).toBeGreaterThan(headerEndIdx);
  });

  test("HamburgerButton is NOT rendered inside a fixed-position overlay anymore (no top-2/left-2/z-modal sibling)", () => {
    const html = renderRouterHtml();
    // The pre-v1.6.2 layout placed hamburger inside `<div class="fixed top-2 left-2 z-modal">`.
    // After Stage 1, the only HamburgerButton render path goes through the
    // header bar. We assert no `fixed top-2 left-2` div exists in the HTML.
    expect(html).not.toMatch(/<div\s+class="fixed top-2 left-2/);
  });
});

describe("v1.6.2 Stage 1 — LeftRail controlled-mode props (additive)", () => {
  test("LeftRail exports a typed LeftRailProps with open/onOpenChange/renderHamburger", () => {
    // Use RAW source (don't strip comments — JSDoc inside the type body
    // gets stripped along with field separators by the aggressive strip
    // regex). The raw source has all 3 prop declarations.
    expect(LEFTRAIL_SRC_RAW).toContain("export type LeftRailProps");
    expect(LEFTRAIL_SRC_RAW).toMatch(/open\?:\s*boolean/);
    expect(LEFTRAIL_SRC_RAW).toMatch(/onOpenChange\?:\s*\(open:\s*boolean\)\s*=>\s*void/);
    expect(LEFTRAIL_SRC_RAW).toMatch(/renderHamburger\?:\s*"fixed"\s*\|\s*"inline"\s*\|\s*"none"/);
  });

  test("LeftRail default renderHamburger is 'fixed' (back-compat with v1.6 + v1.6.1 callers)", () => {
    // Sentinel: the default value in the destructure is "fixed".
    expect(LEFTRAIL_SRC_RAW).toMatch(/renderHamburger\s*=\s*"fixed"/);
  });

  test("LeftRail uses controlled-mode when `open` prop is provided (isControlled branch)", () => {
    expect(LEFTRAIL_SRC_RAW).toMatch(/isControlled\s*=\s*controlledOpen\s*!==\s*undefined/);
    expect(LEFTRAIL_SRC_RAW).toMatch(/isControlled\s*\?\s*Boolean\(controlledOpen\)\s*:\s*internalOpen/);
  });

  test("AppRouter passes renderHamburger='none' + controlled-mode props to LeftRail", () => {
    expect(APPROUTER_SRC).toMatch(/<LeftRail[\s\S]+?open=\{drawerOpen\}/);
    expect(APPROUTER_SRC).toMatch(/<LeftRail[\s\S]+?onOpenChange=\{setDrawerOpen\}/);
    expect(APPROUTER_SRC).toMatch(/<LeftRail[\s\S]+?renderHamburger="none"/);
  });

  test("uncontrolled-mode rendering preserved (renderHamburger='fixed' default) — sentinel render", () => {
    // Render LeftRail with NO props (the v1.6/v1.6.1 call shape).
    const html = renderToString(React.createElement(LeftRail)).replace(/<!-- -->/g, "");
    // Default 'fixed' mode wraps hamburger in `fixed top-2 left-2 z-modal` div.
    expect(html).toMatch(/<div\s+class="fixed top-2 left-2 z-modal"/);
    expect(html).toContain('aria-label="Open navigation menu"');
  });

  test("renderHamburger='none' → no hamburger rendered by LeftRail (parent owns it)", () => {
    const html = renderToString(
      React.createElement(LeftRail, { renderHamburger: "none" }),
    ).replace(/<!-- -->/g, "");
    // Hamburger button absent from LeftRail output.
    expect(html).not.toContain('aria-label="Open navigation menu"');
    expect(html).not.toContain('aria-label="Close navigation menu"');
  });
});

describe("v1.6.2 Stage 1 — fixed-position clipping eliminated (regression sentinel)", () => {
  test("rendered AppRouter HTML has exactly ONE hamburger (header bar, not overlay)", () => {
    const html = renderRouterHtml();
    const openMatches = html.match(/aria-label="Open navigation menu"/g) ?? [];
    const closeMatches = html.match(/aria-label="Close navigation menu"/g) ?? [];
    const total = openMatches.length + closeMatches.length;
    // Exactly one hamburger across the entire app-shell render (drawer
    // starts closed, so it's the open-state label).
    expect(total).toBe(1);
  });

  test("the lone hamburger lives inside <header>, not as a top-level fixed sibling", () => {
    const html = renderRouterHtml();
    const headerMatch = html.match(/<header[^>]*>([\s\S]*?)<\/header>/);
    expect(headerMatch).not.toBeNull();
    const headerInner = headerMatch?.[1] ?? "";
    // The hamburger aria-label appears INSIDE the header.
    expect(headerInner).toMatch(/aria-label="Open navigation menu"/);
  });
});
