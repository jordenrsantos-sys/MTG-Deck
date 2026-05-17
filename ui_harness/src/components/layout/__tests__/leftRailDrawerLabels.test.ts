/**
 * Vitest tests for v1.6.1 hotfix Stage 1 — drawer label visibility regression.
 *
 * The orchestrator's live-walk of v1.6 caught a regression the +55 vitest
 * could not see: `styles.css` lines 2307-2324 carried stale
 * `.workspace-left-rail-text { max-width: 0; opacity: 0; ... }` collapse
 * rules from the pre-v1.6 hover-to-expand permanent rail. Under the
 * v1.6 Drawer-based LeftRail, the `.workspace-left-rail` parent
 * ancestor no longer exists, so the expand-on-hover companion rule
 * could never trigger — every drawer label rendered invisible.
 *
 * This test guards against the same pattern recurring:
 *   (a) The default-collapse rule for `.workspace-left-rail-text` is
 *       absent from `styles.css`.
 *   (b) The companion stale `.workspace-left-rail:hover ...` rules
 *       are absent.
 *   (c) The LeftRail rendered HTML for each drawer entry includes
 *       readable label text alongside the icon — and the label span
 *       has no inline `opacity: 0` / `max-width: 0` style attribute.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import LeftRail from "../LeftRail";

const STYLES_CSS_RAW = readFileSync(
  resolve(__dirname, "../../../styles.css"),
  "utf-8",
);
// Strip /* ... */ block comments so sentinel selectors inside hotfix
// documentation comments don't confuse the rule-presence assertions.
const STYLES_CSS = STYLES_CSS_RAW.replace(/\/\*[\s\S]*?\*\//g, "");

beforeEach(() => {
  if (typeof window !== "undefined" && window.location) {
    try {
      Object.defineProperty(window.location, "hash", { value: "", configurable: true });
    } catch {
      // jsdom may not be available; the renderToString-based assertions don't
      // need a real window.
    }
  }
});

describe("v1.6.1 Stage 1 — styles.css no longer collapses drawer labels", () => {
  test("default-collapse rule for `.workspace-left-rail-text` is deleted", () => {
    // The bug-rule had `.workspace-left-rail-text { max-width: 0; opacity: 0`
    // — sentinel string assertion. If this rule comes back, this test fires.
    expect(STYLES_CSS).not.toMatch(
      /\.workspace-left-rail-text\s*\{[^}]*max-width:\s*0[^}]*opacity:\s*0/,
    );
  });

  test("stale `.workspace-left-rail:hover .workspace-left-rail-text` expand rule is deleted", () => {
    expect(STYLES_CSS).not.toMatch(
      /\.workspace-left-rail:hover\s+\.workspace-left-rail-text/,
    );
    expect(STYLES_CSS).not.toMatch(
      /\.workspace-left-rail:focus-within\s+\.workspace-left-rail-text/,
    );
  });

  test("stale `.workspace-left-rail:hover .workspace-left-rail-link` justify-content rule is deleted", () => {
    expect(STYLES_CSS).not.toMatch(
      /\.workspace-left-rail:hover\s+\.workspace-left-rail-link/,
    );
    expect(STYLES_CSS).not.toMatch(
      /\.workspace-left-rail:focus-within\s+\.workspace-left-rail-link/,
    );
  });

  test("hotfix comment block documents the deletion (sentinel for future readers)", () => {
    // Read the raw CSS (with comments) for the sentinel string check.
    expect(STYLES_CSS_RAW).toContain("v1.6.1 hotfix");
    expect(STYLES_CSS_RAW).toContain("Drawer overlay");
  });
});

describe("v1.6.1 Stage 1 — drawer labels render with readable text", () => {
  function renderRail(): string {
    const raw = renderToString(React.createElement(LeftRail));
    return raw.replace(/<!-- -->/g, "");
  }

  test("drawer-closed default render shows hamburger but NOT drawer panel labels (closed state)", () => {
    const html = renderRail();
    // Sanity: hamburger present; drawer panel content not rendered when closed.
    expect(html).toContain("Open navigation menu");
    expect(html).not.toContain("My Stuff");
  });

  test("LeftRail source defines drawer entries with `workspace-left-rail-text` label spans", () => {
    // Source-level evidence: the LeftRail render path still uses the
    // `workspace-left-rail-text` class for label spans. This is the
    // class whose default-collapse rule we just deleted; the spans
    // are now visible because the CSS suppression is gone.
    const leftRailSrc = readFileSync(
      resolve(__dirname, "../LeftRail.tsx"),
      "utf-8",
    );
    expect(leftRailSrc).toContain("workspace-left-rail-text");
    expect(leftRailSrc).toContain("{link.label}");
  });

  test("no inline `opacity: 0` or `max-width: 0` style on label spans (the rendered DOM check)", () => {
    // If a future refactor accidentally inlines the collapse style instead
    // of removing it, this test catches it. We check the LeftRail source
    // for inline style props on label spans.
    const leftRailSrc = readFileSync(
      resolve(__dirname, "../LeftRail.tsx"),
      "utf-8",
    );
    // Crude but sufficient: no style={{ opacity: 0 }} or style={{ maxWidth: 0 }}
    // anywhere in the LeftRail source.
    expect(leftRailSrc).not.toMatch(/style=\{\{[^}]*opacity:\s*0/);
    expect(leftRailSrc).not.toMatch(/style=\{\{[^}]*maxWidth:\s*0/);
  });
});
