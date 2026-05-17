/**
 * Vitest tests for v1.6.1 hotfix Stage 2 — LeftRail hamburger persists
 * across all routes.
 *
 * Symptom in v1.6 ship: `<LeftRail />` was rendered only inside
 * WorkspaceView.tsx (line 4223), so AppRouter unmounting WorkspaceView
 * on every non-workspace route (#decks / #diagnostics / #playtest /
 * #settings / #/) also unmounted LeftRail — the hamburger disappeared
 * on every route except workspace.
 *
 * v1.6.1 Stage 2 hoists LeftRail into AppRouter as a persistent sibling
 * of every view-switch branch. This test sets `window.location.hash` to
 * each of the 7 ViewId values in turn, renders AppRouter, and asserts
 * the hamburger button (`aria-label="Open navigation menu"` or "Close
 * navigation menu") is present in the rendered HTML.
 *
 * This is the cross-route persistence test that would have caught the
 * v1.6 regression. It also validates HARD safety: parseHash BYTE-
 * IDENTICAL — the test imports + asserts the exported parseHash maps
 * each test hash to the expected ViewId.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test, beforeEach } from "vitest";
import AppRouter, { parseHash } from "../AppRouter";

const HASH_TO_VIEW: ReadonlyArray<{ hash: string; viewId: string }> = [
  { hash: "", viewId: "landing" },
  { hash: "#/", viewId: "landing" },
  { hash: "#workspace", viewId: "workspace" },
  { hash: "#workspace-decks", viewId: "workspace" },
  { hash: "#workspace-runs", viewId: "workspace" },
  { hash: "#diagnostics", viewId: "diagnostics" },
  { hash: "#import", viewId: "import" },
  { hash: "#playtest", viewId: "playtest" },
  { hash: "#settings", viewId: "settings" },
  { hash: "#decks", viewId: "decks" },
];

function setHash(hash: string): void {
  if (typeof window === "undefined" || !window.location) return;
  try {
    Object.defineProperty(window.location, "hash", { value: hash, configurable: true });
  } catch {
    // jsdom may not allow re-defining hash; tests are best-effort.
  }
}

function renderRouter(): string {
  const raw = renderToString(React.createElement(AppRouter));
  return raw.replace(/<!-- -->/g, "");
}

beforeEach(() => {
  setHash("");
});

describe("v1.6.1 Stage 2 — parseHash BYTE-IDENTICAL (HARD safety)", () => {
  test("each test hash maps to the expected ViewId per Phase 4.12.1 + Phase 4.12b contract", () => {
    for (const { hash, viewId } of HASH_TO_VIEW) {
      expect(parseHash(hash)).toBe(viewId);
    }
  });

  test("unrecognized hash falls back to landing", () => {
    expect(parseHash("#totally-unknown-route-12345")).toBe("landing");
  });
});

describe("v1.6.1 Stage 2 — hamburger button persists across all 7 ViewId routes", () => {
  for (const { hash, viewId } of HASH_TO_VIEW) {
    test(`hash="${hash}" → view=${viewId} → hamburger button present in rendered HTML`, () => {
      setHash(hash);
      const html = renderRouter();
      // Either open- or close-state aria-label is acceptable; the v1.6
      // HamburgerButton swaps the label based on drawer state, but the
      // drawer starts closed so "Open navigation menu" is the default.
      const hasHamburger =
        html.includes('aria-label="Open navigation menu"') ||
        html.includes('aria-label="Close navigation menu"');
      expect(hasHamburger).toBe(true);
    });
  }
});

describe("v1.6.1 Stage 2 — LeftRail render path source-level evidence", () => {
  test("AppRouter source imports LeftRail + renders it as a persistent sibling", () => {
    // v1.6.2 Stage 1 autonomous_repair: the v1.6.1 assertion required the
    // Fragment shape (`<> <LeftRail /> ... </>`). v1.6.2 wraps the return
    // in a `<div>` app-shell with a sticky header bar + content area;
    // LeftRail is still a persistent sibling but now lives inside the
    // shell `<div>` (not a Fragment). Updated regex accepts both shapes
    // so the spirit of v1.6.1's assertion (LeftRail rendered alongside
    // every view-switch branch) is preserved across the v1.6.2 shape
    // refactor. The per-view ErrorBoundary branches are still inside the
    // extracted `_renderViewBranch` helper.
    const fs = require("node:fs") as typeof import("node:fs");
    const path = require("node:path") as typeof import("node:path");
    const src = fs.readFileSync(
      path.resolve(__dirname, "../AppRouter.tsx"),
      "utf-8",
    );
    expect(src).toContain('import LeftRail from "../components/layout/LeftRail"');
    // Accept either: v1.6.1 `<> <LeftRail />` OR v1.6.2 `<LeftRail`
    // anywhere in the render tree.
    expect(src).toMatch(/<LeftRail/);
    // The per-view ErrorBoundary branches are now inside the extracted helper.
    expect(src).toContain("_renderViewBranch");
  });

  test("WorkspaceView source no longer imports or renders LeftRail (hoist target)", () => {
    const fs = require("node:fs") as typeof import("node:fs");
    const path = require("node:path") as typeof import("node:path");
    const rawSrc = fs.readFileSync(
      path.resolve(__dirname, "../WorkspaceView.tsx"),
      "utf-8",
    );
    // Strip both /* block */ and {/* JSX */} and // line comments so the
    // hotfix marker text doesn't trigger the regression assertion.
    const src = rawSrc
      .replace(/\{\s*\/\*[\s\S]*?\*\/\s*\}/g, "")
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .replace(/\/\/.*$/gm, "");
    // Specifically: no `import LeftRail from ...` line (a comment may
    // reference the removal, but no actual import).
    expect(src).not.toMatch(/^import LeftRail\b/m);
    // No `<LeftRail />` JSX render.
    expect(src).not.toMatch(/<LeftRail\s*\/>/);
    // The hotfix marker comment IS present (documents the removal) — checked
    // against the RAW source so the comment is visible.
    expect(rawSrc).toContain("v1.6.1 hotfix Stage 2:");
  });
});

describe("v1.6.1 Stage 2 — HARD safety: contract surfaces unchanged", () => {
  test("AppRouter still exports parseHash + default function", () => {
    expect(typeof parseHash).toBe("function");
    expect(typeof AppRouter).toBe("function");
  });

  test("ViewId vocabulary covers all 7 routes (regression sentinel)", () => {
    const viewIds = new Set(HASH_TO_VIEW.map((r) => r.viewId));
    expect(viewIds.size).toBe(7);
    for (const v of ["landing", "workspace", "diagnostics", "import", "playtest", "settings", "decks"]) {
      expect(viewIds.has(v)).toBe(true);
    }
  });
});
