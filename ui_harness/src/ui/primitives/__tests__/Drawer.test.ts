/**
 * Vitest tests for Drawer primitive `side` prop — left/right anchoring.
 *
 * The original Drawer was left-anchored only. A new `side` prop lets
 * callers (e.g. CombosDrawer) request a right-anchored variant. The
 * default must remain `"left"` so all existing LeftRail consumers stay
 * BYTE-IDENTICAL — the existing leftRailDrawer.test.ts already pins
 * the left-anchored class names; this file additionally verifies:
 *
 *   - `side="left"` and the default both produce `top-0 left-0` + `border-r`.
 *   - `side="right"` produces `top-0 right-0` + `border-l`.
 *
 * Per project convention, no @testing-library/react — uses
 * react-dom/server.renderToString.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import { Drawer, DrawerContent, DrawerTitle } from "../Drawer";


function renderDrawer(side: "left" | "right" | undefined): string {
  const contentProps: { side?: "left" | "right" } = {};
  if (side !== undefined) contentProps.side = side;
  const raw = renderToString(
    React.createElement(
      Drawer,
      { open: true, onOpenChange: () => undefined },
      React.createElement(
        DrawerContent,
        contentProps,
        React.createElement(DrawerTitle, {}, "Title"),
        "Body",
      ),
    ),
  );
  return raw.replace(/<!-- -->/g, "");
}


describe("Drawer side prop — anchoring class output", () => {
  test("default (no side prop) → left-anchored classes (top-0 left-0 + border-r)", () => {
    const html = renderDrawer(undefined);
    expect(html).toContain("fixed top-0 left-0");
    expect(html).toContain("border-r");
    expect(html).not.toContain("fixed top-0 right-0");
    expect(html).not.toContain("border-l ");
  });

  test('side="left" → left-anchored classes (matches default)', () => {
    const html = renderDrawer("left");
    expect(html).toContain("fixed top-0 left-0");
    expect(html).toContain("border-r");
    expect(html).not.toContain("fixed top-0 right-0");
  });

  test('side="right" → right-anchored classes (top-0 right-0 + border-l)', () => {
    const html = renderDrawer("right");
    expect(html).toContain("fixed top-0 right-0");
    expect(html).toContain("border-l");
    expect(html).not.toContain("fixed top-0 left-0");
    expect(html).not.toContain("border-r ");
  });

  test('side="right" still has h-full + role=dialog + aria-modal (parity with left)', () => {
    const html = renderDrawer("right");
    expect(html).toContain("h-full");
    expect(html).toContain('role="dialog"');
    expect(html).toContain('aria-modal="true"');
    expect(html).toContain("Title");
    expect(html).toContain("Body");
  });
});
