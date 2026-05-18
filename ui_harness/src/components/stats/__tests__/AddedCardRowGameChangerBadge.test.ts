/**
 * Vitest tests for AddedCardRow Game-Changer (GC) badge — Phase A.
 *
 * The row receives an optional `gameChangers: ReadonlySet<string>` prop.
 * When the row's card name is in the set, a GC Badge renders next to
 * the name with a fixed tooltip describing the bracket-floor impact.
 *
 * Per project convention, no @testing-library/react — uses
 * react-dom/server.renderToString.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import AddedCardRow, {
  GAME_CHANGER_TOOLTIP_TEXT,
  type AddedCardEntry,
} from "../AddedCardRow";
import AddedCardsPanel from "../AddedCardsPanel";


function renderRow(entry: AddedCardEntry, gameChangers?: ReadonlySet<string>): string {
  const props: { entry: AddedCardEntry; gameChangers?: ReadonlySet<string> } = { entry };
  if (gameChangers !== undefined) props.gameChangers = gameChangers;
  const raw = renderToString(
    React.createElement(
      "ul",
      {},
      React.createElement(AddedCardRow, props),
    ),
  );
  return raw.replace(/<!-- -->/g, "");
}


function renderPanel(
  rows: ReadonlyArray<AddedCardEntry>,
  gameChangers?: ReadonlySet<string>,
): string {
  const props: { rows: ReadonlyArray<AddedCardEntry>; gameChangers?: ReadonlySet<string> } = {
    rows,
  };
  if (gameChangers !== undefined) props.gameChangers = gameChangers;
  const raw = renderToString(React.createElement(AddedCardsPanel, props));
  return raw.replace(/<!-- -->/g, "");
}


describe("AddedCardRow — GC badge presence", () => {
  test("renders GC badge when card name is in the gameChangers set", () => {
    const html = renderRow(
      { name: "Sol Ring" },
      new Set(["Sol Ring"]),
    );
    expect(html).toContain("Sol Ring");
    expect(html).toContain('data-game-changer="true"');
    expect(html).toContain(">GC<");
  });

  test("does NOT render GC badge when name is absent from the set", () => {
    const html = renderRow(
      { name: "Forest" },
      new Set(["Sol Ring", "Mana Crypt"]),
    );
    expect(html).toContain("Forest");
    expect(html).not.toContain('data-game-changer="true"');
  });

  test("does NOT render GC badge when gameChangers prop is omitted (back-compat)", () => {
    const html = renderRow({ name: "Sol Ring" });
    expect(html).toContain("Sol Ring");
    expect(html).not.toContain('data-game-changer="true"');
  });

  test("GC badge carries the spec tooltip text via title attribute", () => {
    const html = renderRow(
      { name: "Cyclonic Rift" },
      new Set(["Cyclonic Rift"]),
    );
    expect(html).toContain('data-game-changer="true"');
    expect(html).toContain(`title="${GAME_CHANGER_TOOLTIP_TEXT}"`);
    // Tooltip copy must match the spec verbatim.
    expect(GAME_CHANGER_TOOLTIP_TEXT).toBe(
      "Game Changer — counts toward bracket floor (1-3 GCs → B3, 4+ → B4)",
    );
  });

  test("trims whitespace before comparing (Set lookup uses .trim()'d name)", () => {
    const html = renderRow(
      { name: "  Sol Ring  " },
      new Set(["Sol Ring"]),
    );
    expect(html).toContain('data-game-changer="true"');
  });
});


describe("AddedCardsPanel → AddedCardRow GC integration", () => {
  test("Sol Ring + Cyclonic Rift + Smothering Tithe all render GC badges", () => {
    // Spec scenario: a deck with three known GC cards must surface badges
    // on each row. The set is hardcoded here to mirror the real
    // gc_v0_userlist_2025-11-20.json entries used by the engine.
    const gc = new Set<string>([
      "Sol Ring",
      "Cyclonic Rift",
      "Smothering Tithe",
      "Mana Crypt",
    ]);
    const rows: AddedCardEntry[] = [
      { name: "Sol Ring" },
      { name: "Cyclonic Rift" },
      { name: "Smothering Tithe" },
      { name: "Forest" },
      { name: "Counterspell" },
    ];
    const html = renderPanel(rows, gc);
    // Three GC badges expected (one per matching row).
    const gcMatches = html.match(/data-game-changer="true"/g) ?? [];
    expect(gcMatches.length).toBe(3);
    // Non-GC rows still render their card name.
    expect(html).toContain("Forest");
    expect(html).toContain("Counterspell");
  });

  test("Empty gameChangers set → no badges across the panel", () => {
    const rows: AddedCardEntry[] = [
      { name: "Sol Ring" },
      { name: "Counterspell" },
    ];
    const html = renderPanel(rows, new Set<string>());
    expect(html).not.toContain('data-game-changer="true"');
  });
});


describe("Tooltip copy contract", () => {
  test("exported constant text matches spec verbatim", () => {
    expect(GAME_CHANGER_TOOLTIP_TEXT).toBe(
      "Game Changer — counts toward bracket floor (1-3 GCs → B3, 4+ → B4)",
    );
  });
});
