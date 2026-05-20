/**
 * Vitest for Pillar A.6 Stage 1 — Game-Changer badges on the deck list.
 *
 * Bug 1: amber GC chips were only rendering in AddedCardsPanel rows;
 * the canonical deck list (DeckEditorPanel → CardList → CardRow) did
 * not receive the game_changers_v1 set, so cards on the GC userlist
 * showed no badge in the visible deck.
 *
 * Fix: extend CardListItem with `nameBadge`, render it inside CardRow
 * next to the card name, plumb a `gameChangers?: ReadonlySet<string>`
 * prop through DeckEditorPanel, and wire `gameChangerNameSet` from
 * WorkspaceView's completionResult into that prop.
 *
 * Per project vitest convention (no @testing-library/react) — uses
 * react-dom/server.renderToString + source-string sentinels.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import CardList, { type CardListItem } from "../../cards/CardList";
import Badge from "../../../ui/primitives/Badge";
import { GAME_CHANGER_TOOLTIP_TEXT } from "../../stats/AddedCardRow";


const DECK_EDITOR_SRC = readFileSync(
  resolve(__dirname, "../DeckEditorPanel.tsx"),
  "utf-8",
);

const WORKSPACE_VIEW_SRC = readFileSync(
  resolve(__dirname, "../../../views/WorkspaceView.tsx"),
  "utf-8",
);


function gcBadgeNode(): React.ReactNode {
  return React.createElement(
    "span",
    {
      "data-game-changer": "true",
      title: GAME_CHANGER_TOOLTIP_TEXT,
      "aria-label": GAME_CHANGER_TOOLTIP_TEXT,
      className: "inline-flex",
    },
    React.createElement(Badge, { variant: "warn", children: "GC" }),
  );
}


function renderList(items: CardListItem[]): string {
  const raw = renderToString(React.createElement(CardList, { items }));
  return raw.replace(/<!-- -->/g, "");
}


describe("CardListItem.nameBadge — inline badge rendered next to name", () => {
  test("CardRow renders nameBadge inside workspace-card-row-name-badge span", () => {
    const html = renderList([
      { name: "1 Cyclonic Rift", nameBadge: gcBadgeNode() },
    ]);
    expect(html).toContain("Cyclonic Rift");
    expect(html).toContain("workspace-card-row-name-badge");
    expect(html).toContain('data-game-changer="true"');
    expect(html).toContain(">GC<");
  });

  test("Rows without nameBadge omit the badge wrapper", () => {
    const html = renderList([{ name: "1 Forest" }]);
    expect(html).toContain("Forest");
    expect(html).not.toContain("workspace-card-row-name-badge");
    expect(html).not.toContain('data-game-changer="true"');
  });
});


describe("DeckEditorPanel — gameChangers prop wired into the row builder", () => {
  test("props type declares gameChangers as an optional ReadonlySet<string>", () => {
    expect(DECK_EDITOR_SRC).toMatch(/gameChangers\?\:\s*ReadonlySet<string>/);
    expect(DECK_EDITOR_SRC).toMatch(/gameChangers,\s*\}\s*=\s*props/);
  });

  test("buildDeckEditorRowItem trims row name then checks the GC set", () => {
    expect(DECK_EDITOR_SRC).toMatch(/const\s+trimmedName\s*=\s*row\.name\.trim\(\)/);
    expect(DECK_EDITOR_SRC).toMatch(/gameChangers\.has\(trimmedName\)/);
  });

  test("GC badge node uses Badge variant=warn + GC label", () => {
    expect(DECK_EDITOR_SRC).toMatch(/<Badge\s+variant="warn">GC<\/Badge>/);
    expect(DECK_EDITOR_SRC).toMatch(/data-game-changer="true"/);
  });

  test("nameBadge slot wires through CardListItem", () => {
    expect(DECK_EDITOR_SRC).toMatch(/nameBadge:\s*isGameChanger/);
  });

  test("Tooltip copy reuses the AddedCardRow exported constant", () => {
    expect(DECK_EDITOR_SRC).toMatch(
      /import\s+\{[^}]*GAME_CHANGER_TOOLTIP_TEXT[^}]*\}\s+from\s+["']\.\.\/stats\/AddedCardRow["']/,
    );
    expect(GAME_CHANGER_TOOLTIP_TEXT).toContain("Game Changer");
  });
});


describe("WorkspaceView → DeckEditorPanel wiring", () => {
  test("passes gameChangerNameSet as the gameChangers prop", () => {
    const editorBlock = WORKSPACE_VIEW_SRC.match(/<DeckEditorPanel[\s\S]*?\/>/);
    expect(editorBlock).not.toBeNull();
    const block = editorBlock?.[0] ?? "";
    expect(block).toMatch(/gameChangers=\{gameChangerNameSet\}/);
  });

  test("gameChangerNameSet derives from completionResult.game_changers_v1", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(/completionResult\?\.game_changers_v1/);
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /const\s+gameChangerNameSet\s*=\s*useMemo<ReadonlySet<string>>/,
    );
  });
});


describe("Smoke: deck-row rendering for the bug-report scenario", () => {
  test("Cyclonic Rift / Field of the Dead / Jeska's Will all render the GC chip when in the set", () => {
    const gcSet = new Set<string>(["Cyclonic Rift", "Field of the Dead", "Jeska's Will"]);
    const rowNames = ["Cyclonic Rift", "Field of the Dead", "Jeska's Will", "Plains"];
    const items: CardListItem[] = rowNames.map((name) => ({
      name: `1 ${name}`,
      nameBadge: gcSet.has(name) ? gcBadgeNode() : null,
    }));
    const html = renderList(items);
    const badgeMatches = html.match(/data-game-changer="true"/g) ?? [];
    expect(badgeMatches.length).toBe(3);
    expect(html).toContain("Plains");
  });
});
