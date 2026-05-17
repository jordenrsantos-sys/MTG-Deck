/**
 * Vitest tests for v1.6 Stage 4 — better empty states.
 *
 * WorkspaceView empty-state card replaces Krenko fallback when
 * `state.source === "fallback"`. GoldfishView opt-in synthetic-deck
 * empty state replaces silent auto-load when `deckSource === "synthetic"`.
 *
 * Per AUTOMATION_RULES halt-and-ask + autonomous_repair_log soft-safety:
 * no @testing-library/react install. Source-level grep proves the
 * marker data-attributes + CTA copy + gate conditionals are in place;
 * reducer INITIAL_STATE BYTE-IDENTICAL (HARD #9) verified by reading
 * lib/workspaceDeckState.ts.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WS_SRC = readFileSync(
  resolve(__dirname, "../../views/WorkspaceView.tsx"),
  "utf-8",
);
const GF_SRC = readFileSync(
  resolve(__dirname, "../../views/GoldfishView.tsx"),
  "utf-8",
);
const REDUCER_SRC = readFileSync(
  resolve(__dirname, "../../lib/workspaceDeckState.ts"),
  "utf-8",
);

describe("v1.6 Stage 4 — WorkspaceView empty-state card (Krenko fallback hidden)", () => {
  test("empty-state card has v16 data-attribute marker", () => {
    expect(WS_SRC).toContain('data-v16-stage="empty-state-card"');
  });

  test("renders ONLY when isEditMode AND source==='fallback' (gates Krenko display)", () => {
    expect(WS_SRC).toMatch(/isEditMode && deckState\.source === "fallback"/);
  });

  test("CTA: 'Import a deck' → routes to #import hash", () => {
    expect(WS_SRC).toContain('aria-label="Import a deck"');
    expect(WS_SRC).toMatch(/window\.location\.hash = "#import";/);
  });

  test("CTA: 'Start from a seed' → switches to SEED_BUILDER mode", () => {
    expect(WS_SRC).toContain('aria-label="Start from a seed"');
    expect(WS_SRC).toMatch(/setPageMode\("SEED_BUILDER"\)/);
  });

  test("empty-state headline + body copy matches spec", () => {
    expect(WS_SRC).toContain("No deck loaded");
    expect(WS_SRC).toContain(
      "Import a deck from Archidekt, Arena, MTGO, or plain text — or start a new deck from a seed.",
    );
  });
});

describe("v1.6 Stage 4 — GoldfishView opt-in synthetic deck (no auto-load)", () => {
  test("empty-state card has v16 data-attribute marker", () => {
    expect(GF_SRC).toContain('data-v16-stage="goldfish-empty-state"');
  });

  test("auto-start useEffect gates synthetic — only fires for real-deck sources", () => {
    expect(GF_SRC).toMatch(/if \(deckSource === "synthetic"\) return;/);
  });

  test("pre_game render branches on deckSource === 'synthetic' for empty state", () => {
    expect(GF_SRC).toMatch(/deckSource === "synthetic"\s*\?/);
  });

  test("CTAs: Open importer / Open workspace / Use sample deck", () => {
    expect(GF_SRC).toContain('aria-label="Open importer"');
    expect(GF_SRC).toContain('aria-label="Open workspace"');
    expect(GF_SRC).toContain('aria-label="Use sample deck"');
  });

  test("'Use sample deck' button explicitly dispatches START_GAME (opt-in, not auto)", () => {
    const idx = GF_SRC.indexOf('aria-label="Use sample deck"');
    expect(idx).toBeGreaterThan(0);
    const slice = GF_SRC.slice(Math.max(0, idx - 400), idx);
    expect(slice).toMatch(/dispatch\(\{ type: "START_GAME"/);
  });

  test("empty-state copy mentions importer + workspace options", () => {
    expect(GF_SRC).toContain("No deck staged");
    expect(GF_SRC).toContain("Import a deck or save one from the workspace first");
  });

  test("3-source precedence chain (deckSource computation) BYTE-IDENTICAL", () => {
    // HARD #11: Stage 4 modifies fallback RENDER, not the precedence logic.
    // Sentinel: the 3 source-determination branches are still in place.
    expect(GF_SRC).toMatch(/if \(stagedPayload\?\.deck && stagedPayload\.deck\.length > 0\) return "playtest_staged"/);
    expect(GF_SRC).toMatch(
      /if \(workspaceActiveDeck\?\.decklist && workspaceActiveDeck\.decklist\.trim\(\) !== ""\) return "workspace_active"/,
    );
    expect(GF_SRC).toMatch(/return "synthetic";/);
  });
});

describe("v1.6 Stage 4 — reducer INITIAL_STATE BYTE-IDENTICAL (HARD #9)", () => {
  test("INITIAL_STATE still includes 'fallback' source value (Krenko defense-in-depth)", () => {
    // Sentinel: reducer INITIAL_STATE preserved; only WorkspaceView render
    // gates on source === "fallback" to hide Krenko display.
    expect(REDUCER_SRC).toContain('source: "fallback"');
  });

  test("USER_EDIT_DECK_TEXT action handler preserved (v1.1 contract)", () => {
    expect(REDUCER_SRC).toContain('case "USER_EDIT_DECK_TEXT"');
  });

  test("LOAD_SAVED_DECK action handler preserved", () => {
    expect(REDUCER_SRC).toContain('case "LOAD_SAVED_DECK"');
  });
});
