/**
 * Vitest for v1.7.1 micro-hotfix — Saved Decks "Load" button commander sync.
 *
 * Bug observed in v1.7 Cowork browser-walk (2026-05-16):
 *   1. User opens Saved Decks.
 *   2. Clicks "Load <DeckName>" on a deck whose decklist begins
 *      `Commander\n1 Zada, Hedron Grinder\nDeck\n...` with
 *      `commander_oracle_id: null`.
 *   3. Workspace receives the decklist but the active workspace's
 *      `commander` field stays at the previous value (e.g.
 *      "Krenko, Mob Boss"). Metric pill h2 + Commander pill display
 *      the stale commander.
 *
 * Root cause: `SavedDecksView.handleLoad` stages a payload with
 * `commander: ""` hardcoded. WorkspaceView's hydration useEffect at
 * line 1398 falls back to `INITIAL_DECK_STATE.commander` (the Krenko
 * fallback) when the staged commander is empty — by design, because
 * the IMPORT_STAGED_KEY contract treats empty as "no commander". The
 * fix is to derive the commander from the saved decklist's Commander
 * header BEFORE staging — reusing the existing `parsePlainText`
 * parser (which already handles `Commander\n1 X` correctly under
 * Phase 4.3's contract).
 *
 * The fix is Option A from the v1.7.1 spec: SavedDecksView parses
 * the commander before dispatching, no reducer / payload-shape change.
 *
 * Test approach: this project intentionally avoids
 * @testing-library/react (per Phase 4.x autonomous_repair_log). The
 * test exercises the wire-up at TWO levels — source sentinel against
 * SavedDecksView.tsx + behavioral assertion that simulates the
 * handler's logic against the existing `parsePlainText` parser and
 * the existing `readStagedImport` / `IMPORT_STAGED_KEY` adapter.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { parsePlainText } from "../../parsers/plainText";


const SAVED_DECKS_VIEW_SRC = readFileSync(
  resolve(__dirname, "../SavedDecksView.tsx"),
  "utf-8",
);


function deriveCommanderForStage(decklist: string): string {
  // Replicates the SHAPE OF the fixed handler's commander-derivation
  // logic. The actual SavedDecksView source must encode the same
  // logic — the source-sentinel tests below verify that.
  const parsed = parsePlainText(decklist);
  if (parsed.status === "OK" && parsed.deck && parsed.deck.commander) {
    return parsed.deck.commander.name;
  }
  return "";
}


describe("v1.7.1 — SavedDecksView.handleLoad source sentinels (fix wires commander into the staged payload)", () => {
  test("handleLoad NO LONGER hardcodes commander: '' in the staged payload", () => {
    // The pre-fix source had a literal `commander: ""` inside the
    // payload object. The fix replaces that with a derived value from
    // the decklist's Commander header. Regression sentinel against
    // re-introducing the hardcoded empty string.
    const handlerBlock = SAVED_DECKS_VIEW_SRC.match(
      /function\s+handleLoad[\s\S]*?(?=function\s+handleDelete|\n  function\s|\n}\s*$)/,
    );
    expect(handlerBlock).not.toBeNull();
    const handler = handlerBlock?.[0] ?? "";
    expect(handler).not.toMatch(/commander:\s*""\s*,/);
  });

  test("handleLoad invokes parsePlainText (the reused parser) to derive the commander", () => {
    expect(SAVED_DECKS_VIEW_SRC).toMatch(/import\s*\{[\s\S]*?\bparsePlainText\b[\s\S]*?\}\s*from\s*"\.\.\/parsers\/plainText"/);
    const handlerBlock = SAVED_DECKS_VIEW_SRC.match(
      /function\s+handleLoad[\s\S]*?(?=function\s+handleDelete|\n  function\s|\n}\s*$)/,
    );
    const handler = handlerBlock?.[0] ?? "";
    expect(handler).toMatch(/parsePlainText\s*\(/);
  });

  test("handleLoad still stages IMPORT_STAGED_KEY (existing 4.6 contract preserved)", () => {
    expect(SAVED_DECKS_VIEW_SRC).toMatch(/IMPORT_STAGED_KEY/);
    const handlerBlock = SAVED_DECKS_VIEW_SRC.match(
      /function\s+handleLoad[\s\S]*?(?=function\s+handleDelete|\n  function\s|\n}\s*$)/,
    );
    const handler = handlerBlock?.[0] ?? "";
    expect(handler).toMatch(/localStorage\.setItem\s*\(\s*IMPORT_STAGED_KEY/);
  });
});


describe("v1.7.1 — Load handler commander-derivation logic (pure-function simulation)", () => {
  test("Zada decklist (Commander\\n1 Zada, Hedron Grinder\\nDeck...) derives 'Zada, Hedron Grinder'", () => {
    const decklist = "Commander\n1 Zada, Hedron Grinder\nDeck\n1 Sol Ring\n1 Lightning Bolt";
    expect(deriveCommanderForStage(decklist)).toBe("Zada, Hedron Grinder");
  });

  test("Loading does NOT leak a prior Krenko workspace commander (derivation is purely from decklist)", () => {
    const decklist = "Commander\n1 Zada, Hedron Grinder\nDeck\n1 Sol Ring";
    const derived = deriveCommanderForStage(decklist);
    expect(derived).toBe("Zada, Hedron Grinder");
    expect(derived).not.toBe("Krenko, Mob Boss");
  });

  test("Decklist without a Commander section derives empty (preserves current fallback path through hydration)", () => {
    // Edge case: a saved deck whose decklist is just card lines (no
    // Commander header). Pre-fix and post-fix BOTH stage empty here;
    // WorkspaceView's hydration useEffect at line 1398 falls back to
    // INITIAL_DECK_STATE.commander when staged commander is empty.
    // This sentinel ensures the fix does NOT regress the no-header case.
    expect(deriveCommanderForStage("1 Sol Ring\n1 Lightning Bolt")).toBe("");
  });

  test("Partner-commander decklist takes the FIRST commander (documented v1.7.1 limitation)", () => {
    // For `Commander\n1 Ishai\n1 Reyhan\nDeck...`, parsePlainText
    // assigns the first card to `commander` and treats the second
    // as a regular deck entry. This is a known limitation: the
    // workspace's `commander` field is scalar; partner-commander
    // second-name propagation is deferred to v1.7.2 if needed.
    // Strictly better than the pre-fix behavior (which staged "" →
    // fell back to Krenko fallback) — at least the primary commander
    // now syncs.
    const decklist = "Commander\n1 Ishai, Ojutai Dragonspeaker\n1 Reyhan, Last of the Abzan\nDeck\n1 Sol Ring";
    expect(deriveCommanderForStage(decklist)).toBe("Ishai, Ojutai Dragonspeaker");
  });

  test("Standalone commander entry without a Deck section derives the commander correctly", () => {
    // Some saved decklists omit the `Deck` divider entirely:
    //   Commander\n1 Krenko, Mob Boss\n1 Goblin Matron\n1 Skirk Prospector
    // The parser still recognizes the Commander section header; the
    // first card under it becomes the commander; the rest fall into
    // commander-section as overflow (which parsePlainText demotes to
    // regular cards). Smoke-test that the derivation is non-empty.
    const decklist = "Commander\n1 Krenko, Mob Boss\n1 Goblin Matron";
    expect(deriveCommanderForStage(decklist)).toBe("Krenko, Mob Boss");
  });
});
