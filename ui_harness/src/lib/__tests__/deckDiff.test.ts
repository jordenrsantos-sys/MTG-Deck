/**
 * Vitest tests for deckDiff — v1.3 Stage 1.
 *
 * Covers the diff fallback semantics WorkspaceView relies on when
 * the engine's `added_cards_v1` array is empty: single addition,
 * multi-count duplicate addition, full reshuffle, empty edges,
 * deterministic alphabetical ordering, parser permissiveness
 * (banners + comments + set/collector annotations).
 */
import { describe, expect, test } from "vitest";
import { computeDeckAdditions, _decklistToCountMap } from "../deckDiff";

describe("_decklistToCountMap — parser permissiveness", () => {
  test("empty string → empty map", () => {
    expect(_decklistToCountMap("").size).toBe(0);
  });

  test("single line, no count prefix → count 1", () => {
    const m = _decklistToCountMap("Sol Ring");
    expect(m.get("Sol Ring")).toBe(1);
  });

  test("`Nx Card` syntax → count N", () => {
    const m = _decklistToCountMap("4 Forest\n2x Mountain\n3 Plains");
    expect(m.get("Forest")).toBe(4);
    expect(m.get("Mountain")).toBe(2);
    expect(m.get("Plains")).toBe(3);
  });

  test("merges duplicate-name lines", () => {
    const m = _decklistToCountMap("1 Forest\n3 Forest\n1 Forest");
    expect(m.get("Forest")).toBe(5);
  });

  test("strips Commander/Deck/Sideboard banners", () => {
    const m = _decklistToCountMap("Commander\n1 Krenko\nDeck\n1 Sol Ring\nSideboard\n1 Mountain");
    expect(m.has("Commander")).toBe(false);
    expect(m.has("Deck")).toBe(false);
    expect(m.has("Sideboard")).toBe(false);
    expect(m.get("Krenko")).toBe(1);
    expect(m.get("Sol Ring")).toBe(1);
    expect(m.get("Mountain")).toBe(1);
  });

  test("v1.5: strips extended banner vocabulary (Mainboard / Companion / Tokens)", () => {
    const m = _decklistToCountMap(
      "Mainboard\n1 Sol Ring\nCompanion\n1 Lutri, the Spellchaser\nTokens\n1 Treasure",
    );
    expect(m.has("Mainboard")).toBe(false);
    expect(m.has("Companion")).toBe(false);
    expect(m.has("Tokens")).toBe(false);
    expect(m.get("Sol Ring")).toBe(1);
    expect(m.get("Lutri, the Spellchaser")).toBe(1);
    expect(m.get("Treasure")).toBe(1);
  });

  test("v1.5: case-insensitive banner stripping (COMMANDER / deck / SiDeBoArD)", () => {
    const m = _decklistToCountMap("COMMANDER\n1 Krenko\ndeck\n1 Sol Ring\nSiDeBoArD\n1 Mountain");
    expect(m.has("COMMANDER")).toBe(false);
    expect(m.has("deck")).toBe(false);
    expect(m.has("SiDeBoArD")).toBe(false);
    expect(m.get("Krenko")).toBe(1);
    expect(m.get("Sol Ring")).toBe(1);
    expect(m.get("Mountain")).toBe(1);
  });

  test("v1.5: trailing-colon-tolerant banner stripping (Commander: / Deck : / Mainboard:)", () => {
    const m = _decklistToCountMap("Commander:\n1 Krenko\nDeck :\n1 Sol Ring\nMainboard:\n1 Mountain");
    expect(m.has("Commander:")).toBe(false);
    expect(m.has("Deck :")).toBe(false);
    expect(m.has("Mainboard:")).toBe(false);
    expect(m.get("Krenko")).toBe(1);
    expect(m.get("Sol Ring")).toBe(1);
    expect(m.get("Mountain")).toBe(1);
  });

  test("v1.5: mixed-format multi-section input with full banner vocabulary", () => {
    const text =
      "Commander\n1 Shelob, Child of Ungoliant\n" +
      "Deck\n1 Sol Ring\n1 Arcane Signet\n" +
      "Mainboard\n1 Forest\n" +
      "Companion\n1 Yorion\n" +
      "Sideboard\n1 Negate\n" +
      "Maybeboard\n1 Counterspell\n" +
      "Tokens\n1 Treasure";
    const m = _decklistToCountMap(text);
    // All banner lines stripped.
    for (const b of ["Commander", "Deck", "Mainboard", "Companion", "Sideboard", "Maybeboard", "Tokens"]) {
      expect(m.has(b)).toBe(false);
    }
    // All cards retained.
    expect(m.get("Shelob, Child of Ungoliant")).toBe(1);
    expect(m.get("Sol Ring")).toBe(1);
    expect(m.get("Forest")).toBe(1);
    expect(m.get("Yorion")).toBe(1);
    expect(m.get("Negate")).toBe(1);
    expect(m.get("Counterspell")).toBe(1);
    expect(m.get("Treasure")).toBe(1);
  });

  test("strips comment lines (// + # + ;)", () => {
    const m = _decklistToCountMap("// header\n# section\n; note\n1 Sol Ring");
    expect(m.size).toBe(1);
    expect(m.get("Sol Ring")).toBe(1);
  });

  test("strips set + collector annotations (M21) 132 + foil marker", () => {
    const m = _decklistToCountMap("1 Sol Ring (M21) 132\n1 Forest *F*\n2 Mountain [edition]");
    expect(m.get("Sol Ring")).toBe(1);
    expect(m.get("Forest")).toBe(1);
    expect(m.get("Mountain")).toBe(2);
  });

  test("skips zero/negative counts + malformed lines", () => {
    const m = _decklistToCountMap("0 Sol Ring\n-1 Forest");
    expect(m.size).toBe(0);
  });
});

describe("computeDeckAdditions — pure additions diff", () => {
  test("empty before, single card after → 1 entry", () => {
    const adds = computeDeckAdditions("", "1 Sol Ring");
    expect(adds).toHaveLength(1);
    expect(adds[0].name).toBe("Sol Ring");
    expect(adds[0].reasons_v1).toEqual(["added_during_completion"]);
    expect(adds[0].primitives_added_v1).toEqual([]);
  });

  test("count-bump: Forest x1 → Forest x5 = 4 added entries", () => {
    const adds = computeDeckAdditions("1 Forest", "5 Forest");
    expect(adds).toHaveLength(4);
    for (const e of adds) {
      expect(e.name).toBe("Forest");
      expect(e.reasons_v1).toEqual(["added_during_completion"]);
    }
  });

  test("no change → empty additions", () => {
    const same = "1 Sol Ring\n1 Forest";
    expect(computeDeckAdditions(same, same)).toHaveLength(0);
  });

  test("removals are NOT emitted (cuts out of scope)", () => {
    const adds = computeDeckAdditions("3 Forest\n1 Sol Ring", "1 Forest\n1 Sol Ring");
    expect(adds).toHaveLength(0);
  });

  test("full reshuffle: input cards present + new cards appended", () => {
    const before = "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet";
    const after =
      "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet\n1 Lightning Bolt\n2 Mountain";
    const adds = computeDeckAdditions(before, after);
    expect(adds).toHaveLength(3);
    expect(adds.map((a) => a.name)).toEqual(["Lightning Bolt", "Mountain", "Mountain"]);
  });

  test("deterministic alphabetical ordering across runs", () => {
    const before = "";
    const after = "1 Zur the Enchanter\n1 Sol Ring\n1 Arcane Signet\n1 Mountain";
    const a = computeDeckAdditions(before, after);
    const b = computeDeckAdditions(before, after);
    expect(a.map((e) => e.name)).toEqual(["Arcane Signet", "Mountain", "Sol Ring", "Zur the Enchanter"]);
    expect(a.map((e) => e.name)).toEqual(b.map((e) => e.name));
  });

  test("empty before + empty after → empty additions", () => {
    expect(computeDeckAdditions("", "")).toHaveLength(0);
  });

  test("Shelob canonical 78→100 grows by 22 entries", () => {
    // Mirrors v1.2 Stage 1 canonical Shelob 1010839 deck growth fixture
    // (78 input → 100 completed; 22 added). Verifies length-equals-growth
    // invariant on the diff source (parallel to v1.2's engine-source test).
    const before =
      "Commander\n1 Shelob, Child of Ungoliant\nDeck\n" +
      Array.from({ length: 6 }, (_, i) => `1 InputCard${i}`).join("\n") +
      "\n35 Forest\n36 Swamp";
    const after =
      "Commander\n1 Shelob, Child of Ungoliant\nDeck\n" +
      Array.from({ length: 6 }, (_, i) => `1 InputCard${i}`).join("\n") +
      "\n35 Forest\n36 Swamp\n" +
      Array.from({ length: 22 }, (_, i) => `1 FillCard${i}`).join("\n");
    const adds = computeDeckAdditions(before, after);
    expect(adds).toHaveLength(22);
    // All entries carry the diff-fallback reason.
    for (const e of adds) {
      expect(e.reasons_v1).toEqual(["added_during_completion"]);
    }
  });

  test("count-bump + new-card combined", () => {
    const adds = computeDeckAdditions(
      "1 Forest\n1 Sol Ring",
      "3 Forest\n1 Sol Ring\n1 Mountain",
    );
    // +2 Forest entries, +1 Mountain entry
    expect(adds).toHaveLength(3);
    expect(adds.map((a) => a.name)).toEqual(["Forest", "Forest", "Mountain"]);
  });

  test("permissive parsing: set + collector + count + duplicate-merge", () => {
    const before = "1 Sol Ring (M21) 132\n1 Forest *F*\n1 Forest";
    const after = "1 Sol Ring (M21) 132\n3 Forest\n1 Mountain [edition]";
    const adds = computeDeckAdditions(before, after);
    // before: Sol Ring=1, Forest=2; after: Sol Ring=1, Forest=3, Mountain=1
    // diff: +1 Forest, +1 Mountain
    expect(adds.map((a) => a.name)).toEqual(["Forest", "Mountain"]);
  });
});
