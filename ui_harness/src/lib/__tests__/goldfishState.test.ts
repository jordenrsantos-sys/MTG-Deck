/** Vitest tests for lib/goldfishState (Phase 4.9 Stage 1). */
import { describe, expect, test } from "vitest";
import {
  reducer,
  INITIAL_STATE,
  DEFAULT_STARTING_LIFE,
  OPENING_HAND_SIZE,
  categorizeBattlefield,
  type GoldfishCard,
  type PlayState,
} from "../goldfishState";

function mkCard(id: string, name: string, typeLine: string | null = null): GoldfishCard {
  return { instanceId: id, name, oracleId: null, typeLine };
}

function mkDeck(n: number): GoldfishCard[] {
  return Array.from({ length: n }, (_, i) => mkCard(`c${i}`, `Card ${i}`));
}

describe("categorizeBattlefield", () => {
  test("creature wins over enchantment", () => {
    expect(categorizeBattlefield("Enchantment Creature — God")).toBe("creature");
  });
  test("land detected", () => {
    expect(categorizeBattlefield("Land")).toBe("land");
    expect(categorizeBattlefield("Basic Land — Forest")).toBe("land");
  });
  test("artifact / enchantment / planeswalker / battle", () => {
    expect(categorizeBattlefield("Artifact")).toBe("artifact");
    expect(categorizeBattlefield("Enchantment")).toBe("enchantment");
    expect(categorizeBattlefield("Legendary Planeswalker — Liliana")).toBe("planeswalker");
    expect(categorizeBattlefield("Battle — Siege")).toBe("battle");
  });
  test("null / empty / unknown → other", () => {
    expect(categorizeBattlefield(null)).toBe("other");
    expect(categorizeBattlefield("")).toBe("other");
    expect(categorizeBattlefield("Tribal — Wizard")).toBe("other");
  });
});

describe("reducer — START_GAME", () => {
  test("draws OPENING_HAND_SIZE and sets phase=in_game, turn=1, life=40", () => {
    const deck = mkDeck(60);
    const s = reducer(INITIAL_STATE, { type: "START_GAME", deck, seedToken: "test" });
    expect(s.phase).toBe("in_game");
    expect(s.turn).toBe(1);
    expect(s.life).toBe(DEFAULT_STARTING_LIFE);
    expect(s.hand).toHaveLength(OPENING_HAND_SIZE);
    expect(s.library).toHaveLength(60 - OPENING_HAND_SIZE);
    expect(s.mulligansTaken).toBe(0);
    expect(s.isFirstTurn).toBe(true);
  });

  test("commander placed in command zone when provided", () => {
    const deck = mkDeck(99);
    const cmdr = mkCard("cmdr", "Krenko, Mob Boss");
    const s = reducer(INITIAL_STATE, { type: "START_GAME", deck, commander: cmdr });
    expect(s.commandZone).toEqual([cmdr]);
  });

  test("custom startingLife respected", () => {
    const s = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(60), startingLife: 20 });
    expect(s.life).toBe(20);
  });

  test("deterministic given same seedToken", () => {
    const deck = mkDeck(60);
    const a = reducer(INITIAL_STATE, { type: "START_GAME", deck, seedToken: "abc" });
    const b = reducer(INITIAL_STATE, { type: "START_GAME", deck, seedToken: "abc" });
    expect(a.hand.map((c) => c.instanceId)).toEqual(b.hand.map((c) => c.instanceId));
  });
});

describe("reducer — MULLIGAN / KEEP_HAND (London)", () => {
  function start(): PlayState {
    return reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(60), seedToken: "mull-test" });
  }
  test("MULLIGAN reshuffles hand+library, increments mulligansTaken, redraws 7", () => {
    const s = reducer(start(), { type: "MULLIGAN" });
    expect(s.mulligansTaken).toBe(1);
    expect(s.hand).toHaveLength(7);
    expect(s.hand.length + s.library.length).toBe(60);
  });

  test("KEEP_HAND with bottomIndices puts N cards on bottom (London)", () => {
    let s = start();
    s = reducer(s, { type: "MULLIGAN" });
    expect(s.mulligansTaken).toBe(1);
    const beforeLibBottom = s.library[s.library.length - 1];
    const sentDown = s.hand[0];
    s = reducer(s, { type: "KEEP_HAND", bottomIndices: [0] });
    expect(s.hand).toHaveLength(6);
    expect(s.library[s.library.length - 1]).toEqual(sentDown);
    expect(beforeLibBottom).not.toEqual(sentDown);
  });

  test("KEEP_HAND noop when mulligansTaken=0", () => {
    const s0 = start();
    const s1 = reducer(s0, { type: "KEEP_HAND", bottomIndices: [0, 1] });
    expect(s1.hand).toEqual(s0.hand);
    expect(s1.library).toEqual(s0.library);
  });
});

describe("reducer — DRAW / PLAY_CARD / zone moves", () => {
  function start(): PlayState {
    return reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(40), seedToken: "moves" });
  }

  test("DRAW pulls from library to hand", () => {
    const s0 = start();
    const s1 = reducer(s0, { type: "DRAW", count: 3 });
    expect(s1.hand).toHaveLength(s0.hand.length + 3);
    expect(s1.library).toHaveLength(s0.library.length - 3);
  });

  test("DRAW caps at library size; never goes negative", () => {
    const tiny = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(8), seedToken: "tiny" });
    const post = reducer(tiny, { type: "DRAW", count: 100 });
    expect(post.library).toHaveLength(0);
    expect(post.hand.length).toBe(8);
  });

  test("PLAY_CARD moves card from hand to battlefield with category", () => {
    let s = start();
    s = { ...s, hand: [{ instanceId: "id1", name: "Forest", oracleId: null, typeLine: "Basic Land — Forest" }, ...s.hand] };
    const post = reducer(s, { type: "PLAY_CARD", from: "hand", instanceId: "id1" });
    expect(post.battlefield).toHaveLength(1);
    expect(post.battlefield[0].category).toBe("land");
    expect(post.battlefield[0].tapped).toBe(false);
    expect(post.hand.find((c) => c.instanceId === "id1")).toBeUndefined();
  });

  test("MOVE_TO_GRAVEYARD from battlefield", () => {
    let s = start();
    s = { ...s, hand: [{ instanceId: "x", name: "X", oracleId: null, typeLine: "Creature — Goblin" }, ...s.hand] };
    s = reducer(s, { type: "PLAY_CARD", from: "hand", instanceId: "x" });
    const post = reducer(s, { type: "MOVE_TO_GRAVEYARD", from: "battlefield", instanceId: "x" });
    expect(post.battlefield).toHaveLength(0);
    expect(post.graveyard.find((c) => c.instanceId === "x")).toBeDefined();
  });

  test("MOVE_TO_EXILE from hand", () => {
    let s = start();
    s = { ...s, hand: [{ instanceId: "y", name: "Y", oracleId: null, typeLine: null }, ...s.hand] };
    const post = reducer(s, { type: "MOVE_TO_EXILE", from: "hand", instanceId: "y" });
    expect(post.exile.find((c) => c.instanceId === "y")).toBeDefined();
    expect(post.hand.find((c) => c.instanceId === "y")).toBeUndefined();
  });

  test("RETURN_TO_HAND from graveyard", () => {
    let s = start();
    s = { ...s, hand: [{ instanceId: "z", name: "Z", oracleId: null, typeLine: null }, ...s.hand] };
    s = reducer(s, { type: "MOVE_TO_GRAVEYARD", from: "hand", instanceId: "z" });
    const post = reducer(s, { type: "RETURN_TO_HAND", from: "graveyard", instanceId: "z" });
    expect(post.hand.find((c) => c.instanceId === "z")).toBeDefined();
    expect(post.graveyard.find((c) => c.instanceId === "z")).toBeUndefined();
  });

  test("missing instanceId is a no-op", () => {
    const s0 = start();
    const s1 = reducer(s0, { type: "TAP", instanceId: "does-not-exist" });
    expect(s1.battlefield).toEqual(s0.battlefield);
  });
});

describe("reducer — TAP / UNTAP / UNTAP_ALL / ADJUST_LIFE / ADVANCE_TURN", () => {
  function withTwoOnBattlefield(): PlayState {
    let s = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(20), seedToken: "tap" });
    s = { ...s, hand: [
      { instanceId: "a", name: "A", oracleId: null, typeLine: "Creature — Cat" },
      { instanceId: "b", name: "B", oracleId: null, typeLine: "Land" },
      ...s.hand,
    ] };
    s = reducer(s, { type: "PLAY_CARD", from: "hand", instanceId: "a" });
    s = reducer(s, { type: "PLAY_CARD", from: "hand", instanceId: "b" });
    return s;
  }

  test("TAP and UNTAP target single instance", () => {
    let s = withTwoOnBattlefield();
    s = reducer(s, { type: "TAP", instanceId: "a" });
    expect(s.battlefield.find((e) => e.card.instanceId === "a")?.tapped).toBe(true);
    expect(s.battlefield.find((e) => e.card.instanceId === "b")?.tapped).toBe(false);
    s = reducer(s, { type: "UNTAP", instanceId: "a" });
    expect(s.battlefield.find((e) => e.card.instanceId === "a")?.tapped).toBe(false);
  });

  test("UNTAP_ALL untaps every entry", () => {
    let s = withTwoOnBattlefield();
    s = reducer(s, { type: "TAP", instanceId: "a" });
    s = reducer(s, { type: "TAP", instanceId: "b" });
    const post = reducer(s, { type: "UNTAP_ALL" });
    expect(post.battlefield.every((e) => !e.tapped)).toBe(true);
  });

  test("ADJUST_LIFE adds delta", () => {
    const s0 = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(40), seedToken: "life" });
    expect(reducer(s0, { type: "ADJUST_LIFE", delta: -7 }).life).toBe(33);
    expect(reducer(s0, { type: "ADJUST_LIFE", delta: 5 }).life).toBe(45);
  });

  test("ADVANCE_TURN: first→second turn untaps + draws 1; first turn no-draw", () => {
    let s = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(40), seedToken: "turn" });
    s = { ...s, hand: [{ instanceId: "p", name: "P", oracleId: null, typeLine: "Creature — Goblin" }, ...s.hand] };
    s = reducer(s, { type: "PLAY_CARD", from: "hand", instanceId: "p" });
    s = reducer(s, { type: "TAP", instanceId: "p" });
    expect(s.turn).toBe(1);
    expect(s.battlefield.find((e) => e.card.instanceId === "p")?.tapped).toBe(true);
    const beforeHand = s.hand.length;
    const beforeLib = s.library.length;
    s = reducer(s, { type: "ADVANCE_TURN" });
    expect(s.turn).toBe(2);
    expect(s.battlefield.find((e) => e.card.instanceId === "p")?.tapped).toBe(false);
    expect(s.isFirstTurn).toBe(false);
    expect(s.hand.length).toBe(beforeHand + 1);
    expect(s.library.length).toBe(beforeLib - 1);
  });
});

describe("reducer — SCRY / SHUFFLE_LIBRARY / RESET_GAME", () => {
  test("SCRY reorders the top N per ordering", () => {
    let s = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(40), seedToken: "scry" });
    const top3 = s.library.slice(0, 3);
    s = reducer(s, {
      type: "SCRY",
      count: 3,
      ordering: [
        { instanceId: top3[0].instanceId, placement: "bottom" },
        { instanceId: top3[1].instanceId, placement: "top" },
        { instanceId: top3[2].instanceId, placement: "top" },
      ],
    });
    expect(s.library[0].instanceId).toBe(top3[1].instanceId);
    expect(s.library[1].instanceId).toBe(top3[2].instanceId);
    expect(s.library[s.library.length - 1].instanceId).toBe(top3[0].instanceId);
  });

  test("SHUFFLE_LIBRARY preserves library size + contents (permutation)", () => {
    const s0 = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(40), seedToken: "shuf" });
    const ids = s0.library.map((c) => c.instanceId).sort();
    const post = reducer(s0, { type: "SHUFFLE_LIBRARY" });
    expect(post.library).toHaveLength(s0.library.length);
    expect(post.library.map((c) => c.instanceId).sort()).toEqual(ids);
  });

  test("RESET_GAME returns to INITIAL_STATE shape (preserving actionCounter monotonic)", () => {
    let s = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(20), seedToken: "rst" });
    s = reducer(s, { type: "DRAW", count: 2 });
    const post = reducer(s, { type: "RESET_GAME" });
    expect(post.phase).toBe("pre_game");
    expect(post.turn).toBe(0);
    expect(post.life).toBe(DEFAULT_STARTING_LIFE);
    expect(post.hand).toEqual([]);
    expect(post.library).toEqual([]);
    expect(post.battlefield).toEqual([]);
    expect(post.actionCounter).toBeGreaterThan(s.actionCounter);
  });
});

describe("reducer — purity / determinism", () => {
  test("does not mutate prior state", () => {
    const s0 = reducer(INITIAL_STATE, { type: "START_GAME", deck: mkDeck(40), seedToken: "pure" });
    const handSnapshot = s0.hand.slice();
    const librarySnapshot = s0.library.slice();
    reducer(s0, { type: "DRAW", count: 5 });
    expect(s0.hand).toEqual(handSnapshot);
    expect(s0.library).toEqual(librarySnapshot);
  });

  test("identical action sequences produce identical state given seed", () => {
    const deck = mkDeck(40);
    const seq = (): PlayState => {
      let s: PlayState = INITIAL_STATE;
      s = reducer(s, { type: "START_GAME", deck, seedToken: "det" });
      s = reducer(s, { type: "DRAW", count: 2 });
      s = reducer(s, { type: "ADVANCE_TURN" });
      s = reducer(s, { type: "ADJUST_LIFE", delta: -3 });
      return s;
    };
    const a = seq();
    const b = seq();
    expect(a.hand.map((c) => c.instanceId)).toEqual(b.hand.map((c) => c.instanceId));
    expect(a.library.map((c) => c.instanceId)).toEqual(b.library.map((c) => c.instanceId));
    expect(a.life).toBe(b.life);
    expect(a.turn).toBe(b.turn);
  });
});
