/**
 * Vitest tests for playtest reducer + state-machine integration patterns
 * that GoldfishView relies on (Phase 4.9 Stage 4). These exercise the
 * end-to-end action sequences a player would actually drive without
 * needing React Testing Library.
 */
import { describe, expect, test } from "vitest";
import {
  reducer,
  INITIAL_STATE,
  OPENING_HAND_SIZE,
  type GoldfishCard,
  type PlayState,
} from "../../../lib/goldfishState";

function mkLand(i: number): GoldfishCard {
  return { instanceId: `land-${i}`, name: "Forest", oracleId: null, typeLine: "Basic Land — Forest" };
}
function mkCreature(i: number): GoldfishCard {
  return { instanceId: `cr-${i}`, name: "Goblin", oracleId: null, typeLine: "Creature — Goblin" };
}

function mkDeck(): GoldfishCard[] {
  const deck: GoldfishCard[] = [];
  for (let i = 0; i < 30; i += 1) deck.push(mkLand(i));
  for (let i = 0; i < 30; i += 1) deck.push(mkCreature(i));
  return deck;
}

describe("end-to-end goldfish flow", () => {
  test("start → mulligan → keep with bottom 1 → advance turn → play card", () => {
    const deck = mkDeck();
    let s: PlayState = reducer(INITIAL_STATE, { type: "START_GAME", deck, seedToken: "e2e" });
    expect(s.hand).toHaveLength(OPENING_HAND_SIZE);
    expect(s.phase).toBe("in_game");

    s = reducer(s, { type: "MULLIGAN" });
    expect(s.mulligansTaken).toBe(1);
    expect(s.hand).toHaveLength(OPENING_HAND_SIZE);

    const targetId = s.hand[0].instanceId;
    s = reducer(s, { type: "KEEP_HAND", bottomIndices: [0] });
    expect(s.hand).toHaveLength(OPENING_HAND_SIZE - 1);
    expect(s.library[s.library.length - 1].instanceId).toBe(targetId);

    const beforeHandCount = s.hand.length;
    const beforeLibCount = s.library.length;
    s = reducer(s, { type: "ADVANCE_TURN" });
    expect(s.turn).toBe(2);
    expect(s.hand.length).toBe(beforeHandCount + 1);
    expect(s.library.length).toBe(beforeLibCount - 1);

    const playable = s.hand[0];
    s = reducer(s, { type: "PLAY_CARD", from: "hand", instanceId: playable.instanceId });
    expect(s.battlefield).toHaveLength(1);
  });

  test("RESET_GAME after a long sequence returns to pre_game state", () => {
    const deck = mkDeck();
    let s: PlayState = reducer(INITIAL_STATE, { type: "START_GAME", deck, seedToken: "reset" });
    s = reducer(s, { type: "DRAW", count: 3 });
    s = reducer(s, { type: "ADVANCE_TURN" });
    s = reducer(s, { type: "ADJUST_LIFE", delta: -10 });
    expect(s.life).toBe(30);
    s = reducer(s, { type: "RESET_GAME" });
    expect(s.phase).toBe("pre_game");
    expect(s.life).toBe(40);
    expect(s.turn).toBe(0);
    expect(s.hand).toHaveLength(0);
  });

  test("hotkey-style sequence (D/U/E/S/R action types) is byte-deterministic given seed", () => {
    const deck = mkDeck();
    function run(): PlayState {
      let s: PlayState = INITIAL_STATE;
      s = reducer(s, { type: "START_GAME", deck, seedToken: "hot" });
      s = reducer(s, { type: "DRAW", count: 1 });
      s = reducer(s, { type: "UNTAP_ALL" });
      s = reducer(s, { type: "ADVANCE_TURN" });
      s = reducer(s, { type: "SHUFFLE_LIBRARY" });
      return s;
    }
    const a = run();
    const b = run();
    expect(a.hand.map((c) => c.instanceId)).toEqual(b.hand.map((c) => c.instanceId));
    expect(a.library.map((c) => c.instanceId)).toEqual(b.library.map((c) => c.instanceId));
    expect(a.turn).toBe(b.turn);
  });
});
