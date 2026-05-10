/**
 * goldfishState — Phase 4.9 (Goldfish playtester).
 *
 * Pure state machine for single-player goldfish per UI_OVERHAUL_DESIGN
 * Decision 7 + OQ-5=(b). NO opponents (HARD #2). NO HTTP, NO localStorage,
 * NO Date.now / unseeded randomness inside reducer (HARD #11). All
 * randomness flows through a seeded mulberry32 PRNG via lib/shuffle.
 *
 * Action union is capped at 16 per autonomous_repair_log soft-safety #3:
 * START_GAME / MULLIGAN / KEEP_HAND / DRAW / PLAY_CARD / MOVE_TO_GRAVEYARD /
 * MOVE_TO_EXILE / RETURN_TO_HAND / TAP / UNTAP / UNTAP_ALL / ADJUST_LIFE /
 * ADVANCE_TURN / SCRY / SHUFFLE_LIBRARY / RESET_GAME (= 16 exactly).
 *
 * London mulligan rules per autonomous_repair_log soft-safety #6: each
 * MULLIGAN draws 7 fresh; KEEP_HAND with mulligansTaken = N puts N cards
 * on the bottom of the library (caller selects which N via the action
 * payload `bottomIndices`).
 */
import { shuffle, stringToSeed } from "./shuffle";

export type GoldfishCard = {
  instanceId: string;
  name: string;
  oracleId: string | null;
  typeLine: string | null;
  isCommander?: boolean;
};

export type BattlefieldCategory =
  | "creature"
  | "land"
  | "artifact"
  | "enchantment"
  | "planeswalker"
  | "battle"
  | "other";

export type BattlefieldEntry = {
  card: GoldfishCard;
  tapped: boolean;
  category: BattlefieldCategory;
};

export type GoldfishPhase = "pre_game" | "in_game";

export type PlayState = {
  phase: GoldfishPhase;
  turn: number;
  life: number;
  mulligansTaken: number;
  isFirstTurn: boolean;
  library: GoldfishCard[];
  hand: GoldfishCard[];
  battlefield: BattlefieldEntry[];
  graveyard: GoldfishCard[];
  exile: GoldfishCard[];
  commandZone: GoldfishCard[];
  /** mulberry32 state — single source of randomness in this reducer */
  rngSeed: number;
  /** monotonic counter for log/replay; not user-visible */
  actionCounter: number;
};

export type PlayAction =
  | {
      type: "START_GAME";
      deck: GoldfishCard[];
      commander?: GoldfishCard | null;
      startingLife?: number;
      seedToken?: string;
    }
  | { type: "MULLIGAN" }
  | { type: "KEEP_HAND"; bottomIndices: number[] }
  | { type: "DRAW"; count?: number }
  | { type: "PLAY_CARD"; from: "hand" | "graveyard" | "exile" | "library"; instanceId: string }
  | { type: "MOVE_TO_GRAVEYARD"; from: "hand" | "battlefield" | "exile" | "library"; instanceId: string }
  | { type: "MOVE_TO_EXILE"; from: "hand" | "battlefield" | "graveyard" | "library"; instanceId: string }
  | { type: "RETURN_TO_HAND"; from: "battlefield" | "graveyard" | "exile" | "library"; instanceId: string }
  | { type: "TAP"; instanceId: string }
  | { type: "UNTAP"; instanceId: string }
  | { type: "UNTAP_ALL" }
  | { type: "ADJUST_LIFE"; delta: number }
  | { type: "ADVANCE_TURN" }
  | { type: "SCRY"; count: number; ordering: ReadonlyArray<{ instanceId: string; placement: "top" | "bottom" }> }
  | { type: "SHUFFLE_LIBRARY" }
  | { type: "RESET_GAME" };

export const DEFAULT_STARTING_LIFE = 40;
export const OPENING_HAND_SIZE = 7;

export const INITIAL_STATE: PlayState = Object.freeze({
  phase: "pre_game",
  turn: 0,
  life: DEFAULT_STARTING_LIFE,
  mulligansTaken: 0,
  isFirstTurn: true,
  library: [],
  hand: [],
  battlefield: [],
  graveyard: [],
  exile: [],
  commandZone: [],
  rngSeed: 0,
  actionCounter: 0,
}) as PlayState;

/**
 * First-type heuristic per autonomous_repair_log soft-safety #5. Reads
 * type_line, splits on em-dash / minus / spaces, picks the first major
 * type token. Split / DFC / Saga edge cases queued as 4.9.1 follow-up.
 */
export function categorizeBattlefield(typeLine: string | null | undefined): BattlefieldCategory {
  if (!typeLine || typeof typeLine !== "string") return "other";
  const lower = typeLine.toLowerCase();
  if (lower.includes("creature")) return "creature";
  if (lower.includes("land")) return "land";
  if (lower.includes("planeswalker")) return "planeswalker";
  if (lower.includes("battle")) return "battle";
  if (lower.includes("artifact")) return "artifact";
  if (lower.includes("enchantment")) return "enchantment";
  return "other";
}

function _step(seed: number): number {
  return (seed + 0x6d2b79f5) >>> 0;
}

function _shuffleWithSeed<T>(items: T[], seed: number): { out: T[]; nextSeed: number } {
  return { out: shuffle(items, seed), nextSeed: _step(seed) };
}

function _drawFromLibrary(state: PlayState, count: number): PlayState {
  const drawCount = Math.max(0, Math.min(count, state.library.length));
  if (drawCount === 0) return state;
  const drawn = state.library.slice(0, drawCount);
  return {
    ...state,
    library: state.library.slice(drawCount),
    hand: [...state.hand, ...drawn],
  };
}

function _findInZone(
  state: PlayState,
  zone: "hand" | "graveyard" | "exile" | "library",
  instanceId: string,
): { card: GoldfishCard | null; rest: GoldfishCard[] } {
  const arr = state[zone];
  const idx = arr.findIndex((c) => c.instanceId === instanceId);
  if (idx === -1) return { card: null, rest: arr };
  const card = arr[idx];
  const rest = arr.slice(0, idx).concat(arr.slice(idx + 1));
  return { card, rest };
}

function _findOnBattlefield(
  state: PlayState,
  instanceId: string,
): { entry: BattlefieldEntry | null; rest: BattlefieldEntry[] } {
  const idx = state.battlefield.findIndex((e) => e.card.instanceId === instanceId);
  if (idx === -1) return { entry: null, rest: state.battlefield };
  const entry = state.battlefield[idx];
  const rest = state.battlefield.slice(0, idx).concat(state.battlefield.slice(idx + 1));
  return { entry, rest };
}

export function reducer(state: PlayState, action: PlayAction): PlayState {
  const next = { ...state, actionCounter: state.actionCounter + 1 };

  switch (action.type) {
    case "START_GAME": {
      const seedToken = action.seedToken ?? `goldfish:${action.deck.length}:${action.commander?.name ?? ""}`;
      const seed = stringToSeed(seedToken);
      const shuffled = shuffle(action.deck, seed);
      const drawCount = Math.min(OPENING_HAND_SIZE, shuffled.length);
      return {
        ...next,
        phase: "in_game",
        turn: 1,
        life: action.startingLife ?? DEFAULT_STARTING_LIFE,
        mulligansTaken: 0,
        isFirstTurn: true,
        library: shuffled.slice(drawCount),
        hand: shuffled.slice(0, drawCount),
        battlefield: [],
        graveyard: [],
        exile: [],
        commandZone: action.commander ? [action.commander] : [],
        rngSeed: _step(seed),
      };
    }

    case "MULLIGAN": {
      const all = [...state.hand, ...state.library];
      const { out, nextSeed } = _shuffleWithSeed(all, state.rngSeed);
      const drawCount = Math.min(OPENING_HAND_SIZE, out.length);
      return {
        ...next,
        mulligansTaken: state.mulligansTaken + 1,
        hand: out.slice(0, drawCount),
        library: out.slice(drawCount),
        rngSeed: nextSeed,
      };
    }

    case "KEEP_HAND": {
      const n = state.mulligansTaken;
      if (n <= 0 || action.bottomIndices.length === 0) return next;
      const sorted = [...action.bottomIndices].sort((a, b) => b - a);
      const newHand = [...state.hand];
      const toBottom: GoldfishCard[] = [];
      for (const idx of sorted) {
        if (idx >= 0 && idx < newHand.length) {
          toBottom.push(newHand[idx]);
          newHand.splice(idx, 1);
        }
      }
      return {
        ...next,
        hand: newHand,
        library: [...state.library, ...toBottom],
      };
    }

    case "DRAW": {
      return _drawFromLibrary(next, action.count ?? 1);
    }

    case "PLAY_CARD": {
      if (action.from === "hand" || action.from === "graveyard" || action.from === "exile" || action.from === "library") {
        const { card, rest } = _findInZone(state, action.from, action.instanceId);
        if (!card) return next;
        const entry: BattlefieldEntry = {
          card,
          tapped: false,
          category: categorizeBattlefield(card.typeLine),
        };
        return {
          ...next,
          [action.from]: rest,
          battlefield: [...state.battlefield, entry],
        } as PlayState;
      }
      return next;
    }

    case "MOVE_TO_GRAVEYARD": {
      if (action.from === "battlefield") {
        const { entry, rest } = _findOnBattlefield(state, action.instanceId);
        if (!entry) return next;
        return { ...next, battlefield: rest, graveyard: [...state.graveyard, entry.card] };
      }
      const { card, rest } = _findInZone(state, action.from, action.instanceId);
      if (!card) return next;
      return { ...next, [action.from]: rest, graveyard: [...state.graveyard, card] } as PlayState;
    }

    case "MOVE_TO_EXILE": {
      if (action.from === "battlefield") {
        const { entry, rest } = _findOnBattlefield(state, action.instanceId);
        if (!entry) return next;
        return { ...next, battlefield: rest, exile: [...state.exile, entry.card] };
      }
      const { card, rest } = _findInZone(state, action.from, action.instanceId);
      if (!card) return next;
      return { ...next, [action.from]: rest, exile: [...state.exile, card] } as PlayState;
    }

    case "RETURN_TO_HAND": {
      if (action.from === "battlefield") {
        const { entry, rest } = _findOnBattlefield(state, action.instanceId);
        if (!entry) return next;
        return { ...next, battlefield: rest, hand: [...state.hand, entry.card] };
      }
      const { card, rest } = _findInZone(state, action.from, action.instanceId);
      if (!card) return next;
      return { ...next, [action.from]: rest, hand: [...state.hand, card] } as PlayState;
    }

    case "TAP": {
      const idx = state.battlefield.findIndex((e) => e.card.instanceId === action.instanceId);
      if (idx === -1) return next;
      const battlefield = state.battlefield.slice();
      battlefield[idx] = { ...battlefield[idx], tapped: true };
      return { ...next, battlefield };
    }

    case "UNTAP": {
      const idx = state.battlefield.findIndex((e) => e.card.instanceId === action.instanceId);
      if (idx === -1) return next;
      const battlefield = state.battlefield.slice();
      battlefield[idx] = { ...battlefield[idx], tapped: false };
      return { ...next, battlefield };
    }

    case "UNTAP_ALL": {
      return {
        ...next,
        battlefield: state.battlefield.map((e) => (e.tapped ? { ...e, tapped: false } : e)),
      };
    }

    case "ADJUST_LIFE": {
      return { ...next, life: state.life + action.delta };
    }

    case "ADVANCE_TURN": {
      // ADVANCE_TURN steps OUT of current turn INTO the next; always draw 1.
      // (The "no draw on opening" case is handled by START_GAME giving you
      // the opening hand without an extra draw step.)
      const untapped = state.battlefield.map((e) => (e.tapped ? { ...e, tapped: false } : e));
      const drawn = _drawFromLibrary({ ...next, battlefield: untapped }, 1);
      return {
        ...drawn,
        turn: state.turn + 1,
        isFirstTurn: false,
      };
    }

    case "SCRY": {
      const k = Math.max(0, Math.min(action.count, state.library.length));
      if (k === 0) return next;
      const looked = state.library.slice(0, k);
      const remainder = state.library.slice(k);
      const tops: GoldfishCard[] = [];
      const bottoms: GoldfishCard[] = [];
      for (const order of action.ordering) {
        const card = looked.find((c) => c.instanceId === order.instanceId);
        if (!card) continue;
        if (order.placement === "top") tops.push(card);
        else bottoms.push(card);
      }
      return {
        ...next,
        library: [...tops, ...remainder, ...bottoms],
      };
    }

    case "SHUFFLE_LIBRARY": {
      const { out, nextSeed } = _shuffleWithSeed(state.library, state.rngSeed);
      return { ...next, library: out, rngSeed: nextSeed };
    }

    case "RESET_GAME": {
      return { ...INITIAL_STATE, actionCounter: state.actionCounter + 1 };
    }

    default:
      return state;
  }
}
