/** Vitest tests for lib/shuffle (Phase 4.9 Stage 1). */
import { describe, expect, test } from "vitest";
import { mulberry32, shuffle, stringToSeed } from "../shuffle";

describe("shuffle", () => {
  test("does not mutate input array", () => {
    const input = [1, 2, 3, 4, 5];
    const snapshot = input.slice();
    shuffle(input, 42);
    expect(input).toEqual(snapshot);
  });

  test("preserves length and contents (permutation)", () => {
    const input = [1, 2, 3, 4, 5, 6, 7];
    const out = shuffle(input, 99);
    expect(out.length).toBe(input.length);
    expect(out.slice().sort()).toEqual(input.slice().sort());
  });

  test("deterministic across calls with same seed", () => {
    const input = ["a", "b", "c", "d", "e"];
    const a = shuffle(input, 12345);
    const b = shuffle(input, 12345);
    expect(a).toEqual(b);
  });

  test("different seeds produce different orderings (probabilistic)", () => {
    const input = Array.from({ length: 20 }, (_, i) => i);
    const a = shuffle(input, 1);
    const b = shuffle(input, 2);
    expect(a).not.toEqual(b);
  });

  test("empty array returns empty array", () => {
    expect(shuffle([], 7)).toEqual([]);
  });

  test("single-element array returns one-element array", () => {
    expect(shuffle(["only"], 7)).toEqual(["only"]);
  });
});

describe("mulberry32", () => {
  test("deterministic sequence given same seed", () => {
    const a = mulberry32(42);
    const b = mulberry32(42);
    for (let i = 0; i < 5; i += 1) {
      expect(a()).toBe(b());
    }
  });

  test("output in [0, 1) range", () => {
    const r = mulberry32(99);
    for (let i = 0; i < 10; i += 1) {
      const v = r();
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThan(1);
    }
  });
});

describe("stringToSeed", () => {
  test("deterministic across calls", () => {
    expect(stringToSeed("hello")).toBe(stringToSeed("hello"));
  });

  test("different inputs yield different seeds (probabilistic)", () => {
    expect(stringToSeed("hello")).not.toBe(stringToSeed("world"));
  });

  test("empty string yields a stable value", () => {
    const v = stringToSeed("");
    expect(typeof v).toBe("number");
    expect(v).toBe(stringToSeed(""));
  });
});
