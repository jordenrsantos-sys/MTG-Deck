/**
 * shuffle — Phase 4.9 (Goldfish playtester).
 *
 * Pure Fisher-Yates shuffle with optional numeric seed for replayability.
 * Uses inline mulberry32 PRNG (no new dep) per autonomous_repair_log #7;
 * returns a fresh array — never mutates input. Determinism:
 * `shuffle(arr, seed)` is byte-identical across calls and platforms.
 *
 * HARD safety: reducer purity (HARD #11) — this helper has no Date.now,
 * no unseeded Math.random when seed is provided, no I/O. The only source
 * of nondeterminism is the optional unseeded path (no seed param) which
 * uses Math.random — callers that need determinism MUST pass a seed.
 */

/**
 * Make a deterministic 0..1 PRNG keyed on a 32-bit integer seed.
 * Mulberry32 — simple, fast, no dep; quality sufficient for shuffle use.
 */
export function mulberry32(seedInt: number): () => number {
  let state = seedInt >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let token = Math.imul(state ^ (state >>> 15), 1 | state);
    token ^= token + Math.imul(token ^ (token >>> 7), 61 | token);
    return ((token ^ (token >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Fold a string into a 32-bit integer seed via FNV-1a; deterministic.
 * Useful when callers want to derive a seed from a build_hash or deck name.
 */
export function stringToSeed(value: string): number {
  let hash = 2166136261 >>> 0;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash >>> 0;
}

/**
 * Returns a NEW array; never mutates `items`. When `seed` is undefined,
 * uses `Math.random()` (nondeterministic; callers needing determinism
 * must pass a seed).
 */
export function shuffle<T>(items: ReadonlyArray<T>, seed?: number): T[] {
  const out = items.slice();
  const rand = typeof seed === "number" ? mulberry32(seed) : Math.random;
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rand() * (i + 1));
    const tmp = out[i];
    out[i] = out[j];
    out[j] = tmp;
  }
  return out;
}
