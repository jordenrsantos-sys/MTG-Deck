/**
 * devLog — Phase 4 BUNDLE Stage 4.10 (polish).
 *
 * DEV-gated console wrapper. In production (`import.meta.env.PROD`) all
 * calls are no-ops; in dev they pass through to the real console. Per
 * autonomous_repair_log soft-safety: console output should NEVER appear
 * in shipped builds (no PII leakage, no perf cost, no devtool noise).
 */
const IS_DEV =
  typeof import.meta !== "undefined" &&
  typeof import.meta.env !== "undefined" &&
  import.meta.env.DEV === true;

export function devLog(...args: unknown[]): void {
  if (!IS_DEV) return;
  // eslint-disable-next-line no-console
  console.log(...args);
}

export function devWarn(...args: unknown[]): void {
  if (!IS_DEV) return;
  // eslint-disable-next-line no-console
  console.warn(...args);
}

export function devError(...args: unknown[]): void {
  if (!IS_DEV) return;
  // eslint-disable-next-line no-console
  console.error(...args);
}

export const __IS_DEV_FOR_TESTS = IS_DEV;
