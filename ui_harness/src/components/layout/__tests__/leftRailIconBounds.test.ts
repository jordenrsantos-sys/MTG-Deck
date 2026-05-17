/**
 * Vitest tests for v1.6.3 Stage 1 — drawer icon visual alignment.
 *
 * The orchestrator's v1.6.2 browser walk caught uneven icon bounding
 * boxes: HOME 18×17, PLAY 14×16, DIAG 14×18 — all below the 18-22 px
 * tolerance the spec defines. v1.6.3 redraws each outlier to fill at
 * least 18×18 within a 24×24 viewBox + 2 px safety margin (target:
 * 20×20 effective).
 *
 * This test parses the geometry attributes of each icon's primitives
 * (rect / circle / polygon / polyline / line / path-with-M+L+H+V+
 * lowercase-relatives) directly from LeftRail.tsx and asserts the
 * combined bounding box falls within 18-22 px in both dimensions.
 *
 * Path parser supports: M, L, H, V (absolute) + m, l, h, v (relative).
 * ICON_SETTINGS uses these exclusively. Arc commands (A/a, C/c, S/s,
 * Q/q, T/t) are NOT parsed — none of the icons use them post-Stage-1.
 *
 * Per AUTOMATION_RULES halt-and-ask + autonomous_repair_log soft-safety:
 * no @testing-library/react install — source-level geometry parsing.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const LEFTRAIL_SRC = readFileSync(
  resolve(__dirname, "../LeftRail.tsx"),
  "utf-8",
);

type Bounds = { minX: number; minY: number; maxX: number; maxY: number };

function _newBounds(): Bounds {
  return { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
}
function _includePoint(b: Bounds, x: number, y: number): void {
  if (x < b.minX) b.minX = x;
  if (y < b.minY) b.minY = y;
  if (x > b.maxX) b.maxX = x;
  if (y > b.maxY) b.maxY = y;
}
function _width(b: Bounds): number {
  return b.maxX - b.minX;
}
function _height(b: Bounds): number {
  return b.maxY - b.minY;
}

/** Parse `points="x,y x,y ..."` or `points="x y x y ..."` into pairs. */
function _parsePoints(s: string): Array<[number, number]> {
  const tokens = s.trim().split(/[\s,]+/).filter((t) => t !== "");
  const out: Array<[number, number]> = [];
  for (let i = 0; i + 1 < tokens.length; i += 2) {
    out.push([parseFloat(tokens[i]), parseFloat(tokens[i + 1])]);
  }
  return out;
}

/** Parse an SVG path `d` attribute. Supports M/L/H/V (absolute) +
 *  m/l/h/v (relative). Returns the bounds of all (x,y) points visited
 *  by both `move` and `lineTo` commands. */
function _parsePathBounds(d: string): Bounds {
  const b = _newBounds();
  // Split into command tokens. Each command is a letter + a sequence of
  // numbers (separated by space or comma; minus signs may double as
  // separators when no whitespace).
  const re = /([MmLlHhVvZz])([^MmLlHhVvZz]*)/g;
  let cur: [number, number] = [0, 0];
  let m: RegExpExecArray | null;
  while ((m = re.exec(d)) !== null) {
    const cmd = m[1];
    const argsStr = m[2].trim();
    const nums = argsStr === ""
      ? []
      : argsStr.split(/[\s,]+|(?=-)/).filter((t) => t !== "" && t !== "-").map(parseFloat);
    if (cmd === "M" || cmd === "L") {
      for (let i = 0; i + 1 < nums.length; i += 2) {
        cur = [nums[i], nums[i + 1]];
        _includePoint(b, cur[0], cur[1]);
      }
    } else if (cmd === "m" || cmd === "l") {
      for (let i = 0; i + 1 < nums.length; i += 2) {
        cur = [cur[0] + nums[i], cur[1] + nums[i + 1]];
        _includePoint(b, cur[0], cur[1]);
      }
    } else if (cmd === "H") {
      for (const v of nums) {
        cur = [v, cur[1]];
        _includePoint(b, cur[0], cur[1]);
      }
    } else if (cmd === "h") {
      for (const v of nums) {
        cur = [cur[0] + v, cur[1]];
        _includePoint(b, cur[0], cur[1]);
      }
    } else if (cmd === "V") {
      for (const v of nums) {
        cur = [cur[0], v];
        _includePoint(b, cur[0], cur[1]);
      }
    } else if (cmd === "v") {
      for (const v of nums) {
        cur = [cur[0], cur[1] + v];
        _includePoint(b, cur[0], cur[1]);
      }
    }
    // Z/z: closepath — no new point.
  }
  return b;
}

/** Compute the bounds of all SVG primitives inside an `_icon(...)`
 *  block. The block is a JSX fragment containing one or more of:
 *  <rect x= y= width= height= />, <circle cx= cy= r= />, <polygon points= />,
 *  <polyline points= />, <line x1= y1= x2= y2= />, <path d= />.
 *
 *  Each primitive's bounds are unioned into the icon's overall bounds. */
function _computeIconBounds(blockSrc: string): Bounds {
  const b = _newBounds();

  // rect
  for (const m of blockSrc.matchAll(
    /<rect[^>]+x="([\d.-]+)"[^>]+y="([\d.-]+)"[^>]+width="([\d.-]+)"[^>]+height="([\d.-]+)"/g,
  )) {
    const x = parseFloat(m[1]);
    const y = parseFloat(m[2]);
    const w = parseFloat(m[3]);
    const h = parseFloat(m[4]);
    _includePoint(b, x, y);
    _includePoint(b, x + w, y + h);
  }
  // circle
  for (const m of blockSrc.matchAll(
    /<circle[^>]+cx="([\d.-]+)"[^>]+cy="([\d.-]+)"[^>]+r="([\d.-]+)"/g,
  )) {
    const cx = parseFloat(m[1]);
    const cy = parseFloat(m[2]);
    const r = parseFloat(m[3]);
    _includePoint(b, cx - r, cy - r);
    _includePoint(b, cx + r, cy + r);
  }
  // polygon / polyline
  for (const m of blockSrc.matchAll(/<(?:polygon|polyline)[^>]+points="([^"]+)"/g)) {
    for (const [px, py] of _parsePoints(m[1])) _includePoint(b, px, py);
  }
  // line
  for (const m of blockSrc.matchAll(
    /<line[^>]+x1="([\d.-]+)"[^>]+y1="([\d.-]+)"[^>]+x2="([\d.-]+)"[^>]+y2="([\d.-]+)"/g,
  )) {
    _includePoint(b, parseFloat(m[1]), parseFloat(m[2]));
    _includePoint(b, parseFloat(m[3]), parseFloat(m[4]));
  }
  // path
  for (const m of blockSrc.matchAll(/<path[^>]+d="([^"]+)"/g)) {
    const pb = _parsePathBounds(m[1]);
    if (pb.minX !== Infinity) {
      _includePoint(b, pb.minX, pb.minY);
      _includePoint(b, pb.maxX, pb.maxY);
    }
  }

  return b;
}

/** Extract the JSX block argument passed to `_icon(...)` for a given
 *  named icon constant. Returns the source-text between the outer `(`
 *  and matching `)`. */
function _extractIconBlock(iconName: string): string {
  // Pattern: `const ICON_NAME = _icon(\n  <>\n    ...\n  </>,\n);`
  // OR self-closing: `const ICON_NAME = _icon(<polygon ... />);`
  const re = new RegExp(`const\\s+${iconName}\\s*=\\s*_icon\\(([\\s\\S]*?)\\);`, "m");
  const m = LEFTRAIL_SRC.match(re);
  if (!m) throw new Error(`Could not extract icon block for ${iconName}`);
  return m[1];
}

const ICON_NAMES = [
  "ICON_HOME",
  "ICON_DECKS",
  "ICON_PLAY",
  "ICON_SETTINGS",
  "ICON_RUNS",
  "ICON_DIAG",
];

const TOLERANCE_MIN_PX = 18;
const TOLERANCE_MAX_PX = 22;

describe("v1.6.3 Stage 1 — drawer icon bounding-box uniformity", () => {
  for (const iconName of ICON_NAMES) {
    test(`${iconName} bounds fit within ${TOLERANCE_MIN_PX}-${TOLERANCE_MAX_PX} px in both dimensions`, () => {
      const block = _extractIconBlock(iconName);
      const bounds = _computeIconBounds(block);
      expect(bounds.minX).not.toBe(Infinity); // sanity: parser found something
      const w = _width(bounds);
      const h = _height(bounds);
      expect(w).toBeGreaterThanOrEqual(TOLERANCE_MIN_PX);
      expect(w).toBeLessThanOrEqual(TOLERANCE_MAX_PX);
      expect(h).toBeGreaterThanOrEqual(TOLERANCE_MIN_PX);
      expect(h).toBeLessThanOrEqual(TOLERANCE_MAX_PX);
    });
  }
});

describe("v1.6.3 Stage 1 — icon geometry preserves the SVG primitive contract", () => {
  test("every icon uses stroke='currentColor' + strokeWidth='2' (via shared _icon helper)", () => {
    // The shared _icon helper at the top of LeftRail.tsx sets these.
    // Sentinel: the function body matches.
    expect(LEFTRAIL_SRC).toMatch(/function _icon[\s\S]+?stroke="currentColor"/);
    expect(LEFTRAIL_SRC).toMatch(/function _icon[\s\S]+?strokeWidth="2"/);
    expect(LEFTRAIL_SRC).toMatch(/function _icon[\s\S]+?strokeLinecap="round"/);
    expect(LEFTRAIL_SRC).toMatch(/function _icon[\s\S]+?strokeLinejoin="round"/);
    expect(LEFTRAIL_SRC).toMatch(/function _icon[\s\S]+?aria-hidden="true"/);
  });

  test("all 6 named icon constants are still defined", () => {
    for (const iconName of ICON_NAMES) {
      expect(LEFTRAIL_SRC).toContain(`const ${iconName} = _icon(`);
    }
  });

  test("ICON_HOME redraw uses polylines (not the v1.6 paths)", () => {
    const block = _extractIconBlock("ICON_HOME");
    expect(block).toContain("<polyline");
    expect(block).not.toContain('d="M3 12l9-9');
  });

  test("ICON_PLAY redraw uses widened polygon (4,3 ... 22,12 ... 4,21)", () => {
    const block = _extractIconBlock("ICON_PLAY");
    expect(block).toContain("<polygon");
    expect(block).toMatch(/points="4,3\s+22,12\s+4,21"/);
    // The v1.6 polygon "6 4 20 12 6 20" should be gone.
    expect(block).not.toMatch(/points="6 4 20 12 6 20"/);
  });

  test("ICON_DIAG redraw uses monitor metaphor (rect + waveform + stand lines)", () => {
    const block = _extractIconBlock("ICON_DIAG");
    expect(block).toContain("<rect");
    expect(block).toContain("<polyline");
    // The v1.6 document-with-corner-fold path should be gone.
    expect(block).not.toContain('d="M9 3h6l4 4');
  });

  test("ICON_DECKS / ICON_SETTINGS / ICON_RUNS kept as-is (BYTE-IDENTICAL from v1.6)", () => {
    // Sentinel: the original geometry primitives still present.
    expect(_extractIconBlock("ICON_DECKS")).toMatch(/<rect[^>]+x="3"[^>]+y="3"[^>]+width="18"/);
    expect(_extractIconBlock("ICON_SETTINGS")).toContain('<circle cx="12" cy="12" r="3"');
    expect(_extractIconBlock("ICON_RUNS")).toContain('<circle cx="12" cy="12" r="10"');
  });
});

describe("v1.6.3 Stage 1 — parser sanity (regression sentinels on the test itself)", () => {
  test("rect parsing produces correct bounds", () => {
    const b = _computeIconBounds('<rect x="2" y="4" width="20" height="14" />');
    expect(b.minX).toBe(2);
    expect(b.minY).toBe(4);
    expect(b.maxX).toBe(22);
    expect(b.maxY).toBe(18);
  });

  test("polygon parsing produces correct bounds", () => {
    const b = _computeIconBounds('<polygon points="4,3 22,12 4,21" />');
    expect(b.minX).toBe(4);
    expect(b.maxX).toBe(22);
    expect(b.minY).toBe(3);
    expect(b.maxY).toBe(21);
  });

  test("circle parsing produces correct bounds", () => {
    const b = _computeIconBounds('<circle cx="12" cy="12" r="10" />');
    expect(b.minX).toBe(2);
    expect(b.minY).toBe(2);
    expect(b.maxX).toBe(22);
    expect(b.maxY).toBe(22);
  });

  test("path with absolute + relative commands (ICON_SETTINGS-style) parses", () => {
    // Subset of ICON_SETTINGS' path: M12 1v6 → (12,1) and (12,7).
    const b = _parsePathBounds("M12 1v6");
    expect(b.minX).toBe(12);
    expect(b.maxX).toBe(12);
    expect(b.minY).toBe(1);
    expect(b.maxY).toBe(7);
  });
});
