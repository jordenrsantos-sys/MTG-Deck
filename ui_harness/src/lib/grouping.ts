/**
 * Multi-axis grouping — Phase 4.6.
 *
 * Pure client-side functions for grouping a list of deck cards by one of
 * 7 axes per UI_OVERHAUL_DESIGN Decision 3. NO HTTP, NO engine calls;
 * inputs come from BuildResponse fields the UI already consumes.
 *
 * Per autonomous_repair_log soft-safety #3 (missing axis fields → subset
 * fallback): cards lacking the field for the requested axis bucket into
 * an "Unknown" group rather than failing the whole grouping.
 *
 * 7 supported axes (per Decision 3 + OQ-6 resolution):
 *   - "type"          — primary type from type_line
 *   - "color"         — color identity union (W/U/B/R/G/Colorless/Multicolor)
 *   - "cmc"           — converted-mana-cost bucket (0/1/2/3/4/5/6+)
 *   - "rarity"        — common/uncommon/rare/mythic
 *   - "role"          — primitive_tag whose name starts with ROLE_ (best-effort)
 *   - "primitive"     — first primitive_tag (best-effort)
 *   - "custom_tags"   — user-supplied tags from preferences (Phase 4.6+)
 */

export type GroupAxis =
  | "type"
  | "color"
  | "cmc"
  | "rarity"
  | "role"
  | "primitive"
  | "custom_tags";

export const ALL_GROUP_AXES: ReadonlyArray<GroupAxis> = [
  "type",
  "color",
  "cmc",
  "rarity",
  "role",
  "primitive",
  "custom_tags",
] as const;

export const GROUP_AXIS_LABELS: Record<GroupAxis, string> = {
  type: "Type",
  color: "Color",
  cmc: "Mana Value",
  rarity: "Rarity",
  role: "Role",
  primitive: "Primitive",
  custom_tags: "Custom Tags",
};

/**
 * Card shape consumed by grouping. All fields optional so callers can pass
 * subset-aware inputs (e.g. when only oracle_id + name are known, every
 * non-trivial axis falls into "Unknown").
 */
export type GroupableCard = {
  oracle_id?: string | null;
  name: string;
  type_line?: string | null;
  color_identity?: ReadonlyArray<string> | null;
  cmc?: number | null;
  rarity?: string | null;
  primitive_tags?: ReadonlyArray<string> | null;
};

export type CustomTagMap = Record<string, ReadonlyArray<string>>; // oracle_id → tags

export type GroupSection = {
  key: string;
  label: string;
  sortKey: string | number;
  cards: GroupableCard[];
};

export type GroupOptions = {
  customTags?: CustomTagMap;
};

const UNKNOWN_BUCKET = "Unknown";

const MAJOR_TYPE_ORDER = [
  "Creature",
  "Planeswalker",
  "Instant",
  "Sorcery",
  "Artifact",
  "Enchantment",
  "Battle",
  "Land",
  "Tribal",
  UNKNOWN_BUCKET,
];

const COLOR_BUCKET_ORDER = [
  "White",
  "Blue",
  "Black",
  "Red",
  "Green",
  "Multicolor",
  "Colorless",
  UNKNOWN_BUCKET,
];

const RARITY_ORDER = ["common", "uncommon", "rare", "mythic", UNKNOWN_BUCKET];

const COLOR_LETTER_TO_NAME: Record<string, string> = {
  W: "White",
  U: "Blue",
  B: "Black",
  R: "Red",
  G: "Green",
};

function _typeBucket(card: GroupableCard): string {
  const tl = (card.type_line ?? "").trim();
  if (tl === "") return UNKNOWN_BUCKET;
  // type_line is "<supertypes> — <subtypes>" or just "<types>"
  // Pick first type token that matches a major-type label.
  const head = tl.split("—")[0] ?? tl;
  const tokens = head.split(/\s+/).filter(Boolean);
  for (const t of MAJOR_TYPE_ORDER) {
    if (tokens.some((tok) => tok.toLowerCase() === t.toLowerCase())) return t;
  }
  return UNKNOWN_BUCKET;
}

function _colorBucket(card: GroupableCard): string {
  const ci = card.color_identity;
  if (!Array.isArray(ci)) return UNKNOWN_BUCKET;
  if (ci.length === 0) return "Colorless";
  if (ci.length > 1) return "Multicolor";
  const letter = (ci[0] ?? "").toUpperCase();
  return COLOR_LETTER_TO_NAME[letter] ?? UNKNOWN_BUCKET;
}

function _cmcBucket(card: GroupableCard): { key: string; sort: number } {
  const cmc = card.cmc;
  if (typeof cmc !== "number" || !Number.isFinite(cmc)) {
    return { key: UNKNOWN_BUCKET, sort: 999 };
  }
  if (cmc <= 0) return { key: "0", sort: 0 };
  if (cmc >= 6) return { key: "6+", sort: 6 };
  const rounded = Math.round(cmc);
  return { key: String(rounded), sort: rounded };
}

function _rarityBucket(card: GroupableCard): string {
  const r = (card.rarity ?? "").trim().toLowerCase();
  if (r === "") return UNKNOWN_BUCKET;
  if (RARITY_ORDER.includes(r)) return r;
  return UNKNOWN_BUCKET;
}

function _roleBucket(card: GroupableCard): string {
  const tags = card.primitive_tags;
  if (!Array.isArray(tags) || tags.length === 0) return UNKNOWN_BUCKET;
  const role = tags.find((t) => typeof t === "string" && t.toUpperCase().startsWith("ROLE_"));
  if (typeof role === "string" && role !== "") return role;
  return UNKNOWN_BUCKET;
}

function _primitiveBucket(card: GroupableCard): string {
  const tags = card.primitive_tags;
  if (!Array.isArray(tags) || tags.length === 0) return UNKNOWN_BUCKET;
  // Use the first non-ROLE tag; falls back to first tag.
  const non = tags.find(
    (t) => typeof t === "string" && t !== "" && !t.toUpperCase().startsWith("ROLE_"),
  );
  return non ?? tags[0] ?? UNKNOWN_BUCKET;
}

function _customTagsBuckets(card: GroupableCard, customTags?: CustomTagMap): string[] {
  const oid = card.oracle_id;
  if (!oid || !customTags) return [UNKNOWN_BUCKET];
  const tags = customTags[oid];
  if (!Array.isArray(tags) || tags.length === 0) return [UNKNOWN_BUCKET];
  return [...tags];
}

function _orderedSort<T extends { sortKey: string | number }>(
  sections: T[],
  order: ReadonlyArray<string>,
  getKey: (s: T) => string,
): T[] {
  return [...sections].sort((a, b) => {
    const aIdx = order.indexOf(getKey(a));
    const bIdx = order.indexOf(getKey(b));
    if (aIdx === -1 && bIdx === -1) return getKey(a).localeCompare(getKey(b));
    if (aIdx === -1) return 1;
    if (bIdx === -1) return -1;
    return aIdx - bIdx;
  });
}

export function groupCards(
  cards: ReadonlyArray<GroupableCard>,
  axis: GroupAxis,
  options: GroupOptions = {},
): GroupSection[] {
  const buckets = new Map<string, GroupSection>();
  const ensure = (key: string, sortKey: string | number, label?: string) => {
    if (!buckets.has(key)) {
      buckets.set(key, {
        key,
        label: label ?? key,
        sortKey,
        cards: [],
      });
    }
    return buckets.get(key)!;
  };

  for (const c of cards) {
    if (axis === "type") {
      const k = _typeBucket(c);
      ensure(k, k).cards.push(c);
    } else if (axis === "color") {
      const k = _colorBucket(c);
      ensure(k, k).cards.push(c);
    } else if (axis === "cmc") {
      const { key, sort } = _cmcBucket(c);
      ensure(key, sort).cards.push(c);
    } else if (axis === "rarity") {
      const k = _rarityBucket(c);
      ensure(k, k, k.charAt(0).toUpperCase() + k.slice(1)).cards.push(c);
    } else if (axis === "role") {
      const k = _roleBucket(c);
      ensure(k, k).cards.push(c);
    } else if (axis === "primitive") {
      const k = _primitiveBucket(c);
      ensure(k, k).cards.push(c);
    } else if (axis === "custom_tags") {
      const ks = _customTagsBuckets(c, options.customTags);
      for (const k of ks) ensure(k, k).cards.push(c);
    }
  }

  const sections = Array.from(buckets.values());
  if (axis === "type") return _orderedSort(sections, MAJOR_TYPE_ORDER, (s) => s.key);
  if (axis === "color") return _orderedSort(sections, COLOR_BUCKET_ORDER, (s) => s.key);
  if (axis === "cmc")
    return [...sections].sort((a, b) => Number(a.sortKey) - Number(b.sortKey));
  if (axis === "rarity") return _orderedSort(sections, RARITY_ORDER, (s) => s.key);
  // role / primitive / custom_tags: alphabetical with Unknown last.
  return [...sections].sort((a, b) => {
    const aUnknown = a.key === UNKNOWN_BUCKET ? 1 : 0;
    const bUnknown = b.key === UNKNOWN_BUCKET ? 1 : 0;
    if (aUnknown !== bUnknown) return aUnknown - bUnknown;
    return a.key.localeCompare(b.key);
  });
}
