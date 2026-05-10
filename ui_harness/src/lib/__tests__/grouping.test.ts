/** Vitest tests for lib/grouping (Phase 4.6 multi-axis pure functions). */
import { describe, expect, test } from "vitest";
import {
  ALL_GROUP_AXES,
  GROUP_AXIS_LABELS,
  groupCards,
  type GroupableCard,
} from "../grouping";

const SOL_RING: GroupableCard = {
  oracle_id: "sol",
  name: "Sol Ring",
  type_line: "Artifact",
  cmc: 1,
  rarity: "uncommon",
  color_identity: [],
  primitive_tags: ["MANA_RAMP_ARTIFACT_ROCK"],
};

const KRENKO: GroupableCard = {
  oracle_id: "krenko",
  name: "Krenko, Mob Boss",
  type_line: "Legendary Creature — Goblin",
  cmc: 4,
  rarity: "mythic",
  color_identity: ["R"],
  primitive_tags: ["TOKEN_PRODUCTION_CREATURE", "ROLE_PAYOFF"],
};

const COUNTERSPELL: GroupableCard = {
  oracle_id: "counter",
  name: "Counterspell",
  type_line: "Instant",
  cmc: 2,
  rarity: "common",
  color_identity: ["U"],
  primitive_tags: ["STACK_COUNTERSPELL"],
};

const ATRAXA: GroupableCard = {
  oracle_id: "ax",
  name: "Atraxa, Praetors' Voice",
  type_line: "Legendary Creature — Phyrexian Angel Horror",
  cmc: 4,
  rarity: "mythic",
  color_identity: ["W", "U", "B", "G"],
  primitive_tags: ["FLYING_GIVE", "ROLE_VALUE_ENGINE"],
};

const FOREST: GroupableCard = {
  oracle_id: "forest",
  name: "Forest",
  type_line: "Basic Land — Forest",
  cmc: 0,
  rarity: "common",
  color_identity: ["G"],
  primitive_tags: [],
};

const UNK: GroupableCard = { oracle_id: "u1", name: "Mystery Card" };

describe("ALL_GROUP_AXES + labels", () => {
  test("7 axes covered", () => {
    expect(ALL_GROUP_AXES.length).toBe(7);
    for (const axis of ALL_GROUP_AXES) {
      expect(GROUP_AXIS_LABELS[axis]).toBeTruthy();
    }
  });
});

describe("groupCards: type axis", () => {
  test("major-type bucketing with deterministic order", () => {
    const sections = groupCards([KRENKO, SOL_RING, COUNTERSPELL, FOREST], "type");
    const labels = sections.map((s) => s.label);
    expect(labels).toContain("Creature");
    expect(labels).toContain("Artifact");
    expect(labels).toContain("Instant");
    expect(labels).toContain("Land");
    // Creature comes before Land in MAJOR_TYPE_ORDER.
    expect(labels.indexOf("Creature")).toBeLessThan(labels.indexOf("Land"));
  });

  test("missing type_line buckets into Unknown", () => {
    const sections = groupCards([UNK], "type");
    expect(sections[0].label).toBe("Unknown");
  });
});

describe("groupCards: color axis", () => {
  test("monocolor / multicolor / colorless / unknown", () => {
    const sections = groupCards([SOL_RING, KRENKO, ATRAXA, UNK], "color");
    const map = Object.fromEntries(sections.map((s) => [s.label, s.cards.length]));
    expect(map.Colorless).toBe(1);
    expect(map.Red).toBe(1);
    expect(map.Multicolor).toBe(1);
    expect(map.Unknown).toBe(1);
  });
});

describe("groupCards: cmc axis", () => {
  test("0 / N / 6+ buckets sorted ascending", () => {
    const high: GroupableCard = { oracle_id: "h", name: "Big", cmc: 8 };
    const sections = groupCards([SOL_RING, COUNTERSPELL, FOREST, high], "cmc");
    const order = sections.map((s) => s.key);
    expect(order).toEqual(["0", "1", "2", "6+"]);
  });

  test("missing cmc → Unknown bucket sorted last", () => {
    const sections = groupCards([SOL_RING, UNK], "cmc");
    expect(sections[sections.length - 1].key).toBe("Unknown");
  });
});

describe("groupCards: rarity axis", () => {
  test("rarity ordering common→mythic", () => {
    const sections = groupCards([COUNTERSPELL, SOL_RING, KRENKO], "rarity");
    const labels = sections.map((s) => s.label);
    expect(labels).toEqual(["Common", "Uncommon", "Mythic"]);
  });
});

describe("groupCards: role axis", () => {
  test("ROLE_-prefixed primitive_tag wins", () => {
    const sections = groupCards([KRENKO, ATRAXA, SOL_RING], "role");
    const map = Object.fromEntries(sections.map((s) => [s.label, s.cards.length]));
    expect(map.ROLE_PAYOFF).toBe(1);
    expect(map.ROLE_VALUE_ENGINE).toBe(1);
    expect(map.Unknown).toBe(1); // sol ring has no ROLE_ tag
  });
});

describe("groupCards: primitive axis", () => {
  test("first non-ROLE primitive tag wins", () => {
    const sections = groupCards([COUNTERSPELL, KRENKO], "primitive");
    const map = Object.fromEntries(sections.map((s) => [s.label, s.cards.length]));
    expect(map.STACK_COUNTERSPELL).toBe(1);
    expect(map.TOKEN_PRODUCTION_CREATURE).toBe(1);
  });
});

describe("groupCards: custom_tags axis", () => {
  test("emits one section per tag with options.customTags", () => {
    const customTags = {
      sol: ["fast-mana"],
      krenko: ["fast-mana", "wincon"],
    };
    const sections = groupCards([SOL_RING, KRENKO], "custom_tags", { customTags });
    const map = Object.fromEntries(sections.map((s) => [s.label, s.cards.length]));
    expect(map["fast-mana"]).toBe(2);
    expect(map.wincon).toBe(1);
  });

  test("missing customTags or unknown oracle_id → Unknown bucket", () => {
    const sections = groupCards([SOL_RING], "custom_tags");
    expect(sections[0].label).toBe("Unknown");
  });
});

describe("groupCards: empty + edge cases", () => {
  test("empty input returns empty sections", () => {
    expect(groupCards([], "type")).toEqual([]);
  });

  test("subset-fallback: cards with only oracle_id+name still group", () => {
    const sections = groupCards([UNK, UNK], "type");
    expect(sections).toHaveLength(1);
    expect(sections[0].cards).toHaveLength(2);
  });

  test("deterministic across repeated calls", () => {
    const a = groupCards([KRENKO, SOL_RING, COUNTERSPELL], "type");
    const b = groupCards([KRENKO, SOL_RING, COUNTERSPELL], "type");
    expect(a).toEqual(b);
  });
});
