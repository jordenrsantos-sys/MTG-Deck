/** Vitest tests for the 5 Phase 4.3 parsers. */
import { describe, expect, test } from "vitest";
import { parsePlainText } from "../plainText";
import { parseArenaText } from "../arenaText";
import { parseMtgoText } from "../mtgoText";
import { detectFormatFromText, parseTextWithDispatch } from "../fileUpload";
import { importFromUrl } from "../archidektUrl";
import type { ImportResult } from "../types";

describe("parsePlainText", () => {
  test("rejects empty input", () => {
    const r = parsePlainText("");
    expect(r.status).toBe("INVALID_REQUEST");
    expect(r.reason_code).toBe("empty_input");
  });

  test("parses count name pairs", () => {
    const r = parsePlainText("1 Sol Ring\n2x Forest\nLightning Bolt");
    expect(r.status).toBe("OK");
    expect(r.deck?.cards).toHaveLength(3);
    expect(r.deck?.cards[0]).toEqual({ name: "Sol Ring", count: 1, oracle_id: null });
    expect(r.deck?.cards[1]).toEqual({ name: "Forest", count: 2, oracle_id: null });
    expect(r.deck?.cards[2]).toEqual({ name: "Lightning Bolt", count: 1, oracle_id: null });
  });

  test("Commander section header sets the commander", () => {
    const r = parsePlainText("Commander\n1 Atraxa, Praetors' Voice\n\nDeck\n1 Sol Ring");
    expect(r.deck?.commander?.name).toBe("Atraxa, Praetors' Voice");
    expect(r.deck?.cards.map((c) => c.name)).toEqual(["Sol Ring"]);
  });

  test("comment + sideboard lines drop with reasons", () => {
    const r = parsePlainText("# header\n// inline\n1 Sol Ring\nSideboard\n1 Force of Will");
    expect(r.deck?.cards).toHaveLength(1);
    expect(r.unknowns).toHaveLength(1);
    expect(r.unknowns[0].reason).toBe("sideboard_dropped_for_commander");
  });
});

describe("parseArenaText", () => {
  test("strips set/collector tail", () => {
    const r = parseArenaText("Commander\n1 Atraxa, Praetors' Voice (CMR) 270\n\nDeck\n1 Sol Ring (CMR) 251");
    expect(r.status).toBe("OK");
    expect(r.deck?.commander?.name).toBe("Atraxa, Praetors' Voice");
    expect(r.deck?.cards[0].name).toBe("Sol Ring");
    expect(r.source).toBe("arena_text");
  });
});

describe("parseMtgoText", () => {
  test("blank-line separator routes post-blank cards into sideboard unknowns", () => {
    const r = parseMtgoText("1 Sol Ring\n1 Forest\n\n1 Force of Will");
    expect(r.deck?.cards.map((c) => c.name)).toEqual(["Sol Ring", "Forest"]);
    expect(r.unknowns.find((u) => u.name === "Force of Will")?.reason).toBe(
      "sideboard_dropped_for_commander",
    );
  });
});

describe("file-upload dispatcher", () => {
  test("detectFormatFromText with .dec extension routes to mtgo", () => {
    expect(detectFormatFromText("1 Sol Ring", "deck.dec")).toBe("mtgo_text");
  });

  test("detectFormatFromText with arena tail routes to arena_text", () => {
    expect(detectFormatFromText("1 Sol Ring (CMR) 251", "deck.txt")).toBe("arena_text");
  });

  test("detectFormatFromText defaults to plain_text", () => {
    expect(detectFormatFromText("1 Sol Ring", "deck.txt")).toBe("plain_text");
  });

  test("parseTextWithDispatch on json defers", () => {
    const r = parseTextWithDispatch('{"cards": []}', "deck.json");
    expect(r.status).toBe("DEFERRED");
    expect(r.reason_code).toBe("json_offline_import_deferred");
  });

  test("plain-text dispatch tags source as file_upload", () => {
    const r = parseTextWithDispatch("1 Sol Ring", "deck.txt");
    expect(r.status).toBe("OK");
    expect(r.source).toBe("file_upload");
  });
});

describe("importFromUrl (Engine-4A POST /import/url adapter)", () => {
  test("rejects empty url", async () => {
    const r = await importFromUrl("");
    expect(r.status).toBe("INVALID_REQUEST");
    expect(r.reason_code).toBe("empty_url");
  });

  test("network error becomes FETCH_ERROR", async () => {
    const r = await importFromUrl("https://archidekt.com/decks/8000000/", {
      fetcher: async () => {
        throw new Error("offline");
      },
    });
    expect(r.status).toBe("FETCH_ERROR");
    expect(r.reason_code).toBe("network_error");
  });

  test("OK response from server passes through", async () => {
    const stub: ImportResult = {
      version: "import_url_v1",
      status: "OK",
      source: "archidekt",
      deck: {
        commander: { name: "Atraxa", count: 1, oracle_id: "ax" },
        cards: [{ name: "Sol Ring", count: 1, oracle_id: "sr" }],
        format: "commander",
      },
      unknowns: [],
      reason_code: null,
      deferred_reason: null,
    };
    const r = await importFromUrl("https://archidekt.com/decks/8000000/", {
      fetcher: async () =>
        new Response(JSON.stringify(stub), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    });
    expect(r.status).toBe("OK");
    expect(r.source).toBe("archidekt");
    expect(r.deck?.commander?.name).toBe("Atraxa");
  });

  test("DEFERRED response (Moxfield) passes through with reason", async () => {
    const stub: ImportResult = {
      version: "import_url_v1",
      status: "DEFERRED",
      source: "moxfield",
      deck: null,
      unknowns: [],
      reason_code: "source_deferred",
      deferred_reason: "moxfield_cloudflare_gated_5a_2_1_pivot",
    };
    const r = await importFromUrl("https://moxfield.com/decks/abc", {
      fetcher: async () =>
        new Response(JSON.stringify(stub), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    });
    expect(r.status).toBe("DEFERRED");
    expect(r.source).toBe("moxfield");
    expect(r.deferred_reason).toContain("cloudflare");
  });

  test("non-JSON server response becomes FETCH_ERROR", async () => {
    const r = await importFromUrl("https://archidekt.com/decks/8000000/", {
      fetcher: async () =>
        new Response("<html>nope</html>", {
          status: 502,
          headers: { "Content-Type": "text/html" },
        }),
    });
    expect(r.status).toBe("FETCH_ERROR");
    expect(r.reason_code).toContain("non_json_response_status_502");
  });
});
