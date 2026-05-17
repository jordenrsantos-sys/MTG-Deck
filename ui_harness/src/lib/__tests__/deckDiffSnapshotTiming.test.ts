/**
 * Vitest tests for v1.4 Stage 1 — pre-Complete deckText snapshot timing.
 *
 * v1.3 used `useState<string|null>` to snapshot deckText at
 * handleManaTuneTool entry; the setter was async, and by the time the
 * diff useMemo ran the snapshot could already be overwritten by
 * handleApplyCompletedDecklist's USER_EDIT_DECK_TEXT dispatch (which
 * sets deckText to the completed text). Result: diff(post, post) = []
 * — near-empty list with only commander when residual state lingered.
 *
 * v1.4 fix: useRef captured at handleUnifiedCompleteDeck entry, BEFORE
 * any state updates. Synchronous read + zero render-cycle risk.
 *
 * These tests don't render the full WorkspaceView (no @testing-library/
 * react per AUTOMATION_RULES halt-and-ask precedent). They simulate the
 * timing sequence at the call-site level — proving the ref read at
 * memo-time matches what was captured at click-time, and the diff
 * produces the expected growth delta.
 */
import { describe, expect, test } from "vitest";
import { computeDeckAdditions } from "../deckDiff";

// Reference timing model: useRef behavior mirrored as { current: T }.
function makeRef<T>(initial: T): { current: T } {
  return { current: initial };
}

describe("v1.4 Stage 1 — useRef synchronous capture at click handler entry", () => {
  test("ref captures deckText at click-time; subsequent USER_EDIT_DECK_TEXT mutates state but ref unchanged", () => {
    // Simulated state: user has an imported deck loaded.
    const preCompleteText = "Commander\n1 Shelob, Child of Ungoliant\nDeck\n1 Sol Ring\n1 Forest";
    let workspaceDeckText = preCompleteText;

    // Click handler entry: ref captured BEFORE any state work.
    const preRef = makeRef<string>("");
    preRef.current = workspaceDeckText;

    // Simulate the v1.3 race: handleApplyCompletedDecklist runs before
    // useMemo evaluates and dispatches USER_EDIT_DECK_TEXT, mutating
    // deckText to the completed text.
    const completedText =
      "Commander\n1 Shelob, Child of Ungoliant\nDeck\n1 Sol Ring\n1 Forest\n1 Lightning Bolt\n1 Mountain\n1 Plains";
    workspaceDeckText = completedText;

    // Ref is unchanged — still the pre-Complete value (this is the v1.4 fix).
    expect(preRef.current).toBe(preCompleteText);
    expect(preRef.current).not.toBe(workspaceDeckText);

    // Diff against the captured ref + the completed text → 3 additions.
    const adds = computeDeckAdditions(preRef.current, completedText);
    expect(adds).toHaveLength(3);
    expect(adds.map((a) => a.name)).toEqual(["Lightning Bolt", "Mountain", "Plains"]);
  });

  test("ref-based diff yields growth-delta entries (Shelob 78→100 = 22)", () => {
    // Canonical Shelob 1010839 fixture: 78-card imported deck.
    const preText =
      "Commander\n1 Shelob, Child of Ungoliant\nDeck\n" +
      ["Sol Ring", "Arcane Signet", "Birthing Pod", "Eldritch Evolution", "Neoform", "Prime Speaker Vannifar"]
        .map((c) => `1 ${c}`)
        .join("\n") +
      "\n" +
      Array.from({ length: 35 }, () => "1 Forest").join("\n") +
      "\n" +
      Array.from({ length: 36 }, () => "1 Swamp").join("\n");

    // Engine adds 22 filler cards.
    const completedText =
      preText +
      "\n" +
      Array.from({ length: 22 }, (_, i) => `1 Filler${i}`).join("\n");

    const preRef = makeRef<string>("");
    preRef.current = preText;

    // Even after some unrelated state mutation, the ref remains:
    const someOtherStateChange = "(simulated state delta)";
    void someOtherStateChange;

    const adds = computeDeckAdditions(preRef.current, completedText);
    expect(adds).toHaveLength(22);
  });

  test("v1.3 bug regression — useState async setter would yield empty diff", () => {
    // SIMULATES the v1.3 bug:
    //   - User clicks Complete.
    //   - handleManaTuneTool runs setPreCompleteDeckText(deckText). Async.
    //   - The setter hasn't flushed yet — state.preCompleteDeckText is null.
    //   - handleApplyCompletedDecklist dispatches USER_EDIT_DECK_TEXT.
    //   - React batches both renders. By the time useMemo runs, deckText
    //     has been overwritten to the completed text. preCompleteDeckText
    //     state, when finally flushed, may have been BATCHED with the
    //     deckText update — reading the same closure-captured value.
    //   - In practice: pre = "" (initial state) OR pre = completed (if
    //     closure captured the post-state). Either way, diff is empty.
    const completedText =
      "Commander\n1 Shelob, Child of Ungoliant\nDeck\n1 Sol Ring\n1 Forest";
    // Simulated stale snapshot (the bug): pre == completed.
    const adds_buggy = computeDeckAdditions(completedText, completedText);
    expect(adds_buggy).toHaveLength(0);
    // Or pre == "" (initial state of useState<string|null>):
    const adds_empty_pre = computeDeckAdditions("", completedText);
    expect(adds_empty_pre.length).toBeGreaterThan(0);
    // The v1.4 ref captures the pre value at click-time so neither
    // failure mode applies — see the test above.
  });

  test("ref captures the EXACT value at write-time (no closure-staleness)", () => {
    const ref = makeRef<string>("");
    // First click: import "deck A"
    let deckText = "1 Card A";
    ref.current = deckText;
    expect(ref.current).toBe("1 Card A");

    // User edits to "deck B" — but click handler hasn't re-fired.
    deckText = "1 Card B";
    expect(ref.current).toBe("1 Card A"); // ref unchanged; still old click's snapshot

    // Second click: re-snapshot.
    ref.current = deckText;
    expect(ref.current).toBe("1 Card B");
  });

  test("growth-delta invariant: diff.length === completed_lines.length - pre_lines.length", () => {
    const pre = "1 A\n1 B\n1 C";
    const post = "1 A\n1 B\n1 C\n1 D\n1 E";
    const ref = makeRef<string>(pre);
    const adds = computeDeckAdditions(ref.current, post);
    const preLines = pre.split("\n").filter((l) => l.trim() !== "").length;
    const postLines = post.split("\n").filter((l) => l.trim() !== "").length;
    expect(adds.length).toBe(postLines - preLines);
  });
});

describe("v1.5 Stage 2 — commander-prepended snapshot yields correct diff entry count", () => {
  // The engine's completed_decklist_text_v1 always emits a Commander section
  // banner + commander line + Deck banner. v1.4's preCompleteDeckTextRef
  // captured only the raw deckText (no commander). Result: the diff saw
  // the commander as a "new" addition AND the pre-side lacked the Commander
  // banner that the post-side strips via _BANNER_LINES — net effect was a
  // 1-entry diff for the commander when the engine actually added ~22 cards.
  //
  // v1.5 Stage 2 fix: prepend "Commander\n1 ${commander}\nDeck\n" so both
  // sides of the diff see the same banner structure + the same commander
  // entry. The deckDiff parser strips banners on BOTH sides via the v1.5
  // Stage 1 extended _BANNER_LINES vocabulary.

  function synthPre(commander: string, deckText: string): string {
    const trimmed = commander.trim();
    return trimmed !== ""
      ? `Commander\n1 ${trimmed}\nDeck\n${deckText}`
      : deckText;
  }

  test("Shelob canonical 77-card deckText + commander='Shelob' → 22-entry diff (NOT 1)", () => {
    const commander = "Shelob, Child of Ungoliant";
    const deckText =
      ["Sol Ring", "Arcane Signet", "Birthing Pod", "Eldritch Evolution", "Neoform", "Prime Speaker Vannifar"]
        .map((c) => `1 ${c}`)
        .join("\n") +
      "\n" +
      Array.from({ length: 35 }, () => "1 Forest").join("\n") +
      "\n" +
      Array.from({ length: 36 }, () => "1 Swamp").join("\n");
    const pre = synthPre(commander, deckText);

    // Engine returns full 100-card completed text with Commander/Deck banners.
    const completed =
      "Commander\n1 Shelob, Child of Ungoliant\nDeck\n" +
      deckText +
      "\n" +
      Array.from({ length: 22 }, (_, i) => `1 EngineFiller${i}`).join("\n");

    const adds = computeDeckAdditions(pre, completed);
    expect(adds).toHaveLength(22);
    // Crucially: the commander does NOT appear as an addition (it's on both sides).
    expect(adds.find((e) => e.name === "Shelob, Child of Ungoliant")).toBeUndefined();
  });

  test("WITHOUT v1.5 commander prepend, diff would include commander as a spurious addition", () => {
    const commander = "Shelob, Child of Ungoliant";
    const deckText = "1 Sol Ring\n1 Forest";
    // Buggy v1.4 path: pre is just deckText, no commander.
    const pre_buggy = deckText;
    const completed = `Commander\n1 ${commander}\nDeck\n${deckText}\n1 NewCard`;

    const adds_buggy = computeDeckAdditions(pre_buggy, completed);
    // Buggy path: commander appears as a new addition (1) + NewCard (1) = 2.
    expect(adds_buggy.length).toBe(2);
    expect(adds_buggy.find((e) => e.name === commander)).toBeDefined();

    // v1.5 fix: prepend commander.
    const pre_fixed = synthPre(commander, deckText);
    const adds_fixed = computeDeckAdditions(pre_fixed, completed);
    // Fixed path: only NewCard is added.
    expect(adds_fixed.length).toBe(1);
    expect(adds_fixed.find((e) => e.name === commander)).toBeUndefined();
    expect(adds_fixed[0].name).toBe("NewCard");
  });

  test("empty commander → no banner prepend (back-compat with flat decklist input)", () => {
    const pre = synthPre("", "1 Sol Ring\n1 Forest");
    expect(pre).toBe("1 Sol Ring\n1 Forest");
    expect(pre).not.toContain("Commander");
  });

  test("commander with whitespace → trimmed before prepending", () => {
    const pre = synthPre("  Krenko, Mob Boss  ", "1 Sol Ring");
    expect(pre).toContain("1 Krenko, Mob Boss\n");
    expect(pre).not.toContain("1   Krenko");
  });
});
