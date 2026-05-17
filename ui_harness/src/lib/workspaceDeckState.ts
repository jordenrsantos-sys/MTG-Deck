/**
 * workspaceDeckState — Phase 4.13.2 architectural refactor.
 *
 * Pure state machine for the workspace's active deck. Replaces the
 * `useState` + `useRef` + two-racing-useEffects pattern in WorkspaceView
 * that drove three sequential hotfixes (4.12.1 + 4.13 + 4.13.1). The
 * fundamental bug was: an active-deck-slot writer useEffect fired
 * immediately on initial render with the Krenko fallback state and wrote
 * that fallback to localStorage BEFORE the IMPORT-consumer useEffect had
 * a chance to populate the imported deck. React 18 StrictMode
 * (mount→unmount→remount) made the race deterministic — second pass
 * found the IMPORT slot empty (consumed) and the active-deck slot
 * populated (with garbage), restoring Krenko but preserving the
 * "archidekt" source label.
 *
 * The fix: a single `isHydrated` flag gates ALL persistence writes.
 * Hydration runs once with explicit precedence (IMPORT > active-deck >
 * none); persistence only writes when isHydrated && source !== "fallback".
 * Default-state writes simply do not happen.
 *
 * HARD safety #14: this module is NEW; existing
 * workspaceIntegrationAdapters helpers stay BYTE-IDENTICAL. The reducer
 * USES them (the hydration useEffect calls _readStagedImport /
 * _restoreFromActiveDeckSlot) but does not modify them.
 *
 * HARD safety: reducer is PURE — no fetch / localStorage / Date.now /
 * unseeded Math.random in the body. All side-effects belong to the
 * useEffects in WorkspaceView.
 */

export type DeckSource =
  // Hardcoded default Krenko deck — NEVER persisted to slot. The whole
  // point of isHydrated + the source !== "fallback" guard.
  | "fallback"
  // Import-flow sources (parser registry from ImportRoute).
  | "archidekt"
  | "arena_text"
  | "mtgo_text"
  | "plain_text"
  | "file_upload"
  // User typed directly into DeckInputPanel.
  | "manual"
  // Loaded from SavedDecksView.
  | "saved_deck"
  // Populated by Build button / `/build` response.
  | "build";

/**
 * Opaque shape — the reducer doesn't introspect BuildResponse fields;
 * panels (SufficiencyDashboard / SwapSuggestionsList /
 * CommanderRecommendationPanel / etc.) read fields via
 * `workspaceIntegrationAdapters` extractors. Per autonomous_repair_log
 * soft-safety: minimal type, no field-level coupling between this state
 * machine and engine response evolution.
 */
export type BuildResponseShape = Record<string, unknown>;

export interface ActiveDeckState {
  commander: string;
  deckText: string;
  /** Monotonically increasing — bumped on every text mutation so observers
   *  can re-parse / re-validate without comparing string content. */
  deckTextRevision: number;
  source: DeckSource;
  /** False until the first HYDRATE_* action lands. Gates persistence
   *  effects so the default Krenko state never writes to localStorage. */
  isHydrated: boolean;
  buildResponse: BuildResponseShape | null;
  buildPending: boolean;
  buildError: string | null;
  // Phase 4.14 Stage 1 — unified Complete-deck flow state. `isCompleted`
  // flips true after a successful /deck/complete_v1 + auto-commit; user
  // can then hit Build (next step in the workflow). USER_EDIT_* and
  // LOAD_SAVED_DECK actions implicitly reset isCompleted (those mutate
  // the deck so prior completion is stale).
  isCompleted: boolean;
  completePending: boolean;
  completeError: string | null;
  // v1.1 Stage 3 — Upgrade-deck flow state. `upgradeSuggestions` carries the
  // `recommended_swaps_v1` array from the latest /deck/tune_v1 success;
  // null when the user hasn't run Upgrade yet OR after a deck mutation
  // invalidates it. `lastUpgradedAt` is the ISO timestamp of the last
  // successful UPGRADE_SUCCESS — used by the UI to disambiguate "user just
  // clicked Upgrade" from "stale prior result still showing".
  upgradePending: boolean;
  upgradeSuggestions: UpgradeSwapSuggestion[] | null;
  upgradeError: string | null;
  lastUpgradedAt: string | null;
  // v1.6.3 Stage 2 — additive: pending-review queue for Complete-deck
  // additions. Engine response stages here via STAGE_PROPOSED_ADDS;
  // user toggles per-card accept/reject; APPLY_ACCEPTED_ADDS commits
  // the accepted subset to deckText. INITIAL_STATE adds exactly ONE
  // new field (empty array); all other field values BYTE-IDENTICAL.
  pendingAdds: ReadonlyArray<ProposedAdd>;
}

/**
 * Engine-side `DeckTuneSwapV1` (cut_name/add_name/reasons_v1) reshaped
 * for the reducer. Keeps the engine's field names so the response can
 * round-trip with zero adaptation in the reducer body. UpgradeSuggestionsList
 * (the consumer panel) handles display + per-row Apply via the existing
 * `lib/applySwap.ts` helper by constructing a SwapSuggestion `{out: cut_name,
 * in: add_name}` at the click site.
 */
/**
 * v1.6.3 Stage 2 — ProposedAdd shape staged by STAGE_PROPOSED_ADDS.
 *
 * Mirrors the `/deck/complete_v1.added_cards_v1[]` entry shape (card_name +
 * reasons array — the v1.6.2 lib/justificationLabels translation map
 * BYTE-IDENTICAL is reused at the UI seam). `accepted: true` is the
 * default — proposed cards are accepted by default; user clicks the
 * per-row checkbox to REJECT a specific card. APPLY_ACCEPTED_ADDS
 * commits only the accepted subset to deckText.
 */
export type ProposedAdd = {
  card_name: string;
  reasons: ReadonlyArray<string>;
  accepted: boolean;
};

export type UpgradeSwapSuggestion = {
  cut_name?: string;
  add_name?: string;
  reasons_v1?: ReadonlyArray<string>;
  [key: string]: unknown;
};

export type DeckAction =
  | { type: "HYDRATE_FROM_IMPORT_SLOT"; commander: string; decklist: string; source: DeckSource }
  | { type: "HYDRATE_FROM_ACTIVE_SLOT"; commander: string; decklist: string; source: DeckSource }
  | { type: "HYDRATE_NO_SOURCE" }
  | { type: "USER_EDIT_COMMANDER"; commander: string }
  | { type: "USER_EDIT_DECK_TEXT"; deckText: string }
  | { type: "LOAD_SAVED_DECK"; commander: string; decklist: string }
  | { type: "BUILD_PENDING" }
  | { type: "BUILD_SUCCESS"; response: BuildResponseShape }
  | { type: "BUILD_ERROR"; error: string }
  | { type: "CLEAR_BUILD_RESPONSE" }
  // Phase 4.14 Stage 1 — unified Complete-deck flow.
  | { type: "COMPLETE_PENDING" }
  | { type: "COMPLETE_SUCCESS"; deckText: string }
  | { type: "COMPLETE_ERROR"; error: string }
  // v1.1 Stage 3 — Upgrade-deck flow (parallel pattern to BUILD_*).
  | { type: "UPGRADE_PENDING" }
  | { type: "UPGRADE_SUCCESS"; suggestions: ReadonlyArray<UpgradeSwapSuggestion>; nowIso?: string }
  | { type: "UPGRADE_ERROR"; error: string }
  | { type: "CLEAR_UPGRADE_SUGGESTIONS" }
  // v1.6.2 Stage 2 — user-initiated clear-deck action. Additive extension
  // (12th DeckAction); semantically mirrors RESET but keeps `isHydrated:
  // true` so the persistence useEffect doesn't re-fire hydration after
  // the user explicitly cleared. INITIAL_STATE BYTE-IDENTICAL — handler
  // returns INITIAL_STATE values per-field with the one isHydrated tweak.
  | { type: "USER_CLEAR_DECK" }
  // v1.6.3 Stage 2 — pending-review queue for Complete-deck additions.
  // Additive extension (4 new actions; DeckAction grows 12 → 16). All
  // existing 12 action handlers BYTE-IDENTICAL. STAGE_PROPOSED_ADDS
  // replaces pendingAdds with the engine response; TOGGLE_PROPOSED_ADD
  // flips a single row's `accepted` flag; APPLY_ACCEPTED_ADDS appends
  // the accepted card_names to deckText (one per line) + clears
  // pendingAdds + upgrades source via the existing fallback→manual
  // helper; DISMISS_PROPOSED_ADDS clears without mutating the deck.
  | { type: "STAGE_PROPOSED_ADDS"; adds: ReadonlyArray<{ card_name: string; reasons?: ReadonlyArray<string> }> }
  | { type: "TOGGLE_PROPOSED_ADD"; index: number }
  | { type: "APPLY_ACCEPTED_ADDS" }
  | { type: "DISMISS_PROPOSED_ADDS" }
  | { type: "RESET" };

const FALLBACK_DECK_TEXT = ["1 Sol Ring", "1 Arcane Signet", "Goblin Matron", "Skirk Prospector", "Impact Tremors"].join("\n");
const FALLBACK_COMMANDER = "Krenko, Mob Boss";

export const INITIAL_STATE: ActiveDeckState = {
  commander: FALLBACK_COMMANDER,
  deckText: FALLBACK_DECK_TEXT,
  deckTextRevision: 0,
  source: "fallback",
  isHydrated: false,
  buildResponse: null,
  buildPending: false,
  buildError: null,
  isCompleted: false,
  completePending: false,
  completeError: null,
  // v1.1 Stage 3 — Upgrade-deck flow defaults.
  upgradePending: false,
  upgradeSuggestions: null,
  upgradeError: null,
  lastUpgradedAt: null,
  // v1.6.3 Stage 2 — ONE new field; default empty array. All other
  // INITIAL_STATE field values BYTE-IDENTICAL from v1.6.2.
  pendingAdds: [],
};

/**
 * USER_EDIT_* upgrades source from "fallback" to "manual" only — preserves
 * real source labels (archidekt / saved_deck / build / etc.) when the user
 * tweaks the imported deck. Without this guard, every keystroke would
 * overwrite the provenance label.
 */
function _upgradeFallbackToManual(source: DeckSource): DeckSource {
  return source === "fallback" ? "manual" : source;
}

/**
 * BUILD_SUCCESS upgrades source from "fallback" to "build" only — same
 * reasoning: if a user imported via Archidekt then hit Build, we keep
 * "archidekt" as the deck-text source; "build" only describes the
 * BuildResponse, not the deck origin.
 */
function _upgradeFallbackToBuild(source: DeckSource): DeckSource {
  return source === "fallback" ? "build" : source;
}

export function deckReducer(state: ActiveDeckState, action: DeckAction): ActiveDeckState {
  switch (action.type) {
    case "HYDRATE_FROM_IMPORT_SLOT":
    case "HYDRATE_FROM_ACTIVE_SLOT": {
      // Idempotent — defends against React 18 StrictMode's double-mount.
      // Once hydrated, ignore subsequent HYDRATE_* attempts.
      if (state.isHydrated) return state;
      return {
        ...state,
        commander: action.commander,
        deckText: action.decklist,
        deckTextRevision: state.deckTextRevision + 1,
        source: action.source,
        isHydrated: true,
      };
    }

    case "HYDRATE_NO_SOURCE": {
      // Both slots empty — flip isHydrated, preserve fallback values so
      // the user sees the default Krenko deck. Subsequent USER_EDIT_*
      // will upgrade source to "manual" and unlock persistence.
      if (state.isHydrated) return state;
      return { ...state, isHydrated: true };
    }

    case "USER_EDIT_COMMANDER": {
      if (action.commander === state.commander) return state;
      return {
        ...state,
        commander: action.commander,
        source: _upgradeFallbackToManual(state.source),
        // Commander change invalidates BuildResponse (different deck identity).
        buildResponse: null,
        // Phase 4.14: deck mutation invalidates prior Complete state.
        isCompleted: false,
        completeError: null,
        // v1.1: deck mutation invalidates prior upgrade suggestions (stale).
        upgradeSuggestions: null,
        upgradeError: null,
      };
    }

    case "USER_EDIT_DECK_TEXT": {
      if (action.deckText === state.deckText) return state;
      return {
        ...state,
        deckText: action.deckText,
        deckTextRevision: state.deckTextRevision + 1,
        source: _upgradeFallbackToManual(state.source),
        // Deck text change invalidates BuildResponse.
        buildResponse: null,
        // Phase 4.14: deck mutation invalidates prior Complete state.
        isCompleted: false,
        completeError: null,
        // v1.1: deck mutation invalidates prior upgrade suggestions.
        upgradeSuggestions: null,
        upgradeError: null,
      };
    }

    case "LOAD_SAVED_DECK": {
      return {
        ...state,
        commander: action.commander,
        deckText: action.decklist,
        deckTextRevision: state.deckTextRevision + 1,
        source: "saved_deck",
        buildResponse: null,
        buildError: null,
        // Phase 4.14: loading a different deck invalidates prior Complete.
        isCompleted: false,
        completeError: null,
        // v1.1: loading a different deck invalidates prior upgrade suggestions.
        upgradeSuggestions: null,
        upgradeError: null,
      };
    }

    case "BUILD_PENDING": {
      return { ...state, buildPending: true, buildError: null };
    }

    case "BUILD_SUCCESS": {
      return {
        ...state,
        buildResponse: action.response,
        buildPending: false,
        buildError: null,
        source: _upgradeFallbackToBuild(state.source),
      };
    }

    case "BUILD_ERROR": {
      // Preserve prior buildResponse — a transient network error shouldn't
      // wipe a previous successful build's panels.
      return { ...state, buildPending: false, buildError: action.error };
    }

    case "CLEAR_BUILD_RESPONSE": {
      if (state.buildResponse === null && state.buildError === null) return state;
      return { ...state, buildResponse: null, buildError: null };
    }

    case "COMPLETE_PENDING": {
      return { ...state, completePending: true, completeError: null };
    }

    case "COMPLETE_SUCCESS": {
      // /deck/complete_v1 returned a 99-card decklist. Commit it as the new
      // deckText, bump revision, mark isCompleted, and clear buildResponse —
      // the prior Build is stale because the deck changed. Source is
      // PRESERVED (archidekt → still archidekt; manual → still manual).
      return {
        ...state,
        deckText: action.deckText,
        deckTextRevision: state.deckTextRevision + 1,
        completePending: false,
        completeError: null,
        isCompleted: true,
        buildResponse: null,
      };
    }

    case "COMPLETE_ERROR": {
      // Preserve prior deckText + buildResponse — transient error doesn't
      // wipe the user's working state.
      return { ...state, completePending: false, completeError: action.error };
    }

    case "UPGRADE_PENDING": {
      // v1.1 Stage 3 — mirrors BUILD_PENDING. Clears prior error; preserves
      // prior upgradeSuggestions until UPGRADE_SUCCESS replaces them (lets
      // the user see the old list while the new request is in flight).
      return { ...state, upgradePending: true, upgradeError: null };
    }

    case "UPGRADE_SUCCESS": {
      // v1.1 Stage 3 — captures recommended_swaps_v1 verbatim + bumps
      // lastUpgradedAt. Does NOT mutate deckText (apply is per-row user
      // action via USER_EDIT_DECK_TEXT). Caller is expected to pass an
      // explicit `nowIso` for deterministic tests; falls through to ""
      // when omitted (the reducer is PURE — no Date.now() per HARD #11).
      return {
        ...state,
        upgradePending: false,
        upgradeError: null,
        upgradeSuggestions: Array.from(action.suggestions),
        lastUpgradedAt: action.nowIso ?? "",
      };
    }

    case "UPGRADE_ERROR": {
      // v1.1 Stage 3 — mirrors BUILD_ERROR. Preserves prior suggestions —
      // transient error shouldn't wipe a previous successful tune call's
      // recommendations.
      return { ...state, upgradePending: false, upgradeError: action.error };
    }

    case "CLEAR_UPGRADE_SUGGESTIONS": {
      // v1.1 Stage 3 — explicit dismissal (UI "× Clear suggestions" affordance).
      if (state.upgradeSuggestions === null && state.upgradeError === null) return state;
      return { ...state, upgradeSuggestions: null, upgradeError: null };
    }

    case "RESET": {
      return INITIAL_STATE;
    }

    case "USER_CLEAR_DECK": {
      // v1.6.2 Stage 2 — clear deck affordance for the user. Returns
      // INITIAL_STATE semantics (commander/deckText/buildResponse all
      // reset to defaults) but keeps `isHydrated: true` so the
      // hydration useEffect in WorkspaceView doesn't immediately
      // re-hydrate from the staged-import / active-deck slots.
      // INITIAL_STATE BYTE-IDENTICAL per HARD #2 — this handler builds
      // a fresh object from INITIAL_STATE rather than mutating it.
      return { ...INITIAL_STATE, isHydrated: true };
    }

    case "STAGE_PROPOSED_ADDS": {
      // v1.6.3 Stage 2 — stage engine response for per-card review.
      // Each proposed add defaults to `accepted: true`; user toggles
      // a row to REJECT it. Replaces any prior pendingAdds (e.g. a
      // second Complete click overrides the first). Also clears
      // completePending (the spinner) + completeError because this
      // action runs AFTER the engine response lands — the in-flight
      // signal should drop. completeError clears to avoid surfacing
      // a stale prior failure alongside fresh successful adds.
      const adds = Array.isArray(action.adds) ? action.adds : [];
      const next: ProposedAdd[] = [];
      for (const add of adds) {
        if (!add || typeof add !== "object") continue;
        const name = typeof add.card_name === "string" ? add.card_name.trim() : "";
        if (name === "") continue;
        const reasons = Array.isArray(add.reasons)
          ? add.reasons.filter((r): r is string => typeof r === "string" && r !== "")
          : [];
        next.push({ card_name: name, reasons, accepted: true });
      }
      return {
        ...state,
        pendingAdds: next,
        completePending: false,
        completeError: null,
      };
    }

    case "TOGGLE_PROPOSED_ADD": {
      // v1.6.3 Stage 2 — flip the `accepted` flag of a single row.
      // Out-of-range indices are no-ops (defensive).
      const idx = typeof action.index === "number" ? action.index : -1;
      if (idx < 0 || idx >= state.pendingAdds.length) return state;
      const next = state.pendingAdds.map((row, i) =>
        i === idx ? { ...row, accepted: !row.accepted } : row,
      );
      return { ...state, pendingAdds: next };
    }

    case "APPLY_ACCEPTED_ADDS": {
      // v1.6.3 Stage 2 — commit accepted subset to deckText. Cards
      // are appended one per line in pendingAdds insertion order.
      // Clears pendingAdds. Bumps deckTextRevision + upgrades source
      // via fallback→manual (matches USER_EDIT_DECK_TEXT semantics
      // since this IS a user-initiated deck mutation).
      const accepted = state.pendingAdds.filter((row) => row.accepted === true);
      if (accepted.length === 0) {
        // No-op deck mutation; still clear the queue (user rejected all).
        return { ...state, pendingAdds: [] };
      }
      const appendedLines = accepted.map((row) => `1 ${row.card_name}`).join("\n");
      const sep = state.deckText.endsWith("\n") || state.deckText === "" ? "" : "\n";
      const nextDeckText = `${state.deckText}${sep}${appendedLines}`;
      return {
        ...state,
        deckText: nextDeckText,
        deckTextRevision: state.deckTextRevision + 1,
        source: _upgradeFallbackToManual(state.source),
        // Deck mutation invalidates build / upgrade state (matches v1.1
        // USER_EDIT_DECK_TEXT semantics). Sets isCompleted: true since
        // the user explicitly applied a completion result — the
        // semantically-equivalent signal that v1.6.2 COMPLETE_SUCCESS
        // produced (now replaced by stage-then-apply).
        buildResponse: null,
        isCompleted: true,
        completeError: null,
        upgradeSuggestions: null,
        upgradeError: null,
        pendingAdds: [],
      };
    }

    case "DISMISS_PROPOSED_ADDS": {
      // v1.6.3 Stage 2 — clear the queue without mutating the deck.
      if (state.pendingAdds.length === 0) return state;
      return { ...state, pendingAdds: [] };
    }

    default:
      return state;
  }
}
