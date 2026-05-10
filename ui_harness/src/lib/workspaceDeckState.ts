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
}

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

    case "RESET": {
      return INITIAL_STATE;
    }

    default:
      return state;
  }
}
