/**
 * usePlaytest — Phase 4.9 Stage 2.
 *
 * React hook wrapping the goldfishState reducer with useReducer + global
 * keyboard listeners. Hotkeys per spec: D=draw, M=mulligan, U=untap-all,
 * E=advance-turn, S=shuffle, R=reset, Esc=close-modal (handled via
 * `onEscape` callback caller passes in).
 *
 * Per HARD safety #11 (reducer purity): all state changes go through the
 * pure reducer. The hook owns side-effects (event listeners, dev logging)
 * and never bypasses the reducer.
 *
 * Per autonomous_repair_log soft-safety #10 (hotkey listener leaks):
 * `useEffect` cleanup pairs every `addEventListener` with
 * `removeEventListener`. Listeners ignore events when an `<input>` /
 * `<textarea>` / contentEditable element is focused so typing in form
 * fields doesn't accidentally trigger hotkeys.
 */
import { useCallback, useEffect, useReducer } from "react";
import { reducer, INITIAL_STATE, type PlayAction, type PlayState } from "./goldfishState";
import { devLog } from "./devLog";

export type UsePlaytestOptions = {
  /** Disable hotkey listeners entirely (e.g., when GoldfishView is unmounted but hook persists). */
  hotkeysEnabled?: boolean;
  /** Optional Esc handler — hook does NOT dispatch on Esc, just calls this. */
  onEscape?: () => void;
};

const SHOULD_IGNORE_TARGET_TAGS = new Set(["INPUT", "TEXTAREA", "SELECT"]);

function _shouldIgnoreEvent(target: EventTarget | null): boolean {
  if (!target || !(target instanceof HTMLElement)) return false;
  if (SHOULD_IGNORE_TARGET_TAGS.has(target.tagName)) return true;
  if (target.isContentEditable) return true;
  return false;
}

export function usePlaytest(opts: UsePlaytestOptions = {}): {
  state: PlayState;
  dispatch: (action: PlayAction) => void;
} {
  const { hotkeysEnabled = true, onEscape } = opts;
  const [state, baseDispatch] = useReducer(reducer, INITIAL_STATE);

  const dispatch = useCallback(
    (action: PlayAction) => {
      devLog("[goldfish dispatch]", action.type);
      baseDispatch(action);
    },
    [baseDispatch],
  );

  useEffect(() => {
    if (!hotkeysEnabled) return;
    function handleKey(ev: KeyboardEvent) {
      if (_shouldIgnoreEvent(ev.target)) return;
      if (ev.metaKey || ev.ctrlKey || ev.altKey) return;
      switch (ev.key.toLowerCase()) {
        case "d":
          ev.preventDefault();
          dispatch({ type: "DRAW", count: 1 });
          break;
        case "m":
          ev.preventDefault();
          dispatch({ type: "MULLIGAN" });
          break;
        case "u":
          ev.preventDefault();
          dispatch({ type: "UNTAP_ALL" });
          break;
        case "e":
          ev.preventDefault();
          dispatch({ type: "ADVANCE_TURN" });
          break;
        case "s":
          ev.preventDefault();
          dispatch({ type: "SHUFFLE_LIBRARY" });
          break;
        case "r":
          ev.preventDefault();
          dispatch({ type: "RESET_GAME" });
          break;
        case "escape":
          if (onEscape) {
            ev.preventDefault();
            onEscape();
          }
          break;
        default:
          break;
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => {
      window.removeEventListener("keydown", handleKey);
    };
  }, [dispatch, hotkeysEnabled, onEscape]);

  return { state, dispatch };
}
