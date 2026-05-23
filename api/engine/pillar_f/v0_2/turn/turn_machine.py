"""Turn / phase / step state machine per CR 5.1.

Phase 3 of mega-task v9.

Step order (per CR 500.1):
  Beginning phase:    untap → upkeep → draw
  Precombat main:     main_1
  Combat phase:       beginning_of_combat → declare_attackers →
                      declare_blockers → first_strike_damage →
                      combat_damage → end_of_combat
  Postcombat main:    main_2
  Ending phase:       end_step → cleanup

After cleanup, active_player rotates clockwise and the new turn
begins at untap.

Each step has its own priority-open behavior:
  - untap: no priority. Active player untaps permanents + draws no card.
  - cleanup: no priority UNLESS state-based actions trigger something
    pending OR a triggered ability waits. If priority opens during
    cleanup, return to ANOTHER cleanup step (CR 514.3).
  - All other steps: priority opens for the active player first.

This module exposes:
  - `start_step(state, step)` — sets step + fires phase-change triggers
    (e.g., "at beginning of combat").
  - `advance_step(state)` — moves to the next step (or rotates turn).
  - `untap_step(state)` — performs untap actions (untaps active player's
    permanents, clears summoning sickness, empties mana pool).
  - `draw_step(state)` — active player draws a card unless first-turn-skip.
  - `cleanup_step(state)` — discard to 7, clear damage, expire
    until-end-of-turn effects.

The "perform" functions don't open priority; that's the priority loop's
job. The state machine just walks the steps.

Phase 4/5/6 wire in:
  - Phase 4: state-based actions called between every priority pass
    AND at start of each step.
  - Phase 5: continuous effects reapplied after every state change.
  - Phase 6: combat damage steps use this state machine's first_strike
    and combat_damage steps.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    GameState, Phase, Step, ManaPool,
)
from api.engine.pillar_f.v0_2.stack import (
    enqueue_triggers, drain_triggers_to_stack,
)


TURN_MACHINE_VERSION = "pillar_f_v0_2_turn_v1"

# Canonical step order. The state machine walks this list. Cleanup
# loops back when conditions trigger another cleanup pass per CR 514.3.
STEP_ORDER: List[Step] = [
    Step.UNTAP,
    Step.UPKEEP,
    Step.DRAW,
    Step.MAIN_1,
    Step.BEGINNING_OF_COMBAT,
    Step.DECLARE_ATTACKERS,
    Step.DECLARE_BLOCKERS,
    Step.FIRST_STRIKE_DAMAGE,
    Step.COMBAT_DAMAGE,
    Step.END_OF_COMBAT,
    Step.MAIN_2,
    Step.END_STEP,
    Step.CLEANUP,
]

# Map step → phase per CR 5.1.
STEP_TO_PHASE: Dict[Step, Phase] = {
    Step.UNTAP: Phase.BEGINNING,
    Step.UPKEEP: Phase.BEGINNING,
    Step.DRAW: Phase.BEGINNING,
    Step.MAIN_1: Phase.PRECOMBAT_MAIN,
    Step.BEGINNING_OF_COMBAT: Phase.COMBAT,
    Step.DECLARE_ATTACKERS: Phase.COMBAT,
    Step.DECLARE_BLOCKERS: Phase.COMBAT,
    Step.FIRST_STRIKE_DAMAGE: Phase.COMBAT,
    Step.COMBAT_DAMAGE: Phase.COMBAT,
    Step.END_OF_COMBAT: Phase.COMBAT,
    Step.MAIN_2: Phase.POSTCOMBAT_MAIN,
    Step.END_STEP: Phase.ENDING,
    Step.CLEANUP: Phase.ENDING,
}

# Steps that don't open priority (CR 502, 514).
NO_PRIORITY_STEPS = {Step.UNTAP, Step.CLEANUP}


# Step-change trigger callbacks. Keyed by step; called when entering.
# Iter-10 ships a generic "phase_change_trigger" handler that scans
# all permanents for matching trigger conditions in their oracle_text.
# Sub-mega-task A's minimal implementation fires hardcoded triggers
# (e.g., upkeep-trigger cards like Sylvan Library or Yawgmoth's Bargain
# stub registration); iter 11+ wires the LLM-driven oracle compiler.

# Registry: step → list of (source_card_id, trigger_dict) tuples awaiting
# attachment when the step starts. Each trigger fires once per step
# entry.
_STEP_TRIGGERS: Dict[Step, List[Dict[str, Any]]] = {step: [] for step in STEP_ORDER}


def register_step_trigger(step: Step, trigger: Dict[str, Any]) -> None:
    """Register a step-change triggered ability. The trigger fires
    every time the named step begins. Trigger dict shape per the
    enqueue_triggers contract."""
    _STEP_TRIGGERS[step].append(trigger)


def clear_step_triggers(step: Optional[Step] = None) -> None:
    """Clear registered step triggers — used by tests for setup hygiene.
    step=None clears all."""
    if step is None:
        for k in _STEP_TRIGGERS:
            _STEP_TRIGGERS[k] = []
    else:
        _STEP_TRIGGERS[step] = []


def start_step(state: GameState, step: Step) -> None:
    """Set state.step + state.phase. Fire any registered step-change
    triggers. Does NOT open priority — the priority loop's caller
    handles that based on NO_PRIORITY_STEPS membership.
    """
    state.step = step
    state.phase = STEP_TO_PHASE[step]
    # Fire registered step triggers.
    triggers = list(_STEP_TRIGGERS.get(step, []))
    if triggers:
        enqueue_triggers(state, triggers, source_event={
            "type": "StepStartEvent", "step": step.value,
            "active_player": state.active_player,
        })
        # Drain to stack so they're visible when priority opens next.
        drain_triggers_to_stack(state)


def step_opens_priority(step: Step) -> bool:
    """Returns False for untap + cleanup unless something pending
    forces a priority window. Iter-10's contract: caller checks this
    flag AFTER start_step to decide whether to run a priority round."""
    return step not in NO_PRIORITY_STEPS


def untap_step(state: GameState) -> None:
    """CR 502: active player's permanents untap, summoning sickness
    clears for permanents they've controlled since their previous turn,
    mana pool empties, per-turn counters reset. No priority opens."""
    active = state.active_player
    if not (0 <= active < len(state.players)):
        return
    player = state.players[active]
    # Untap all permanents the active player controls.
    for cid in list(player.zones.battlefield):
        card = state.get_card(cid)
        if card is None:
            continue
        # CR 502.1 — "phasing happens first" — iter 10 skips phasing.
        # Untap step proper: untap all permanents the active player
        # controls, unless an effect says otherwise (Static Orb,
        # don't-untap-this-turn). Iter 10 ships the default untap-all
        # behavior; effects-driven exclusions ship in Phase 4.
        card.tapped = False
    # Clear summoning sickness for active player's permanents.
    for cid in player.zones.battlefield:
        card = state.get_card(cid)
        if card is None:
            continue
        if card.controller == active:
            card.summoning_sick = False
    # Empty all players' mana pools (CR 106.4 — happens at end of each
    # phase; untap is the start of beginning phase so prior pool is gone).
    for p in state.players:
        p.mana_pool.empty()
    # Reset per-turn counters for active player only.
    player.lands_played_this_turn = 0
    player.cards_drawn_this_turn = 0
    player.spells_cast_this_turn = 0


def draw_step(state: GameState, *, skip_first_turn_draw: bool = True) -> None:
    """CR 504: active player draws a card. First turn of a multiplayer
    game, the starting player skips this draw per common EDH convention
    (when skip_first_turn_draw=True AND turn_number==1 AND active_player
    is the starting player at index 0)."""
    if skip_first_turn_draw and state.turn_number == 1 and state.active_player == 0:
        return
    active = state.active_player
    if not (0 <= active < len(state.players)):
        return
    player = state.players[active]
    if not player.zones.library:
        # Draw from empty library — SBA flag (CR 704.5b cascades to loss).
        player.has_drawn_from_empty_library = True
        return
    cid = player.zones.library.pop(0)  # top
    player.zones.hand.append(cid)
    player.cards_drawn_this_turn += 1


def cleanup_step(state: GameState) -> bool:
    """CR 514: active player discards down to maximum hand size (7),
    all damage marked on permanents is removed, "until end of turn"
    effects expire. Returns True if a re-entry to cleanup is needed
    (SBAs or triggered abilities fired during cleanup → another cleanup
    step opens with priority per CR 514.3).

    Iter-10 simplification: discard-to-7 picks the LAST cards in hand
    (insertion order = bottom-of-stack). Sub-mega-task B will plug an
    LLM-driven choice.

    "Until end of turn" effects: iter 10 walks
    state.continuous_effects + removes those tagged with
    `target_pattern["until_end_of_turn"] = True`. Phase 5 fully wires
    the layered effects; iter 10's cleanup just expires them.
    """
    active = state.active_player
    if not (0 <= active < len(state.players)):
        return False
    player = state.players[active]
    # Discard down to 7.
    max_hand = 7
    while len(player.zones.hand) > max_hand:
        discarded = player.zones.hand.pop()  # last = "bottom"
        player.zones.graveyard.append(discarded)
    # Clear damage marks on all permanents.
    for p in state.players:
        for cid in p.zones.battlefield:
            card = state.get_card(cid)
            if card is not None:
                card.damage_marked = 0
    # Expire "until end of turn" continuous effects.
    state.continuous_effects = [
        ce for ce in state.continuous_effects
        if not ce.target_pattern.get("until_end_of_turn")
    ]
    # Empty mana pools (CR 106.4 — happens at end of phase).
    for p in state.players:
        p.mana_pool.empty()
    # Returns True if any triggers got enqueued during cleanup —
    # iter 10's minimal version always returns False (no triggers
    # fire in cleanup yet). Phase 4 wires this when SBAs trigger.
    return False


def advance_step(state: GameState) -> Step:
    """Move to the next step in STEP_ORDER. Returns the new step.
    If current step is the last (CLEANUP), advance to next player's
    UNTAP + increment turn_number + rotate active_player clockwise.
    """
    try:
        idx = STEP_ORDER.index(state.step)
    except ValueError:
        idx = -1
    if idx < 0 or idx >= len(STEP_ORDER) - 1:
        # End of turn — rotate.
        next_player = (state.active_player + 1) % len(state.players)
        # Skip eliminated players.
        n = len(state.players)
        for _ in range(n):
            if not state.players[next_player].has_lost:
                break
            next_player = (next_player + 1) % n
        state.active_player = next_player
        # Increment turn if we've wrapped back to player 0.
        if next_player == 0:
            state.turn_number += 1
        new_step = Step.UNTAP
    else:
        new_step = STEP_ORDER[idx + 1]
    start_step(state, new_step)
    return new_step


def run_turn(
    state: GameState,
    *,
    priority_runner: Optional[Callable[[GameState], None]] = None,
    max_steps: int = 50,
) -> List[Step]:
    """Walk one full turn of the active player. Returns list of steps
    visited in order. `priority_runner` is a callable that takes the
    current state and (if appropriate per step_opens_priority) runs a
    priority round + resolves stack to completion. Iter-10 default:
    no priority runner (just walks steps).

    Combat substeps (declare_attackers, declare_blockers, first_strike,
    combat_damage) call into Phase 6's combat module via the priority
    runner. For Phase 3 unit tests, the runner is a no-op.
    """
    visited: List[Step] = []
    # Start: assume state.step is set to UNTAP by caller before turn 1.
    # For subsequent turns, advance_step from CLEANUP wraps to next
    # player's UNTAP.
    # Run perform fn for the current step.
    safety = max_steps
    started_at_step = state.step
    started_at_player = state.active_player
    started_at_turn = state.turn_number
    # Run the step we're entering first.
    start_step(state, started_at_step)
    while safety > 0:
        safety -= 1
        visited.append(state.step)
        # Perform step's action.
        if state.step == Step.UNTAP:
            untap_step(state)
        elif state.step == Step.DRAW:
            draw_step(state)
        elif state.step == Step.CLEANUP:
            re_enter = cleanup_step(state)
            # CR 514.3: if cleanup triggers fire OR SBAs hit during
            # cleanup, another cleanup step occurs. Iter-10's
            # cleanup_step returns False; Phase 4 will return True
            # when SBAs fire.
            if re_enter:
                start_step(state, Step.CLEANUP)
                continue
        # Priority opens for non-untap, non-cleanup steps.
        if step_opens_priority(state.step) and priority_runner is not None:
            priority_runner(state)
        # Check for turn end.
        if state.step == Step.CLEANUP:
            # Rotate to next player + break.
            advance_step(state)
            return visited
        # Otherwise advance to next step.
        advance_step(state)
    return visited
