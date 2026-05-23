"""Stack mechanics + priority loop + counterspell + APNAP trigger ordering.

Phase 2 of mega-task v9.

The stack is a LIFO list of StackEntry objects living in
GameState.stack. Phase 4's replacement-effect system runs during stack
resolution; Phase 5's continuous-effect system reapplies after every
state mutation. Both are dependencies pulled in by their respective phases.

For Phase 2 (this module), spell/ability resolution is delegated to a
`resolve_fn` callable registered on each StackEntry's metadata. Iter-10
ships a minimal resolver registry (3 generic resolvers: noop, deal-damage,
draw-cards) sufficient for the Phase 2 unit tests + the Phase 8 fixture
suite's basic stack scenarios. The full per-card oracle compilation
pipeline is iter 11+ scope per the kickoff.

Priority loop is callback-driven for sub-mega-task B (LLM policy) plumbing:
the engine calls `priority_response_fn(state, player_id, stack)` for each
player when priority is offered. Iter-10 ships a mock responder that
always passes, which is sufficient to exercise the loop's flow control
in unit tests.
"""
from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    GameState, StackEntry,
)


STACK_VERSION = "pillar_f_v0_2_stack_v1"

# Type alias for the priority responder. A responder returns either
# None (player passes priority) or a StackEntry-shape dict to push onto
# the stack (player takes an action: cast spell, activate ability).
# Sub-mega-task B will plug an LLM-driven responder here; iter 10 ships
# a mock_pass_responder that returns None for all players.
PriorityResponderFn = Callable[[GameState, int], Optional[Dict[str, Any]]]


def _new_stack_entry_id() -> str:
    """Generate a unique stack-entry id. UUID4 hex 8 chars."""
    return uuid.uuid4().hex[:8]


def push_to_stack(
    state: GameState,
    *,
    card_id: Optional[str],
    controller: int,
    entry_type: str = "spell",
    targets: Optional[List[Any]] = None,
    payment: Optional[Dict[str, Any]] = None,
    description: str = "",
    entry_id: Optional[str] = None,
) -> StackEntry:
    """Push a new StackEntry. Returns the constructed entry.

    Pushing onto the stack invalidates the current priority round —
    when a player takes an action, priority restarts at the active
    player and propagates clockwise again per CR 117.3.

    Iter-10 simplification: spell-cost payment, target legality, and
    cast-from-zone semantics are caller's responsibility. Iter 11+
    plumbs cost payment + LLM-driven target selection.
    """
    entry = StackEntry(
        entry_id=entry_id or _new_stack_entry_id(),
        card_id=card_id,
        controller=controller,
        entry_type=entry_type,
        targets=list(targets or []),
        payment=dict(payment or {}),
        description=description,
    )
    state.stack.append(entry)
    # Stack mutation resets the priority round.
    state.priority_passes_this_round = set()
    return entry


def pop_top(state: GameState) -> Optional[StackEntry]:
    """Pop and return the top of the stack (LIFO), or None if empty."""
    if not state.stack:
        return None
    return state.stack.pop()


def peek_top(state: GameState) -> Optional[StackEntry]:
    return state.stack[-1] if state.stack else None


def counter_target(state: GameState, target_entry_id: str) -> bool:
    """Counter the stack entry with the given entry_id: remove it from
    the stack without resolving. Returns True if found + removed.

    Specific counterspell card implementations call this from their own
    resolve(). The counterspell's stack entry itself resolves normally
    AFTER its target is removed.

    Cards with "can't be countered" register a replacement effect (Phase
    4) that filters their entry_id from this call's effect — iter 10's
    minimal implementation skips the can't-be-countered check; iter 11+
    plumbs the full replacement-pattern matching.
    """
    for i, e in enumerate(state.stack):
        if e.entry_id == target_entry_id:
            state.stack.pop(i)
            state.priority_passes_this_round = set()
            return True
    return False


# ============================================================
# Resolution registry
# ============================================================

# Resolution registry. Maps a resolver_name string (passed in
# StackEntry.payment["resolver"]) to a callable
# `(state, entry) -> Optional[List[Dict]]` that mutates state and
# returns a list of new triggered abilities to enqueue (or None).
_RESOLVERS: Dict[str, Callable[[GameState, StackEntry], Optional[List[Dict[str, Any]]]]] = {}


def register_resolver(name: str, fn: Callable[[GameState, StackEntry], Optional[List[Dict[str, Any]]]]) -> None:
    """Register a resolver function under a string name. The Card or
    ability description specifies its resolver via
    StackEntry.payment['resolver'] = name."""
    _RESOLVERS[name] = fn


def get_resolver(name: str) -> Optional[Callable[..., Any]]:
    return _RESOLVERS.get(name)


def _resolver_noop(state: GameState, entry: StackEntry) -> None:
    """Default resolver — does nothing. Used by simple sorcery/instant
    stubs in unit tests where the resolution mechanics are out of scope."""
    return None


def _resolver_deal_damage_to_player(state: GameState, entry: StackEntry) -> None:
    """Deals `amount` damage to `targets[0]` (a player_id).
    payment = {"resolver": "deal_damage_to_player", "amount": int}.
    Iter-10 short-circuits: no replacement-effect chain (Phase 4 wires it)."""
    amount = int(entry.payment.get("amount", 0) or 0)
    if not entry.targets or amount <= 0:
        return None
    target_pid = entry.targets[0]
    if isinstance(target_pid, int) and 0 <= target_pid < len(state.players):
        state.players[target_pid].life_total -= amount
    return None


def _resolver_draw_cards(state: GameState, entry: StackEntry) -> None:
    """Controller draws `amount` cards. payment = {"resolver":
    "draw_cards", "amount": int}. Iter-10 implements as a simple
    library-pop; the full DrawEvent + replacement-effect chain ships in
    Phase 4."""
    amount = int(entry.payment.get("amount", 0) or 0)
    if amount <= 0:
        return None
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    player = state.players[pid]
    for _ in range(amount):
        if not player.zones.library:
            # Iter-10 stub: draw from empty library flags SBA loss; the
            # full SBA cascade ships in Phase 4.
            player.has_drawn_from_empty_library = True
            break
        card_id = player.zones.library.pop(0)  # top of library = index 0
        player.zones.hand.append(card_id)
        player.cards_drawn_this_turn += 1
    return None


# Register the iter-10 minimal resolvers.
register_resolver("noop", _resolver_noop)
register_resolver("deal_damage_to_player", _resolver_deal_damage_to_player)
register_resolver("draw_cards", _resolver_draw_cards)


def resolve_top(state: GameState) -> Optional[StackEntry]:
    """Pop the top entry + invoke its resolver. Returns the resolved
    entry, or None if stack was empty. Triggers + replacements wire in
    via Phase 4."""
    entry = pop_top(state)
    if entry is None:
        return None
    resolver_name = entry.payment.get("resolver") or "noop"
    fn = get_resolver(resolver_name)
    if fn is not None:
        fn(state, entry)
    return entry


# ============================================================
# Priority loop
# ============================================================


def _next_player_clockwise(pid: int, num_players: int) -> int:
    """Iter-10: clockwise = increasing player_id wraps to 0."""
    return (pid + 1) % num_players


def apnap_order(state: GameState) -> List[int]:
    """Returns players in APNAP order starting from active_player.
    Skips eliminated players (PlayerState.has_lost=True)."""
    order: List[int] = []
    n = len(state.players)
    if n == 0:
        return order
    start = state.active_player
    for i in range(n):
        pid = (start + i) % n
        if not state.players[pid].has_lost:
            order.append(pid)
    return order


def priority_round(
    state: GameState,
    responder_fn: PriorityResponderFn,
    *,
    max_responses_per_round: int = 100,
) -> int:
    """Run one priority round per CR 117. Each surviving player in
    APNAP order gets priority. A response (non-None return from
    responder_fn) pushes onto the stack and resets the round to start
    at the active player. When all players pass in succession without
    a response, the round exits.

    Returns the number of responses made during this round (0 = fully
    passed = ready to resolve top of stack or advance step).

    Iter-10's responder contract: returns either
        None (pass priority)
    OR  Dict that the engine treats as a push_to_stack kwargs payload,
        e.g. {"card_id": "...", "entry_type": "activated",
              "controller": pid, "targets": [...], "payment": {...},
              "description": "..."}.

    Sub-mega-task B will plug an LLM-driven responder in iter 11.
    """
    state.priority_passes_this_round = set()
    responses = 0
    safety = max_responses_per_round
    while safety > 0:
        safety -= 1
        order = apnap_order(state)
        if not order:
            return responses
        # Find the player who hasn't passed yet (iterating from active).
        next_player_id: Optional[int] = None
        for pid in order:
            if pid not in state.priority_passes_this_round:
                next_player_id = pid
                break
        if next_player_id is None:
            # All surviving players have passed in succession — round complete.
            state.priority_holder = None
            return responses
        # Offer priority.
        state.priority_holder = next_player_id
        try:
            action = responder_fn(state, next_player_id)
        except Exception:
            action = None
        if action is None:
            # Pass.
            state.priority_passes_this_round.add(next_player_id)
            continue
        # Took an action — push to stack + reset round.
        push_to_stack(
            state,
            card_id=action.get("card_id"),
            controller=int(action.get("controller", next_player_id)),
            entry_type=action.get("entry_type", "spell"),
            targets=action.get("targets") or [],
            payment=action.get("payment") or {},
            description=action.get("description", ""),
            entry_id=action.get("entry_id"),
        )
        responses += 1
    return responses


def run_stack_to_resolution(
    state: GameState,
    responder_fn: PriorityResponderFn,
    *,
    max_iterations: int = 200,
) -> List[StackEntry]:
    """Top-level helper: runs priority round + resolves top + repeats
    until the stack is empty AND all players have passed. Returns the
    list of entries resolved in order."""
    resolved: List[StackEntry] = []
    safety = max_iterations
    while safety > 0:
        safety -= 1
        priority_round(state, responder_fn)
        if not state.stack:
            return resolved
        entry = resolve_top(state)
        if entry is not None:
            resolved.append(entry)
        # After a resolution, priority returns to the active player
        # (handled by next priority_round call resetting passes).
    return resolved


# ============================================================
# Trigger queue (APNAP ordering)
# ============================================================


def enqueue_triggers(
    state: GameState,
    triggers: List[Dict[str, Any]],
    source_event: Optional[Dict[str, Any]] = None,
) -> None:
    """Add triggered abilities to state.delayed_triggers_pending. Each
    trigger dict shape:
        {"source_card_id": str, "controller": int,
         "resolver": str, "payment": {...}, "description": str,
         "source_event": {...} or None}

    The actual placement onto the stack happens at the next priority
    open (CR 603.3). Order: APNAP, with same-controller stacking by
    controller's choice — iter 10 collapses to insertion order for
    same-controller (which matches "controller picks order" as long as
    triggers are added in the controller's preferred order).
    """
    if not triggers:
        return
    enriched = []
    for t in triggers:
        d = dict(t)
        if source_event is not None and "source_event" not in d:
            d["source_event"] = dict(source_event)
        enriched.append(d)
    state.delayed_triggers_pending.extend(enriched)


def drain_triggers_to_stack(state: GameState) -> int:
    """Move all pending triggered abilities from delayed_triggers_pending
    onto the stack in APNAP order. Same-controller triggers preserve
    insertion order (= controller's choice in iter 10).

    Returns the number of triggers placed on the stack. Called by the
    turn machine + priority loop at appropriate boundaries (CR 603.3:
    triggers go on stack before priority opens again).
    """
    if not state.delayed_triggers_pending:
        return 0
    pending = state.delayed_triggers_pending
    state.delayed_triggers_pending = []
    # Bucket by controller.
    by_controller: Dict[int, List[Dict[str, Any]]] = {}
    for t in pending:
        pid = int(t.get("controller", state.active_player))
        by_controller.setdefault(pid, []).append(t)
    # Push in APNAP order.
    pushed = 0
    for pid in apnap_order(state):
        for t in by_controller.get(pid, []):
            push_to_stack(
                state,
                card_id=t.get("source_card_id"),
                controller=pid,
                entry_type="triggered",
                targets=t.get("targets") or [],
                payment={"resolver": t.get("resolver", "noop"),
                         **(t.get("payment") or {})},
                description=t.get("description", "Triggered ability"),
            )
            pushed += 1
    return pushed
