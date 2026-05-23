"""London mulligan + game-start setup.

Phase 7 of mega-task v9.

London mulligan per current Magic comprehensive rules (CR 103.4):
  1. Each player draws 7 cards.
  2. Starting with the player to choose, each player may take a mulligan.
  3. To take a mulligan, shuffle hand back into library, draw 7 again.
  4. After choosing to keep, put N cards on the bottom of the library
     (where N = number of mulligans taken). Player chooses which cards.

Iter-10 contract: a `mulligan_decider_fn` callback determines whether a
given player keeps or mulligans. Mock iter-10 decider always keeps after
1 mulligan attempt; sub-mega-task B will plug an LLM-driven decider in
iter 11.

`shuffle_library` uses Python's `random.shuffle` with a seeded RNG to
support deterministic test fixtures + reproducible Stage-2 playtest
runs in iter 12+.
"""
from __future__ import annotations

import random
from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    GameState, PlayerState,
)


MULLIGAN_VERSION = "pillar_f_v0_2_mulligan_v1"

# Decider signature: (state, player_id, current_hand_card_ids, num_mulligans_taken) → bool
# Returns True to mulligan (shuffle hand back + draw new 7); False to keep.
MulliganDeciderFn = Callable[[GameState, int, List[str], int], bool]

# Bottom-picker signature: (state, player_id, hand_card_ids, n_to_put_on_bottom) → list of card_ids
# Returns the card_ids the player chooses to put on the bottom of library
# (in bottom-to-top order). Length must match n_to_put_on_bottom.
BottomPickerFn = Callable[[GameState, int, List[str], int], List[str]]


def always_keep_decider(
    state: GameState, player_id: int,
    current_hand: List[str], num_mulligans: int,
) -> bool:
    """Default decider: keep every hand (no mulligans). Tests can pass
    a different decider via the mulligan_decider_fn parameter."""
    return False


def keep_after_n_mulligans_decider(n: int) -> MulliganDeciderFn:
    """Returns a decider that takes exactly N mulligans then keeps.
    Useful for testing the put-N-on-bottom behavior."""
    def _decider(state, player_id, current_hand, num_mulligans):
        return num_mulligans < n
    return _decider


def default_bottom_picker(
    state: GameState, player_id: int,
    hand: List[str], n_to_put_on_bottom: int,
) -> List[str]:
    """Default: put the LAST N cards (insertion-order = LLM doesn't
    care). Tests override via bottom_picker_fn parameter."""
    if n_to_put_on_bottom <= 0:
        return []
    return list(hand[-n_to_put_on_bottom:])


def shuffle_library(
    state: GameState, player_id: int, *, seed: Optional[int] = None,
) -> None:
    """Shuffle the player's library. Seedable for deterministic tests
    + reproducible Stage-2 playtest runs."""
    if not (0 <= player_id < len(state.players)):
        return
    lib = state.players[player_id].zones.library
    rng = random.Random(seed) if seed is not None else random.Random()
    rng.shuffle(lib)


def draw_n(state: GameState, player_id: int, n: int) -> List[str]:
    """Draw N cards from the top of player_id's library into their hand.
    Returns the list of card_ids drawn. Stops at empty library (sets
    has_drawn_from_empty_library SBA flag)."""
    if not (0 <= player_id < len(state.players)):
        return []
    player = state.players[player_id]
    drawn: List[str] = []
    for _ in range(n):
        if not player.zones.library:
            player.has_drawn_from_empty_library = True
            break
        cid = player.zones.library.pop(0)  # top
        player.zones.hand.append(cid)
        drawn.append(cid)
    return drawn


def opening_hand_size(num_mulligans: int) -> int:
    """London mulligan: hand size is always 7 — N cards go to bottom
    of library AFTER drawing 7 (CR 103.4d)."""
    return 7


def mulligan_setup(
    state: GameState,
    *,
    decider_fn: MulliganDeciderFn = always_keep_decider,
    bottom_picker_fn: BottomPickerFn = default_bottom_picker,
    seed_per_player: Optional[Dict[int, int]] = None,
    max_mulligans: int = 7,
) -> Dict[int, int]:
    """Run the mulligan phase for all players. After mulligans are
    chosen + cards placed on bottom, the game is ready to start turn 1
    with active_player = 0 untap step.

    Returns {player_id → num_mulligans_taken}.

    Sub-mega-task B will plug an LLM-driven decider_fn; iter-10 uses
    `always_keep_decider` by default + `default_bottom_picker`.

    Iter-10 simplification: skips the "first to mulligan vs later
    mulligan" turn order optimization (CR 103.4 says players decide
    in turn order; iter-10 processes players in player_id order).
    """
    seed_per_player = seed_per_player or {}
    results: Dict[int, int] = {}
    for ps in state.players:
        pid = ps.player_id
        # Shuffle initial library.
        shuffle_library(state, pid, seed=seed_per_player.get(pid))
        # Draw 7.
        draw_n(state, pid, opening_hand_size(0))
        num_mulligans = 0
        # Mulligan loop.
        while num_mulligans < max_mulligans:
            keep_mulligan = decider_fn(state, pid, list(ps.zones.hand), num_mulligans)
            if not keep_mulligan:
                break
            # Shuffle hand back into library.
            ps.zones.library.extend(ps.zones.hand)
            ps.zones.hand.clear()
            ps.has_drawn_from_empty_library = False  # reset
            shuffle_library(state, pid, seed=seed_per_player.get(pid))
            draw_n(state, pid, opening_hand_size(num_mulligans + 1))
            num_mulligans += 1
        # After choosing to keep: put `num_mulligans` cards on bottom.
        if num_mulligans > 0:
            n_to_bottom = min(num_mulligans, len(ps.zones.hand))
            picked = bottom_picker_fn(state, pid, list(ps.zones.hand), n_to_bottom)
            # Remove picked cards from hand + put on bottom of library
            # (end of library list = bottom).
            for cid in picked:
                if cid in ps.zones.hand:
                    ps.zones.hand.remove(cid)
                    ps.zones.library.append(cid)
        results[pid] = num_mulligans
    return results
