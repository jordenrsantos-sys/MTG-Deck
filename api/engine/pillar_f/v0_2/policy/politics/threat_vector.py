"""Threat-vector feature extraction.

Per sub-mega-task B scoping doc section 5. Given (state, viewer_id,
opponent_id), compute the feature vector the LLM uses to decide who
to attack, who to ally with, what to remove.

All component features and the final score are normalized to ~[0, 1]
so the LLM-facing politics_context contract is consistent regardless
of game length, life total convention, or board size.
"""
from __future__ import annotations

from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.state import Card, GameState


THREAT_VECTOR_VERSION = "pillar_f_v0_2_policy_threat_vector_v1"


# Keyword multipliers applied to creature (power + toughness) per
# scoping section 5 — these reflect how much harder a creature is to
# answer, not raw stats.
_KEYWORD_MULTIPLIERS: Dict[str, float] = {
    "deathtouch": 1.20,
    "hexproof": 1.30,
    "lifelink": 1.10,
    "indestructible": 1.25,
    "shroud": 1.25,
    "flying": 1.10,
    "trample": 1.10,
    "menace": 1.05,
    "double strike": 1.20,
    "first strike": 1.10,
}

# Final-score weights per scoping section 5. Tunable; iter-12+ may
# learn these from playtest data.
_SCORE_WEIGHTS: Dict[str, float] = {
    "board_strength": 0.40,
    "tempo": 0.20,
    "life_pressure_inverse": 0.15,   # (1 - life_pressure)
    "recent_aggression": 0.15,
    "archetype_hint": 0.10,
}

# Normalization anchors: a "fully strong" opponent at score 1.0 has
# ~30 weighted P/T on board, ~12 tempo points, has dealt 20+ recent
# damage. Numbers picked from typical Commander mid-late game.
_BOARD_STRENGTH_CAP = 30.0
_TEMPO_CAP = 12.0
_RECENT_AGGRESSION_CAP = 20.0


def _creature_threat_value(card: Card) -> float:
    """One creature's contribution to the opponent's board strength."""
    if not card.is_creature():
        return 0.0
    base = float(card.power_int() + card.toughness_int())
    if base <= 0:
        return 0.0
    multiplier = 1.0
    for kw in card.keywords:
        kw_lower = kw.lower()
        bump = _KEYWORD_MULTIPLIERS.get(kw_lower, 0.0)
        if bump > 0:
            # Multiplicative stacking: each keyword scales the base.
            multiplier *= bump
    return base * multiplier


def _board_strength(state: GameState, player_id: int) -> float:
    """Raw weighted P/T sum, then normalize to [0, 1]."""
    if not (0 <= player_id < len(state.players)):
        return 0.0
    player = state.players[player_id]
    total = 0.0
    for cid in player.zones.battlefield:
        card = state.get_card(cid)
        if card is None:
            continue
        total += _creature_threat_value(card)
    return min(total / _BOARD_STRENGTH_CAP, 1.0)


def _tempo(state: GameState, player_id: int) -> float:
    """Tempo = mana_pool.total() + hand×1.5 + 1.0 if drew this turn.

    Caps at _TEMPO_CAP after normalization."""
    if not (0 <= player_id < len(state.players)):
        return 0.0
    player = state.players[player_id]
    raw = (
        float(player.mana_pool.total())
        + float(len(player.zones.hand)) * 1.5
        + (1.0 if player.cards_drawn_this_turn > 0 else 0.0)
    )
    return min(raw / _TEMPO_CAP, 1.0)


def _life_pressure(state: GameState, player_id: int) -> float:
    """life_total / starting_life (default 40 for Commander).

    Returns float in [0, 1+]; values > 1 mean opponent is gaining
    life (lifegain decks). The final-score formula uses
    (1 - life_pressure) so HIGHER life = lower threat contribution.
    """
    if not (0 <= player_id < len(state.players)):
        return 0.0
    player = state.players[player_id]
    starting = 40.0  # Commander default; iter-12+ reads from state.format
    if starting <= 0:
        return 0.0
    return max(0.0, float(player.life_total) / starting)


def _recent_aggression(
    viewer_state: Any, opponent_id: int,
) -> float:
    """Sum of damage opponent has dealt to viewer in the recent turns
    window. Read from viewer.politics_state['damage_log'][opponent_id].

    `viewer_state` is the viewer's PlayerState (not the GameState).
    """
    if viewer_state is None:
        return 0.0
    damage_log = (
        viewer_state.politics_state.get("damage_log") or {}
    )
    raw_damage_recent = float(damage_log.get(opponent_id, 0))
    return min(raw_damage_recent / _RECENT_AGGRESSION_CAP, 1.0)


def _archetype_hint(state: GameState, opponent_id: int) -> float:
    """Reveal-based archetype heuristic per scoping doc section 5.

    Counts opponent's played-out cards by primary type. Iter-11
    classifies as:
      - aggro: many low-cmc creatures (≥3 creatures with cmc ≤2)
      - control: many instants in graveyard (≥3 instants resolved)
      - ramp: many lands per turn (lands/turn > 1.5)
      - combo: large hand + low life pressure (cards_drawn proxy)

    Each detected archetype contributes a fixed threat-hint bump
    capped at 1.0. Iter-12+ will replace this with Bayesian inference
    on commander identity.
    """
    if not (0 <= opponent_id < len(state.players)):
        return 0.0
    player = state.players[opponent_id]

    low_cmc_creatures = 0
    instants_in_gy = 0
    lands_played_lifetime = 0  # proxy: battlefield lands count
    for cid in player.zones.battlefield:
        card = state.get_card(cid)
        if card is None:
            continue
        if card.is_creature():
            try:
                cmc_int = int(card.cmc or 0)
            except (TypeError, ValueError):
                cmc_int = 0
            if cmc_int <= 2:
                low_cmc_creatures += 1
        if card.is_land():
            lands_played_lifetime += 1
    for cid in player.zones.graveyard:
        card = state.get_card(cid)
        if card is None:
            continue
        if "instant" in (card.type_line or "").lower():
            instants_in_gy += 1

    hint = 0.0
    if low_cmc_creatures >= 3:
        hint += 0.4  # aggro
    if instants_in_gy >= 3:
        hint += 0.4  # control
    # Ramp signal: large battlefield + many lands.
    turn = max(state.turn_number, 1)
    if lands_played_lifetime > 1.5 * turn:
        hint += 0.3  # ramp
    if len(player.zones.hand) >= 7 and player.life_total > 30:
        hint += 0.2  # combo-fishing
    return min(hint, 1.0)


def compute_threat_vector(
    state: GameState, viewer_id: int, opponent_id: int,
) -> Dict[str, float]:
    """Compute the full threat vector for `opponent_id` from `viewer_id`'s
    perspective. Returns a dict with the component features + the
    final aggregate `score` in [0, 1].

    Returns zeros if either player is out of range or has lost.
    """
    if not (0 <= viewer_id < len(state.players)):
        return _zero_vector()
    if not (0 <= opponent_id < len(state.players)):
        return _zero_vector()
    if viewer_id == opponent_id:
        return _zero_vector()
    viewer = state.players[viewer_id]
    opponent = state.players[opponent_id]
    if opponent.has_lost:
        return _zero_vector()

    board = _board_strength(state, opponent_id)
    tempo = _tempo(state, opponent_id)
    life_pressure = _life_pressure(state, opponent_id)
    recent_agg = _recent_aggression(viewer, opponent_id)
    archetype = _archetype_hint(state, opponent_id)

    # Weighted aggregate. life_pressure flipped via (1 - life_pressure)
    # so HIGH life → LOW threat contribution.
    score = (
        board * _SCORE_WEIGHTS["board_strength"]
        + tempo * _SCORE_WEIGHTS["tempo"]
        + (1.0 - min(life_pressure, 1.0)) * _SCORE_WEIGHTS["life_pressure_inverse"]
        + recent_agg * _SCORE_WEIGHTS["recent_aggression"]
        + archetype * _SCORE_WEIGHTS["archetype_hint"]
    )
    score = max(0.0, min(score, 1.0))
    return {
        "score": score,
        "board_strength": board,
        "tempo": tempo,
        "life_pressure": life_pressure,
        "recent_aggression": recent_agg,
        "archetype_hint": archetype,
    }


def compute_all_threat_vectors(
    state: GameState, viewer_id: int,
) -> Dict[int, Dict[str, float]]:
    """Convenience: compute_threat_vector for every opponent of viewer.

    Skips the viewer themselves and eliminated players.
    """
    out: Dict[int, Dict[str, float]] = {}
    for pid in range(len(state.players)):
        if pid == viewer_id:
            continue
        if state.players[pid].has_lost:
            continue
        out[pid] = compute_threat_vector(state, viewer_id, pid)
    return out


def _zero_vector() -> Dict[str, float]:
    return {
        "score": 0.0, "board_strength": 0.0, "tempo": 0.0,
        "life_pressure": 0.0, "recent_aggression": 0.0,
        "archetype_hint": 0.0,
    }
