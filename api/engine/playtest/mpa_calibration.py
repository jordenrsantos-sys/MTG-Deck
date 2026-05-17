"""
mpa_calibration — Phase 5b.2 (anti-bias seat-swap) + 5b.9 (strength oracle
Measurement B wire-in stub).

Provides:
  - `run_match_anti_bias(deck_a, deck_b, n_games)` — N-game match with mirrored
    seat assignment per DESIGN_DECISIONS.md rule 1.4. Returns win-rate of A
    aggregated across both seat orderings.
  - `compute_measurement_b(deck, opposition_decks, n_games_per)` — Run the
    candidate against a panel of opposition decks, return aggregate win-rate
    and per-opposition breakdown. This is the function /deck/strength_check_v1
    consults when include_measurement_b=True.

IMPORTANT CALIBRATION GATE (DESIGN_DECISIONS rule 1.4):
  The MPA in this ship is a heuristic skeleton (Phase 5b.1). It does NOT yet
  pass the bracket-4-vs-precon calibration suite (Phase 5b.3). Therefore:
    - Measurement B values returned here are MARKED as "uncalibrated".
    - The /deck/strength_check_v1 response wraps them with a
      `calibration_status: "uncalibrated"` flag so consumers know not to
      treat the win-rates as strength claims yet.
    - Calibration unlocks when 5b.3 (benchmark suite) passes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .mpa_game_state import new_commander_game
from .mpa_runner import run_game


CALIBRATION_VERSION = "mpa_calibration_v0.2_anti_bias_seat_swap"


@dataclass
class MatchResult:
    deck_a_wins: int = 0
    deck_b_wins: int = 0
    draws: int = 0
    games_played: int = 0
    avg_game_length_turns: float = 0.0
    seat_swap_pairs: int = 0  # how many mirrored pairs were run
    decision_quality_a: float = 0.0  # placeholder until 5b.2 expands
    decision_quality_b: float = 0.0
    games_log: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def deck_a_winrate(self) -> float:
        return self.deck_a_wins / max(1, self.games_played)


def run_match_anti_bias(
    *,
    deck_a_oracle_tuples: List[Tuple[str, str, str]],
    deck_b_oracle_tuples: List[Tuple[str, str, str]],
    commander_a: Tuple[str, str, str],
    commander_b: Tuple[str, str, str],
    n_game_pairs: int = 5,
    max_turns_per_game: int = 30,
    seed_base: int = 100,
) -> MatchResult:
    """Run N pairs of mirrored games per DESIGN_DECISIONS 1.4 anti-bias rule.

    Each pair: game 1 = deck_a at seat 0, deck_b at seat 1. Game 2 = swap.
    This controls first-player advantage. Same RNG seed family per pair so
    starting hands are mirror-able if/when policy becomes seat-symmetric in
    its hand evaluation.

    Returns aggregate win counts + per-game log.
    """
    result = MatchResult()
    for pair_idx in range(max(1, int(n_game_pairs))):
        # Game 1: A at seat 0, B at seat 1
        state1 = new_commander_game(
            deck_a_oracle_tuples, deck_b_oracle_tuples,
            commander_a, commander_b,
            seed=seed_base + pair_idx * 2,
        )
        _hydrate_dummy_costs(state1)
        g1 = run_game(state1, max_turns=max_turns_per_game)
        _tally_game(result, g1, deck_a_seat=0, pair_idx=pair_idx, swap_seat=False)

        # Game 2: B at seat 0, A at seat 1 (seat swap)
        state2 = new_commander_game(
            deck_b_oracle_tuples, deck_a_oracle_tuples,
            commander_b, commander_a,
            seed=seed_base + pair_idx * 2 + 1,
        )
        _hydrate_dummy_costs(state2)
        g2 = run_game(state2, max_turns=max_turns_per_game)
        _tally_game(result, g2, deck_a_seat=1, pair_idx=pair_idx, swap_seat=True)

        result.seat_swap_pairs += 1

    if result.games_played > 0:
        avg = sum(g.get("turns", 0) for g in result.games_log) / result.games_played
        result.avg_game_length_turns = round(avg, 2)
    return result


def compute_measurement_b_hydrated(
    *,
    db_snapshot_id: str,
    candidate_commander_name: str,
    candidate_deck_names: List[str],
    opposition_panel: List[Dict[str, Any]],  # each {commander_name, deck_names, label, bracket}
    n_game_pairs_per_opponent: int = 3,
    max_turns_per_game: int = 30,
) -> Dict[str, Any]:
    """Hydrated variant — takes card names + snapshot, fetches full card data
    from cards table so the MPA has mana_cost/power/toughness/primitives to
    work with. This is the path /deck/strength_check_v1 should use for real
    decks (vs the stub-tuple compute_measurement_b below)."""
    from .mpa_card_hydration import new_commander_game_hydrated
    if not opposition_panel:
        return {
            "kind": "playtest_winrate",
            "calibration_status": "uncalibrated",
            "overall_winrate": None,
            "games_total": 0,
            "per_opponent": [],
            "notes": "No opposition panel provided.",
        }

    total_wins = 0
    total_games = 0
    per_opp: List[Dict[str, Any]] = []

    for opp in opposition_panel:
        label = opp.get("label", "opp")
        opp_deck_names = opp.get("deck_names") or []
        opp_cmdr_name = opp.get("commander_name") or "Opponent"
        match = MatchResult()
        for pair_idx in range(max(1, int(n_game_pairs_per_opponent))):
            for swap in (False, True):
                if swap:
                    s = new_commander_game_hydrated(
                        db_snapshot_id=db_snapshot_id,
                        commander_a_name=opp_cmdr_name,
                        deck_a_names=opp_deck_names,
                        commander_b_name=candidate_commander_name,
                        deck_b_names=candidate_deck_names,
                        seed=300 + pair_idx * 2 + 1,
                    )
                    candidate_seat = 1
                else:
                    s = new_commander_game_hydrated(
                        db_snapshot_id=db_snapshot_id,
                        commander_a_name=candidate_commander_name,
                        deck_a_names=candidate_deck_names,
                        commander_b_name=opp_cmdr_name,
                        deck_b_names=opp_deck_names,
                        seed=300 + pair_idx * 2,
                    )
                    candidate_seat = 0
                g = run_game(s, max_turns=max_turns_per_game)
                _tally_game(match, g, deck_a_seat=candidate_seat, pair_idx=pair_idx, swap_seat=swap)
                match.seat_swap_pairs = pair_idx + 1
        total_wins += match.deck_a_wins
        total_games += match.games_played
        if match.games_played > 0:
            match.avg_game_length_turns = round(
                sum(g.get("turns", 0) for g in match.games_log) / match.games_played, 2
            )
        per_opp.append({
            "label": label,
            "bracket": opp.get("bracket"),
            "games": match.games_played,
            "wins": match.deck_a_wins,
            "draws": match.draws,
            "winrate": round(match.deck_a_winrate, 4),
            "avg_game_length_turns": match.avg_game_length_turns,
        })

    overall = total_wins / total_games if total_games > 0 else None
    return {
        "kind": "playtest_winrate",
        "calibration_status": "uncalibrated",
        "overall_winrate": round(overall, 4) if overall is not None else None,
        "games_total": total_games,
        "per_opponent": per_opp,
        "notes": (
            "Win-rates are uncalibrated — pilot is heuristic skeleton "
            "(no blocks, no instant-speed, no priority for triggered abilities, "
            "no LLM consult for hard decisions). Phase 5b.3 benchmark must pass "
            "before these numbers can ground strength claims."
        ),
    }


def compute_measurement_b(
    *,
    candidate_deck_oracle_tuples: List[Tuple[str, str, str]],
    candidate_commander: Tuple[str, str, str],
    opposition_panel: List[Dict[str, Any]],  # each {commander, deck, label, bracket}
    n_game_pairs_per_opponent: int = 3,
    max_turns_per_game: int = 30,
) -> Dict[str, Any]:
    """Run the candidate deck against each opposition deck, aggregate win-rate.

    Returns Measurement B payload for /deck/strength_check_v1:
      {
        "kind": "playtest_winrate",
        "calibration_status": "uncalibrated",  # until 5b.3 benchmark passes
        "overall_winrate": float,
        "games_total": int,
        "per_opponent": [{label, winrate, games, ...}, ...],
        "notes": "..."
      }
    """
    if not opposition_panel:
        return {
            "kind": "playtest_winrate",
            "calibration_status": "uncalibrated",
            "overall_winrate": None,
            "games_total": 0,
            "per_opponent": [],
            "notes": "No opposition panel provided.",
        }

    total_wins = 0
    total_games = 0
    per_opp: List[Dict[str, Any]] = []

    for opp in opposition_panel:
        opp_label = opp.get("label", "opp")
        opp_deck = opp.get("deck") or []
        opp_cmdr = opp.get("commander") or ("opp-cmdr", "Opponent", "Legendary Creature")
        match = run_match_anti_bias(
            deck_a_oracle_tuples=candidate_deck_oracle_tuples,
            deck_b_oracle_tuples=opp_deck,
            commander_a=candidate_commander,
            commander_b=opp_cmdr,
            n_game_pairs=n_game_pairs_per_opponent,
            max_turns_per_game=max_turns_per_game,
            seed_base=200,
        )
        total_wins += match.deck_a_wins
        total_games += match.games_played
        per_opp.append({
            "label": opp_label,
            "bracket": opp.get("bracket"),
            "games": match.games_played,
            "wins": match.deck_a_wins,
            "draws": match.draws,
            "winrate": round(match.deck_a_winrate, 4),
            "avg_game_length_turns": match.avg_game_length_turns,
        })

    overall = total_wins / total_games if total_games > 0 else None
    return {
        "kind": "playtest_winrate",
        "calibration_status": "uncalibrated",
        "overall_winrate": round(overall, 4) if overall is not None else None,
        "games_total": total_games,
        "per_opponent": per_opp,
        "notes": (
            "Win-rates are uncalibrated: the MPA is a heuristic skeleton "
            "(no blocks, no instant-speed, no priority handling for triggered "
            "abilities). Phase 5b.3 benchmark suite must pass before these "
            "numbers can ground strength claims."
        ),
    }


# ============================================================
# Helpers
# ============================================================


def _hydrate_dummy_costs(state) -> None:
    """For decks built from (oid, name, type_line) tuples, fill in cmc/colors/
    power/toughness from minimal heuristics so the MPA can actually play them.

    This is a stub — Phase 5b.4 hydrates from real card data via the engine's
    cards table. Used only when callers don't pre-hydrate.
    """
    for p in state.players:
        for c in p.command_zone + p.library + p.hand:
            if c.cmc == 0 and not c.is_land():
                # Approximate cost from name (test stubs only)
                if "Lightning" in c.name or "Bolt" in c.name:
                    c.cmc = 1
                elif "Goblin" in c.name and "Chieftain" in c.name:
                    c.cmc = 3
                else:
                    c.cmc = 3  # default mid-curve
                if not c.color_identity:
                    c.color_identity = ("R",)
            if c.is_creature() and c.power is None:
                c.power = 2
                c.toughness = 2


def _tally_game(
    result: MatchResult,
    game,
    *,
    deck_a_seat: int,
    pair_idx: int,
    swap_seat: bool,
) -> None:
    """Apply a single game's result to the match tally. deck_a_seat = seat
    that contains deck A in this game."""
    result.games_played += 1
    won_seat = game.winner_seat
    if won_seat is None:
        result.draws += 1
        outcome = "draw"
    elif won_seat == deck_a_seat:
        result.deck_a_wins += 1
        outcome = "A_wins"
    else:
        result.deck_b_wins += 1
        outcome = "B_wins"
    result.games_log.append({
        "pair": pair_idx,
        "swap": swap_seat,
        "outcome": outcome,
        "winner_seat": won_seat,
        "turns": game.final_turn,
        "actions": game.actions_taken,
        "loss_reason": game.loss_reason,
    })
