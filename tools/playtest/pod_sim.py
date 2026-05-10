"""Pod simulation wiring for Phase 5b offline playtest framework (stage 5b.6).

Connects opposition_pool.py (corpus-based bracket-deterministic opposition
selection) to sim_runner.py (single-game runner). Produces per-(test_deck,
target_bracket, seed) game records.

Per Decision 4 of OFFLINE_PLAYTEST_DESIGN.md: fixed 4-player Commander pod
(1 test deck + 3 opposition). Per safety #7: B5 pod-sim collision detection
fires when test_deck IS one of the few B5 decks; opposition pool falls back
to nearest-neighbor brackets and the resulting `collision_detected` /
`fallback_used` flags propagate into the game record for downstream
analysis.

Calibration-only module.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import opposition_pool, pilot, win_condition_extractor
from .pilot import PilotPersonality
from .sim_runner import DeckProfile, GameSetup, run_single_game


POD_SIM_V1_VERSION = "pod_sim_v1"

DEFAULT_OPPOSITION_PRESET = "casual"


def _envelope_for_deck(
    deck_dict: Dict[str, Any],
    signals_override: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Compute the win-condition envelope for a corpus deck.

    Per stage 5b.11: pod_sim wires envelopes into run_single_game so the
    sim's combat dispatch table can route on win_condition_primary. The
    envelope's `signals_used` field gives the dispatch handlers their
    enabler counts.
    """
    return win_condition_extractor.envelope_for_deck_dict(
        deck_dict, signals_override=signals_override,
    )


def _build_deck_profile(deck_dict: Dict[str, Any]) -> DeckProfile:
    """Build a DeckProfile from a corpus deck dict."""
    return DeckProfile(
        deck_id=deck_dict["deck_id"],
        commander_oracle_ids=tuple(deck_dict.get("commander_oracle_ids") or []),
        deck_oracle_ids=tuple(deck_dict.get("deck_oracle_ids") or []),
    )


def _resolve_personalities(
    *,
    test_personality: PilotPersonality,
    opposition_preset_names: Optional[Sequence[str]] = None,
    pod_size: int = opposition_pool.POD_SIZE_DEFAULT,
) -> Tuple[PilotPersonality, ...]:
    """Resolve 4 personalities: test deck pilot + 3 opposition pilots."""
    pack = pilot.load_personalities_pack()
    if opposition_preset_names is None:
        # Default: all 3 opposition use DEFAULT_OPPOSITION_PRESET
        opposition_preset_names = [DEFAULT_OPPOSITION_PRESET] * pod_size
    if len(opposition_preset_names) != pod_size:
        raise ValueError(
            f"opposition_preset_names must have length {pod_size}; got "
            f"{len(opposition_preset_names)}"
        )
    opposition_personalities = tuple(
        pilot.get_preset(name, pack) for name in opposition_preset_names
    )
    return (test_personality,) + opposition_personalities


def run_pod_game(
    *,
    test_deck_id: str,
    target_bracket: str,
    test_personality: PilotPersonality,
    seed: int,
    opposition_preset_names: Optional[Sequence[str]] = None,
    corpus: Optional[Dict[str, Any]] = None,
    max_turns: int = None,
    per_game_timeout_sec: float = None,
    signals_by_deck_id: Optional[Dict[str, Dict[str, int]]] = None,
) -> Dict[str, Any]:
    """Run one pod game: test_deck + 3 bracket-deterministic opposition.

    Returns the sim-runner game record extended with pod_sim metadata
    (target_bracket, opposition_pool_summary, fallback_used, collision_detected).
    """
    if corpus is None:
        corpus = opposition_pool.load_corpus()
    decks_by_id = {d["deck_id"]: d for d in corpus.get("decks", []) if isinstance(d, dict)}
    if test_deck_id not in decks_by_id:
        raise KeyError(f"test_deck_id not in corpus: {test_deck_id}")

    pool_result = opposition_pool.select_opposition(
        target_bracket=target_bracket,
        exclude_deck_id=test_deck_id,
        corpus=corpus,
        seed=seed,
    )

    test_deck = _build_deck_profile(decks_by_id[test_deck_id])
    opposition_decks = tuple(
        _build_deck_profile(decks_by_id[oid]) for oid in pool_result["opposition_deck_ids"]
    )
    if len(opposition_decks) < opposition_pool.POD_SIZE_DEFAULT:
        raise RuntimeError(
            f"Opposition pool returned {len(opposition_decks)} decks; "
            f"expected {opposition_pool.POD_SIZE_DEFAULT}"
        )

    personalities = _resolve_personalities(
        test_personality=test_personality,
        opposition_preset_names=opposition_preset_names,
    )
    setup = GameSetup(
        decks=(test_deck,) + opposition_decks,
        personalities=personalities,
        seed=seed,
    )

    # Compute win-condition envelopes per player (stage 5b.11). The dispatch
    # table in sim_runner._apply_combat_damage routes on envelope.primary_type.
    setup_decks_for_envelope = [decks_by_id[test_deck_id]] + [
        decks_by_id[oid] for oid in pool_result["opposition_deck_ids"]
    ]
    envelopes_by_player: list = []
    for deck_dict in setup_decks_for_envelope:
        sig_override = None
        if signals_by_deck_id is not None:
            sig_override = signals_by_deck_id.get(deck_dict["deck_id"])
        envelopes_by_player.append(_envelope_for_deck(deck_dict, sig_override))

    game_kwargs: Dict[str, Any] = {"envelopes_by_player": tuple(envelopes_by_player)}
    if max_turns is not None:
        game_kwargs["max_turns"] = max_turns
    if per_game_timeout_sec is not None:
        game_kwargs["per_game_timeout_sec"] = per_game_timeout_sec
    record = run_single_game(setup, **game_kwargs)

    # Extend the record with pod-sim metadata.
    record["pod_sim_version"] = POD_SIM_V1_VERSION
    record["test_deck_id"] = test_deck_id
    record["target_bracket"] = target_bracket
    record["opposition_pool_summary"] = {
        "opposition_deck_ids": pool_result["opposition_deck_ids"],
        "fallback_used": pool_result["fallback_used"],
        "fallback_buckets": pool_result["fallback_buckets"],
        "collision_detected": pool_result["collision_detected"],
    }
    record["winner_is_test_deck"] = (
        record["winner_player_id"] == 0 if record["winner_player_id"] is not None else False
    )
    record["envelope_primary_types"] = [
        (env or {}).get("primary_type") for env in envelopes_by_player
    ]
    return record
