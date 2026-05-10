"""Pilot decision module for Phase 5b offline playtest framework (stage 5b.4).

Per Decision 3 of OFFLINE_PLAYTEST_DESIGN.md: 5-axis pilot personality model
with 6 named presets (vengeful / shark / control / casual / combo /
archenemy_aware). Personality presets live in calibration-only data pack
`pilot_personalities_v1.json`. Per CALIBRATION_BOUNDARY safety #6, all
political / personality / decision-system code lives in calibration.

Stage 5b.4 ships the data model + preset loader + a minimal deterministic
decision evaluator. Stages 5b.5+ flesh out actual decision logic against
real game states.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .game_state import GameState, PHASE_MAIN_COMBAT, PHASE_UPKEEP, update_player_state

PILOT_V1_VERSION = "pilot_v1"

PILOT_PERSONALITIES_PACK_PATH = (
    Path(__file__).resolve().parents[2]
    / "api" / "engine" / "data" / "calibration" / "pilot_personalities_v1.json"
)

PERSONALITY_AXES = (
    "aggression",
    "politics",
    "threat_assessment",
    "alliance_formation",
    "removal_targeting",
)

PRESET_NAMES = (
    "archenemy_aware",
    "casual",
    "combo",
    "control",
    "shark",
    "vengeful",
)


@dataclass(frozen=True)
class PilotPersonality:
    name: str
    aggression: float
    politics: float
    threat_assessment: float
    alliance_formation: float
    removal_targeting: float

    def axis_value(self, axis: str) -> float:
        return getattr(self, axis)


def load_personalities_pack(pack_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the pilot_personalities_v1 calibration pack."""
    path = pack_path if pack_path is not None else PILOT_PERSONALITIES_PACK_PATH
    return json.loads(path.read_text(encoding="utf-8"))


def get_preset(name: str, pack: Optional[Dict[str, Any]] = None) -> PilotPersonality:
    """Resolve a preset name to a PilotPersonality instance."""
    if pack is None:
        pack = load_personalities_pack()
    presets = pack.get("presets", {})
    if name not in presets:
        raise KeyError(f"Unknown personality preset: {name}")
    p = presets[name]
    return PilotPersonality(
        name=name,
        aggression=float(p["aggression"]),
        politics=float(p["politics"]),
        threat_assessment=float(p["threat_assessment"]),
        alliance_formation=float(p["alliance_formation"]),
        removal_targeting=float(p["removal_targeting"]),
    )


def all_presets(pack: Optional[Dict[str, Any]] = None) -> List[PilotPersonality]:
    """Return all named presets sorted by name."""
    if pack is None:
        pack = load_personalities_pack()
    return [get_preset(n, pack) for n in sorted(pack.get("presets", {}).keys())]


def evaluate_pilot_decision(
    *,
    state: GameState,
    player_id: int,
    personality: PilotPersonality,
    seed: int,
) -> GameState:
    """Evaluate a pilot's decision at the current game phase.

    Stage 5b.4 implementation: minimal deterministic baseline. The pilot
    plays a battlefield permanent if hand is non-empty (deterministic via
    seeded RNG) during main_combat; otherwise no-op.

    Stages 5b.5+ extend with: aggression-driven attack decisions, threat-
    assessment-driven target selection, removal_targeting priority, etc.
    """
    p = state.players[player_id]
    if not p.is_alive:
        return state

    rng = random.Random((seed << 16) ^ (state.turn << 8) ^ player_id)

    if state.phase == PHASE_MAIN_COMBAT and p.hand_oracle_ids:
        # Aggression threshold: more aggressive pilots play more permanents.
        # Personality-deterministic decision: fixed threshold per personality.
        play_chance = personality.aggression * 0.5 + 0.25
        if rng.random() < play_chance:
            # Pop the first card from hand to battlefield.
            played = p.hand_oracle_ids[0]
            new_hand = p.hand_oracle_ids[1:]
            new_battlefield = p.battlefield_oracle_ids + (played,)
            return update_player_state(
                state, player_id,
                hand_oracle_ids=new_hand,
                battlefield_oracle_ids=new_battlefield,
            )
    return state


def make_decision_callback(
    personalities_by_player: Dict[int, PilotPersonality],
    seed: int,
):
    """Return a closure usable as `decision_callback` for `turn_loop` advances."""
    def cb(state: GameState, player_id: int) -> GameState:
        personality = personalities_by_player.get(player_id)
        if personality is None:
            return state
        return evaluate_pilot_decision(
            state=state, player_id=player_id, personality=personality, seed=seed,
        )
    return cb
