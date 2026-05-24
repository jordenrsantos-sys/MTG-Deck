"""Threat-vector + politics state tracker.

threat_vector.py: feature extraction from current GameState.
politics_state.py: persistent per-player record of opponent dynamics.
"""
from api.engine.pillar_f.v0_2.policy.politics.threat_vector import (
    THREAT_VECTOR_VERSION,
    compute_threat_vector,
    compute_all_threat_vectors,
)
from api.engine.pillar_f.v0_2.policy.politics.politics_state import (
    POLITICS_STATE_VERSION,
    DEALS_CAP,
    RECENT_AGGRESSION_WINDOW,
    ALLIANCE_VALUES,
    update_politics_state,
    roll_damage_log_for_turn,
    export_politics_context,
)

__all__ = [
    "THREAT_VECTOR_VERSION",
    "compute_threat_vector", "compute_all_threat_vectors",
    "POLITICS_STATE_VERSION",
    "DEALS_CAP", "RECENT_AGGRESSION_WINDOW", "ALLIANCE_VALUES",
    "update_politics_state", "roll_damage_log_for_turn",
    "export_politics_context",
]
