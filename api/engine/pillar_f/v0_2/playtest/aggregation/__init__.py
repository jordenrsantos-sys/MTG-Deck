"""StageTwoReport aggregation + GREEN/YELLOW/RED recommendation (Phase 4)."""
from api.engine.pillar_f.v0_2.playtest.aggregation.aggregator import (
    AGGREGATOR_VERSION,
    GREEN_WINRATE, YELLOW_WINRATE,
    GREEN_AVG_TURN_ELIMINATED, YELLOW_AVG_TURN_ELIMINATED,
    StageTwoReport,
    aggregate_cycle,
)

__all__ = [
    "AGGREGATOR_VERSION",
    "GREEN_WINRATE", "YELLOW_WINRATE",
    "GREEN_AVG_TURN_ELIMINATED", "YELLOW_AVG_TURN_ELIMINATED",
    "StageTwoReport", "aggregate_cycle",
]
