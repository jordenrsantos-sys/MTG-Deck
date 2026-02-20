from __future__ import annotations


DISRUPTION_PRIMITIVE_IDS_V1_VERSION = "disruption_primitives_v1"

# Closed-world primitive IDs for disruption/interaction summaries.
# Keep this list source-controlled and deterministic.
DISRUPTION_PRIMITIVE_IDS = [
    "BOARDWIPE_CREATURES",
    "BOARDWIPE_NONCREATURES",
    "GRAVEYARD_HATE",
    "HAND_DISRUPTION",
    "LIBRARY_HATE_ANTI_TUTOR",
    "PILLOWFORT_TAX_TARGETING",
    "STACK_COUNTERSPELL",
    "STATIC_RULE_SHUT_OFF_GRAVEYARD",
    "STATIC_TAX_ATTACK",
    "STATIC_TAX_SPELLS",
    "TARGETED_REMOVAL_CREATURE",
    "TARGETED_REMOVAL_NONCREATURE",
]
