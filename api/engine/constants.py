import json
from pathlib import Path
from typing import Any, Dict, List

from engine.game_changers import load_game_changers


# --- Versions (core) ---
ENGINE_VERSION = "0.2.3"
RULESET_VERSION = "ruleset_v0"
BRACKET_DEFINITION_VERSION = "bracket_v0"
GAME_CHANGERS_VERSION = "gc_v0_userlist_2025-11-20"
UI_CONTRACT_VERSION = "ui_contract_v1"

# --- Layer Versions ---
CANONICAL_LAYER_VERSION = "canonical_v1"
PRIMITIVE_INDEX_VERSION = "primitive_index_v1"
STRUCTURAL_REPORTING_VERSION = "structural_v1"
BUILD_PIPELINE_STAGE = "PROOF_ATTEMPT_V0"

GRAPH_LAYER_VERSION = "graph_v3_typed"
GRAPH_RULESET_VERSION = "graph_rules_v2_typed_edges"
GRAPH_FINGERPRINT_VERSION = "graph_fingerprint_v2"
GRAPH_TYPED_RULES_VERSION = "typed_edge_rules_v0"

MOTIF_LAYER_VERSION = "motif_v1"
MOTIF_RULESET_VERSION = "motif_rules_v0"
MOTIF_FINGERPRINT_VERSION = "motif_fingerprint_v1"

DISRUPTION_LAYER_VERSION = "disruption_v1"
DISRUPTION_RULESET_VERSION = "disruption_rules_v0_graph_theory"
DISRUPTION_FINGERPRINT_VERSION = "disruption_fingerprint_v1"

PATHWAYS_LAYER_VERSION = "pathways_v1"
PATHWAYS_RULESET_VERSION = "pathways_rules_v0_shortest_paths"
PATHWAYS_FINGERPRINT_VERSION = "pathways_fingerprint_v1"

COMBO_SKELETON_LAYER_VERSION = "combo_skeleton_v0"
COMBO_SKELETON_RULESET_VERSION = "combo_skeleton_rules_v0_graph_cycles"
COMBO_SKELETON_FINGERPRINT_VERSION = "combo_skeleton_fingerprint_v1"

COMBO_CANDIDATE_LAYER_VERSION = "combo_candidate_v0"
COMBO_CANDIDATE_RULESET_VERSION = "combo_candidate_rules_v0_from_cycles"
COMBO_CANDIDATE_FINGERPRINT_VERSION = "combo_candidate_fingerprint_v1"

PROOF_SCAFFOLD_LAYER_VERSION = "proof_scaffold_v1"
PROOF_SCAFFOLD_RULESET_VERSION = "proof_scaffold_rules_v2_topic_preferences"
PROOF_SCAFFOLD_RULES_POLICY_VERSION = "rules_context_v2_preferred_sections"
PROOF_SCAFFOLD_FINGERPRINT_VERSION = "proof_scaffold_fingerprint_v2"
PROOF_ATTEMPT_LAYER_VERSION = "proof_attempt_v0"
PROOF_ATTEMPT_LAYER_VERSION_V1 = "proof_attempt_v1_oracle_anchors"
PROOF_ATTEMPT_BUILD_PIPELINE_STAGE_V1 = "PROOF_ATTEMPT_V1_ORACLE"
PROOF_ATTEMPT_LAYER_VERSION_V2 = "proof_attempt_v2_oracle_anchors_plus_slot_links"
PROOF_ATTEMPT_BUILD_PIPELINE_STAGE_V2 = "PROOF_ATTEMPT_V2_ORACLE"

# --- Rules/Thresholds ---
COMBO_SKELETON_BFS_NODE_CAP = 60
MAX_TRIANGLES = 20
MAX_4CYCLES = 20

# --- Typed Edge Rules (closed-world) ---
TYPED_EDGE_RULES_V0 = [
    {
        "edge_type": "ACCELERATION",
        "requires_all_primitives_a": ["RAMP_MANA"],
        "requires_all_primitives_b": ["TOKEN_PRODUCTION"],
        "reason_template": "A accelerates B via ramp enabling token engine",
    },
    {
        "edge_type": "RESOURCE_ALIGNMENT",
        "requires_all_primitives_a": ["MANA_FIXING"],
        "requires_all_primitives_b": ["RAMP_MANA"],
        "reason_template": "A aligns resources with B (fixing + ramp)",
    },
]

DEFAULT_TOPIC_SELECTION_POLICY_ID = "PREFER_SECTIONS_THEN_SORT_(section_id,rule_id)_TAKE_N"
BUILTIN_TOPIC_SELECTION_DEFAULTS = {
    "take_final": 5,
    "fts_fetch_raw": 25,
}
BUILTIN_TOPIC_SELECTION_TOPICS = {
    "PRIORITY": {"prefer_sections": ["117"]},
    "STATE_BASED_ACTIONS": {"prefer_sections": ["704"]},
    "TRIGGERED_ABILITIES": {"prefer_sections": ["603"]},
    "REPLACEMENT_EFFECTS": {"prefer_sections": ["614", "616"]},
    "TARGETING": {"prefer_sections": ["115"]},
}

# --- Paths (local files) ---
REPO_ROOT = Path(__file__).resolve().parents[2]
GAME_CHANGERS_PATH_REL = Path("data/game_changers/gc_v0_userlist_2025-11-20.json")
GAME_CHANGERS_VERSION, GAME_CHANGERS_ABS_PATH, GAME_CHANGERS_SET = load_game_changers(REPO_ROOT)
RULES_DB_REL_PATH = Path("data/rules/rules.sqlite")
RULES_DB_ABS_PATH = (REPO_ROOT / RULES_DB_REL_PATH).resolve()
RULESET_ID_DEFAULT = "cr_2026-01-16"
OVERRIDES_REL_PATH = Path("data/overrides/overrides_v0.json")
OVERRIDES_ABS_PATH = (REPO_ROOT / OVERRIDES_REL_PATH).resolve()
OVERRIDES_VERSION = "overrides_v0"
RULES_TOPIC_CONFIG_REL = Path("data/rules/topic_selection_rules_v0.json")
RULES_TOPIC_CONFIG_ABS = (REPO_ROOT / RULES_TOPIC_CONFIG_REL).resolve()
RULES_TOPIC_CONFIG_VERSION = "topic_selection_rules_v0"
RULES_DB_AVAILABLE = False
OVERRIDES_AVAILABLE = False
OVERRIDES_OBJ: Dict[str, Any] = {}
RULES_TOPIC_CONFIG_AVAILABLE = False
RULES_TOPIC_CONFIG_OBJ: Dict[str, Any] = {}

if RULES_DB_ABS_PATH.exists():
    RULES_DB_AVAILABLE = True

try:
    if OVERRIDES_ABS_PATH.exists():
        loaded_overrides = json.loads(OVERRIDES_ABS_PATH.read_text(encoding="utf-8"))
        if isinstance(loaded_overrides, dict):
            OVERRIDES_AVAILABLE = True
            OVERRIDES_OBJ = loaded_overrides
except Exception:
    OVERRIDES_AVAILABLE = False
    OVERRIDES_OBJ = {}

try:
    if RULES_TOPIC_CONFIG_ABS.exists():
        loaded_topic_config = json.loads(RULES_TOPIC_CONFIG_ABS.read_text(encoding="utf-8"))
        if isinstance(loaded_topic_config, dict):
            RULES_TOPIC_CONFIG_AVAILABLE = True
            RULES_TOPIC_CONFIG_OBJ = loaded_topic_config
except Exception:
    RULES_TOPIC_CONFIG_AVAILABLE = False
    RULES_TOPIC_CONFIG_OBJ = {}

# --- Rules/Thresholds ---
GENERIC_MINIMUMS = {
    "RAMP_MANA": 8,
    "CARD_DRAW": 8,
    "REMOVAL_SINGLE": 8,
    "BOARD_WIPE": 2,
    "PROTECTION": 3,
}


# --- Singleton Exceptions ---
BASIC_NAMES = {"Plains", "Island", "Swamp", "Mountain", "Forest"}
SNOW_BASIC_NAMES = {
    "Snow-Covered Plains",
    "Snow-Covered Island",
    "Snow-Covered Swamp",
    "Snow-Covered Mountain",
    "Snow-Covered Forest",
}
SINGLETON_EXEMPT_NAMES = BASIC_NAMES | SNOW_BASIC_NAMES

PROOF_RULE_TOPICS_V1 = [
    {
        "topic_id": "PRIORITY",
        "fts_query": "priority",
        "description": "Priority windows and stack interactions.",
    },
    {
        "topic_id": "STATE_BASED_ACTIONS",
        "fts_query": "state-based action",
        "description": "State-based actions checks and outcomes.",
    },
    {
        "topic_id": "TRIGGERED_ABILITIES",
        "fts_query": "triggered ability",
        "description": "Triggered abilities and trigger handling.",
    },
    {
        "topic_id": "REPLACEMENT_EFFECTS",
        "fts_query": "replacement effect",
        "description": "Replacement effects and modification semantics.",
    },
    {
        "topic_id": "TARGETING",
        "fts_query": "target",
        "description": "Target declaration and target legality.",
    },
]
