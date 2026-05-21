import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Optional, Dict, Any, List, AsyncIterator
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from engine.db import (
    commander_legality,
    connect as cards_db_connect,
    is_legal_in_format,
    list_snapshots,
    resolve_db_path,
    resolve_image_cache_dir,
)
from engine.image_cache_contract import (
    IMAGE_CACHE_ALLOWED_SIZES,
    normalize_image_size,
)
from engine.image_runtime import ensure_card_image_cached
from api.engine.constants import (
    ENGINE_VERSION,
    RULESET_VERSION,
    BRACKET_DEFINITION_VERSION,
    GAME_CHANGERS_VERSION,
    GAME_CHANGERS_SET,
    REPO_ROOT,
)
from engine.game_changers import detect_game_changers
from api.engine.decklist_ingest_v1 import (
    build_canonical_deck_input_v1,
    compute_request_hash_v1,
    ingest_decklist,
)
from api.engine.deck_complete_engine_v1 import (
    VERSION as DECK_COMPLETE_ENGINE_V1_VERSION,
    run_deck_complete_engine_v1,
)
from api.engine.deck_completion_v0 import generate_deck_completion_v0
from api.engine.deck_tune_engine_v1 import VERSION as DECK_TUNE_ENGINE_V1_VERSION, run_deck_tune_engine_v1
# v1.7 Stage 5 Deliverable B — production wire-in for bracket-aware
# proactive game-changer recommendations. The wrapper has the same
# signature as run_deck_tune_engine_v1 and post-processes the result
# by appending BRACKET_AWARE_GC entries to recommended_swaps_v1 when
# the bracket cap permits.
from api.engine.layers.bracket_aware_recommendations_v1 import (
    run_deck_tune_with_bracket_aware_recommendations_v1,
)
from api.engine.pipeline_build import run_build_pipeline
from api.engine.layer_registry import (
    filter_response_to_resolved as _filter_response_to_resolved,
    resolve_partial as _resolve_partial,
)
from api.import_url_v1 import import_from_url as _import_from_url
from api.engine.run_history_v0 import diff_runs_v0, get_run_v0, list_runs_v0, save_run_v0
from api.engine.run_bundle_v0 import build_run_bundle_v0
from api.engine.layers.snapshot_preflight_v1 import run_snapshot_preflight_v1
from api.engine.strategy_hypothesis_v0 import generate_strategy_hypotheses_v0
from api.engine.tag_index_query_v0 import (
    get_cards_for_primitive_v0,
    get_primitive_tag_index_status_v0,
    resolve_ruleset_version_v0,
)
from snapshot_build.migrate_card_images_table import REQUIRED_CARD_IMAGES_COLUMNS, ensure_card_images_table

_UTC_NOW = datetime.now


@asynccontextmanager
async def _app_lifespan(_app: FastAPI) -> AsyncIterator[None]:
    startup_image_system_hardening()
    yield


class BuildRequest(BaseModel):
    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    profile_id: str = Field(..., description="Profile ID")
    bracket_id: str = Field(..., description="Bracket definition ID")
    taxonomy_version: Optional[str] = Field(default=None, description="Compiled taxonomy version to use at runtime")

    format: str = "commander"
    commander: Optional[str] = None
    cards: List[str] = Field(default_factory=list)
    engine_patches_v0: List[Dict[str, Any]] = Field(default_factory=list)


class BuildResponse(BaseModel):
    engine_version: str
    ruleset_version: str
    bracket_definition_version: str
    game_changers_version: str
    db_snapshot_id: str
    profile_id: str
    bracket_id: str

    status: str
    deck_size_total: Optional[int] = None
    deck_status: Optional[str] = None
    cards_needed: Optional[int] = None
    cards_to_cut: Optional[int] = None
    cut_order: Optional[List[str]] = None
    build_hash_v1: Optional[str] = None
    graph_hash_v1: Optional[str] = None
    graph_hash_v2: Optional[str] = None
    motif_hash_v1: Optional[str] = None
    disruption_hash_v1: Optional[str] = None
    pathways_hash_v1: Optional[str] = None
    combo_skeleton_hash_v1: Optional[str] = None
    combo_candidates_hash_v1: Optional[str] = None
    proof_scaffolds_hash_v1: Optional[str] = None
    proof_scaffolds_hash_v2: Optional[str] = None
    proof_scaffolds_hash_v3: Optional[str] = None
    request_hash_v1: Optional[str] = None
    unknowns: List[Dict[str, Any]]
    result: Dict[str, Any]


class DecklistUnknownCandidateV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    oracle_id: str
    name: str


class DecklistUnknownV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name_raw: str
    name_norm: str
    count: int
    line_no: int
    reason_code: str
    candidates: List[DecklistUnknownCandidateV1] = Field(default_factory=list)


class DeckValidateViolationV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    card_name: str
    count: int
    line_nos: List[int]
    message: str


class CanonicalDeckInputV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str
    profile_id: str
    bracket_id: str
    format: str
    commander: str
    cards: List[str] = Field(default_factory=list)
    engine_patches_v0: List[Dict[str, str]] = Field(default_factory=list)


class DeckValidateNameOverrideV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name_raw: str
    resolved_oracle_id: Optional[str] = None
    resolved_name: Optional[str] = None


class DeckValidateRequest(BaseModel):
    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    format: str = "commander"
    profile_id: Optional[str] = None
    bracket_id: Optional[str] = None
    commander: Optional[str] = None
    name_overrides_v1: List[DeckValidateNameOverrideV1] = Field(default_factory=list)


class DeckValidateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    db_snapshot_id: str
    format: str
    canonical_deck_input: CanonicalDeckInputV1
    unknowns: List[DecklistUnknownV1]
    violations_v1: List[DeckValidateViolationV1]
    request_hash_v1: str
    parse_version: str
    resolve_version: str
    ingest_version: str


class DeckTuneSwapDeltaSummaryV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_score_delta_v1: float
    coherence_delta_v1: float
    primitive_coverage_delta_v1: int
    gc_compliance_preserved_v1: bool


class DeckTuneSwapV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cut_name: str
    add_name: str
    reasons_v1: List[str] = Field(default_factory=list)
    delta_summary_v1: DeckTuneSwapDeltaSummaryV1


class DeckTuneRequest(BaseModel):
    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    format: str = "commander"
    profile_id: str = Field(..., description="Profile ID")
    bracket_id: str = Field(..., description="Bracket definition ID")
    mulligan_model_id: str = Field(..., description="Mulligan model ID")
    commander: Optional[str] = None
    name_overrides_v1: List[DeckValidateNameOverrideV1] = Field(default_factory=list)
    max_swaps: int = 5
    engine_patches_v0: List[Dict[str, Any]] = Field(default_factory=list)


class DeckTuneResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    db_snapshot_id: str
    format: str
    baseline_summary_v1: Dict[str, Any]
    recommended_swaps_v1: List[DeckTuneSwapV1]
    request_hash_v1: str
    unknowns: List[DecklistUnknownV1]
    violations_v1: List[DeckValidateViolationV1]
    parse_version: str
    resolve_version: str
    ingest_version: str
    tune_engine_version: str


class DeckCompleteAddedCardV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    reasons_v1: List[str] = Field(default_factory=list)
    primitives_added_v1: List[str] = Field(default_factory=list)


class DeckCompleteV1Request(BaseModel):
    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    format: str = "commander"
    profile_id: str = Field(..., description="Profile ID")
    bracket_id: str = Field(..., description="Bracket definition ID")
    mulligan_model_id: str = Field(..., description="Mulligan model ID")
    commander: Optional[str] = None
    name_overrides_v1: List[DeckValidateNameOverrideV1] = Field(default_factory=list)
    target_deck_size: int = 100
    max_adds: int = 30
    allow_basic_lands: bool = True
    land_target_mode: str = "AUTO"


class DeckCompleteV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    codes_v1: List[str] = Field(default_factory=list)
    db_snapshot_id: str
    format: str
    baseline_summary_v1: Dict[str, Any]
    baseline_build_status_v1: str = ""
    baseline_unknowns_v1: List[Dict[str, Any]] = Field(default_factory=list)
    snapshot_preflight_v1: Optional[Dict[str, Any]] = None
    added_cards_v1: List[DeckCompleteAddedCardV1]
    completed_decklist_text_v1: str
    # v1.7.2 Stage 1 — deck-combo insight surfaces (additive, opaque
    # dict shape per entry). Engine layer:
    # api/engine/layers/deck_combo_insights_v1.py. Default empty so
    # legacy clients see the same response keys but with zero-length
    # arrays — strict `extra="forbid"` preserved.
    detected_combos_v1: List[Dict[str, Any]] = Field(default_factory=list)
    missing_partners_v1: List[Dict[str, Any]] = Field(default_factory=list)
    # Game-changer detection for the submitted decklist (commander + cards).
    # Flat sorted list of card names from this deck that appear on the
    # Game Changers list (api/engine/data/game_changers/...). The UI uses
    # this to render "GC" badges on AddedCardRow / deck overview rows.
    # Default empty so legacy clients see a stable shape — strict
    # `extra="forbid"` preserved.
    game_changers_v1: List[str] = Field(default_factory=list)
    # Phase 2.1a — deck theme classification surface (additive, opaque dict
    # shape per entry). Engine layer:
    # api/engine/layers/deck_theme_classifier_v1.py. Each entry has
    # {theme_id, theme_type, subtype, score, confidence_band,
    # classify_threshold, passed_classify_threshold, anti_signal_hit,
    # contributing_primitives}. Default empty for legacy clients +
    # decks with no classified themes.
    deck_themes_v1: List[Dict[str, Any]] = Field(default_factory=list)
    request_hash_v1: str
    unknowns: List[DecklistUnknownV1]
    violations_v1: List[DeckValidateViolationV1]
    parse_version: str
    resolve_version: str
    ingest_version: str
    complete_engine_version: str


class CardSearchV1Filters(BaseModel):
    """Filter set for /card/search_v1. See ENGINE_API_GUIDE.md for full semantics."""
    model_config = ConfigDict(extra="forbid")

    primitives_any: List[str] = Field(default_factory=list)
    primitives_all: List[str] = Field(default_factory=list)
    primitives_none: List[str] = Field(default_factory=list)
    subtypes_any: List[str] = Field(default_factory=list)
    type_any: List[str] = Field(default_factory=list)
    color_identity_subset_of: Optional[List[str]] = None
    color_identity_includes_all: Optional[List[str]] = None
    cmc_min: Optional[float] = None
    cmc_max: Optional[float] = None
    name_contains: Optional[str] = None
    bracket_tier_max: Optional[str] = None
    budget_max_usd: Optional[float] = None
    format_legal_in: Optional[str] = None


class CardSearchV1Request(BaseModel):
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    taxonomy_version: Optional[str] = None
    filters: CardSearchV1Filters = Field(default_factory=CardSearchV1Filters)
    limit: int = 100
    offset: int = 0
    sort_by: str = "name"
    include: List[str] = Field(default_factory=lambda: ["primitives", "type_line", "cmc", "color_identity"])


class CardSearchV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    db_snapshot_id: str
    taxonomy_version: Optional[str] = None
    matched_count: int
    returned_count: int
    offset: int
    limit: int
    sort_by: str
    results: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class AgentContextBundleV1Request(BaseModel):
    """Pillar A.5 — composite agent-kickoff endpoint."""
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    commander: Optional[str] = None
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    intent: Optional[str] = None
    include: List[str] = Field(
        default_factory=lambda: ["analyze", "candidate_pool", "strength_check", "reference_decks"]
    )


class AgentContextBundleV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    db_snapshot_id: str
    commander: Optional[str] = None
    intent: Optional[str] = None
    analyze: Optional[Dict[str, Any]] = None
    candidate_pool: Optional[Dict[str, Any]] = None
    strength_check: Optional[Dict[str, Any]] = None
    reference_decks: Optional[Dict[str, Any]] = None
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class ArchetypeBriefV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    commander: str
    commander_oracle_id: Optional[str] = None
    color_identity: List[str] = Field(default_factory=list)
    corpus_deck_count: int
    common_archetypes: List[Dict[str, Any]] = Field(default_factory=list)
    bracket_distribution: Dict[str, float] = Field(default_factory=dict)
    staple_cards: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class ThemeTopCardsV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    theme_id: str
    subtype: Optional[str] = None
    primitives_used_for_match: List[str] = Field(default_factory=list)
    matched_count: int = 0
    returned_count: int = 0
    results: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class CorpusSimilarDecksV1Request(BaseModel):
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    commander: Optional[str] = None
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    k: int = 5
    include_decklists: bool = False


class CorpusSimilarDecksV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    k_returned: int
    decks: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class AgentBuildDeckV1Request(BaseModel):
    """Pillar D — AI deck-building agent. Builds a 99-card deck honoring
    user intent (commander + bracket + theme hints + must-include cards),
    respecting the creativity envelope (user picks dominate, no forced
    staples that don't match user intent)."""
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    commander: str = Field(..., description="Commander card name (exact match)")
    bracket: str = Field(..., description="B1..B5")
    theme_hints: List[str] = Field(default_factory=list, description="Theme IDs or labels")
    must_include_cards: List[str] = Field(
        default_factory=list, description="Card names that MUST appear in output deck"
    )
    max_iterations: int = Field(default=5, description="Outer build retries (Phase D inner cap is 12)")
    seed: Optional[int] = Field(default=None, description="Deterministic tie-break seed")


class AgentBuildDeckV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    status: str
    deck: List[Dict[str, str]] = Field(default_factory=list)
    summary: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[Dict[str, str]] = Field(default_factory=list)
    elapsed_ms: int = 0


class CorpusBatchIngestEntry(BaseModel):
    """One external deck for batch corpus ingest."""
    model_config = ConfigDict(extra="forbid")

    commander: str
    decklist: List[str]
    claimed_bracket: str = Field(..., description="B1..B5, user-set from source")
    source_url: Optional[str] = None
    source_label: Optional[str] = None
    archetype_hint: Optional[str] = None


class CorpusBatchIngestV1Request(BaseModel):
    """Phase 5a EDHREC-style batch ingest — feeds external decks into the
    strength oracle corpus with bracket cross-check + review queue."""
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    entries: List[CorpusBatchIngestEntry]
    skip_bracket_verification: bool = False


class CorpusBatchIngestV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    status: str
    total_submitted: int = 0
    accepted: int = 0
    auto_bumped: int = 0
    needs_review: int = 0
    rejected: int = 0
    results: List[Dict[str, Any]] = Field(default_factory=list)
    reason: Optional[str] = None


class PlaytestBenchmarkV1Request(BaseModel):
    """Phase 5b.3 benchmark runner — runs N-game anti-bias-mirrored playtest
    between two corpus deck IDs, writes result to calibration_log_v1.jsonl."""
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    deck_a_corpus_id: str = Field(..., description="Corpus ID of first deck")
    deck_b_corpus_id: str = Field(..., description="Corpus ID of second deck")
    benchmark_name: str = Field(default="manual", description="Name to log (e.g. 'mirror_match', 'B4_cedh_vs_B1_precon')")
    n_game_pairs: int = 2
    max_turns_per_game: int = 25
    expected_winrate_min: Optional[float] = None
    expected_winrate_max: Optional[float] = None


class PlaytestBenchmarkV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    benchmark_name: str
    deck_a_label: str
    deck_b_label: str
    n_games: int
    deck_a_winrate: float
    expected_winrate_min: Optional[float] = None
    expected_winrate_max: Optional[float] = None
    passes_assertion: Optional[bool] = None
    overall_calibration_status: str
    per_game_log: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class DeckSaveToLibraryV1Request(BaseModel):
    """Pillar C.2 — save a deck to the Obsidian DECK_LIBRARY."""
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    commander: str = Field(..., description="Commander card name")
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    deck_name: Optional[str] = None
    archetype: Optional[str] = None
    bracket: Optional[str] = None
    user_id: Optional[str] = None
    ingest_to_corpus: bool = False


class DeckSaveToLibraryV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    page_path: Optional[str] = None
    deck_name: Optional[str] = None
    archetype: Optional[str] = None
    bracket: Optional[str] = None
    corpus_id: Optional[str] = None
    reason: Optional[str] = None
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class DeckStrengthCheckV1Request(BaseModel):
    """Pillar A.4 — strength oracle Measurement A (corpus similarity)."""
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    commander: Optional[str] = None
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    include_measurement_b: bool = False
    k_nearest: int = 5


class DeckStrengthCheckV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    db_snapshot_id: str
    commander: Optional[str] = None
    candidate_card_count: int
    measurement_a: Dict[str, Any]
    measurement_b: Optional[Dict[str, Any]] = None
    combined_strength_band: str
    interpretation: str
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class DeckCandidatePoolV1Request(BaseModel):
    """Pillar A.3 — clustered wide candidate pool. NEVER returns top-N."""
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    commander: Optional[str] = None
    raw_decklist_text: str = Field(..., description="Raw decklist text")
    intent: Optional[str] = None
    extra_filters: Optional[Dict[str, Any]] = None
    min_pool_size: int = 100
    max_pool_size: int = 250


class DeckCandidatePoolV1Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    db_snapshot_id: str
    commander: Optional[str] = None
    intent: Optional[str] = None
    color_identity: List[str] = Field(default_factory=list)
    deck_themes_summary: List[str] = Field(default_factory=list)
    pool_size: int
    clusters: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, str]] = Field(default_factory=list)


class DeckAnalyzeV1Request(BaseModel):
    """Pillar A.1 — AI-facing diagnostic snapshot.

    No bracket_id required: analyze estimates the natural bracket itself.
    Designed for AI agents to call as the kickoff context for a build/improve
    session. <500ms warm response target per DESIGN_DECISIONS.md rule 1.2.
    """
    model_config = ConfigDict(extra="forbid")

    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    commander: Optional[str] = None
    raw_decklist_text: str = Field(..., description="Raw decklist text (TappedOut/MTGO format)")
    format: str = "commander"
    include_debug: bool = False


class DeckAnalyzeV1Response(BaseModel):
    """See ENGINE_API_GUIDE.md /deck/analyze_v1 for field semantics."""
    model_config = ConfigDict(extra="forbid")

    version: str
    db_snapshot_id: str
    commander_oracle_id: Optional[str] = None
    commander: Optional[str] = None
    card_count: int
    color_identity: List[str] = Field(default_factory=list)
    mana_curve: Dict[str, int] = Field(default_factory=dict)
    subtype_density: Dict[str, int] = Field(default_factory=dict)
    primitive_density: Dict[str, int] = Field(default_factory=dict)
    deck_themes_v1: List[Dict[str, Any]] = Field(default_factory=list)
    detected_combos_v1: List[Dict[str, Any]] = Field(default_factory=list)
    missing_partners_v1: List[Dict[str, Any]] = Field(default_factory=list)
    bracket_estimate: Optional[str] = None
    bracket_envelope: Dict[str, Any] = Field(default_factory=dict)
    gap_signal: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, str]] = Field(default_factory=list)
    # Debug payload — present only when request.include_debug=True
    debug: Optional[Dict[str, Any]] = None


class StrategyHypothesisRequest(BaseModel):
    anchor_cards: List[str] = Field(default_factory=list)
    commander: Optional[str] = None
    profile_id: str = Field(..., description="Profile ID")
    bracket_id: str = Field(..., description="Bracket definition ID")
    max_packages_per_hypothesis: int = 5
    max_cards_per_package: int = 4
    validate_packages: bool = False


class DeckCompleteRequest(BaseModel):
    commander: str = Field(..., description="Commander card name")
    anchors: List[str] = Field(default_factory=list)
    profile_id: str = Field(..., description="Profile ID")
    bracket_id: str = Field(..., description="Bracket definition ID")
    max_iters: int = 40
    target_deck_size: int = 100
    seed_package: Optional[Dict[str, Any]] = None
    validate_each_iter: bool = True
    db_snapshot_id: Optional[str] = None
    refine: bool = False
    max_refine_iters: int = 30
    swap_batch_size: int = 8
    validate_each_refine_iter: bool = True
    save_run: bool = False


class CardsResolveNamesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_id: Optional[str] = None
    names: List[str] = Field(default_factory=list)


class CardsResolveNamesResultRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input: str
    name: str
    oracle_id: str
    mana_cost: Optional[str] = None
    type_line: Optional[str] = None
    image_uri: Optional[str] = None


class CardsResolveNamesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    snapshot_id: str
    results: List[CardsResolveNamesResultRow] = Field(default_factory=list)
    missing: List[str] = Field(default_factory=list)


app = FastAPI(title="MTG Strategy Engine", version=ENGINE_VERSION, lifespan=_app_lifespan)
logger = logging.getLogger(__name__)

DEV_CORS = os.getenv("MTG_ENGINE_DEV_CORS", "0") == "1"

if DEV_CORS:
    dev_ports = range(5173, 5181)
    allow_origins = [f"http://127.0.0.1:{port}" for port in dev_ports] + [
        f"http://localhost:{port}" for port in dev_ports
    ]
else:
    allow_origins = [
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:5174",
        "http://localhost:5174",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UI_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UI_DIST_DIR_ENV = "MTG_ENGINE_UI_DIST_DIR"
UI_DIST_PATH_OVERRIDE = os.getenv(UI_DIST_DIR_ENV, "").strip()
if UI_DIST_PATH_OVERRIDE != "":
    UI_DIST_PATH = os.path.abspath(UI_DIST_PATH_OVERRIDE)
else:
    UI_DIST_PATH = os.path.join(UI_REPO_ROOT, "ui_harness", "dist")
UI_ASSETS_PATH = os.path.join(UI_DIST_PATH, "assets")
UI_INDEX_PATH = os.path.join(UI_DIST_PATH, "index.html")
UI_STATIC_ENABLED = os.path.isdir(UI_DIST_PATH) and os.path.isfile(UI_INDEX_PATH)

if UI_STATIC_ENABLED and os.path.isdir(UI_ASSETS_PATH):
    app.mount("/assets", StaticFiles(directory=UI_ASSETS_PATH), name="assets")

CARD_IMAGE_CACHE_CONTROL = "public, max-age=31536000"


def _card_images_schema_ok(con) -> bool:
    try:
        rows = con.execute("PRAGMA table_info(card_images)").fetchall()
    except Exception:
        return False

    available_columns: set[str] = set()
    for row in rows:
        try:
            row_dict = dict(row)
        except Exception:
            continue
        column_name = row_dict.get("name")
        if isinstance(column_name, str) and column_name != "":
            available_columns.add(column_name)

    return all(column in available_columns for column in REQUIRED_CARD_IMAGES_COLUMNS)


def _health_cached_image_count(con) -> int:
    try:
        row = con.execute("SELECT COUNT(*) AS total FROM card_images").fetchone()
    except Exception:
        return 0

    if row is None:
        return 0
    try:
        row_dict = dict(row)
        total = row_dict.get("total")
    except Exception:
        total = row[0] if isinstance(row, (tuple, list)) and len(row) > 0 else 0

    if isinstance(total, int):
        return max(0, total)
    return 0


def _health_sample_oracle_id(con) -> str:
    try:
        row = con.execute(
            """
            SELECT oracle_id
            FROM cards
            ORDER BY snapshot_id DESC, oracle_id ASC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        return ""

    if row is None:
        return ""

    try:
        row_dict = dict(row)
        oracle_id = row_dict.get("oracle_id")
    except Exception:
        oracle_id = row[0] if isinstance(row, (tuple, list)) and len(row) > 0 else ""

    return _coerce_nonempty_str(oracle_id)


def startup_image_system_hardening() -> None:
    try:
        db_path = resolve_db_path()
    except Exception as exc:
        logger.exception("Failed resolving DB path at startup")
        raise RuntimeError("Failed resolving DB path at startup") from exc

    image_cache_dir = resolve_image_cache_dir()
    logger.info("DB_PATH=%s", db_path)
    logger.info("IMAGE_CACHE_DIR=%s", image_cache_dir)

    try:
        ensure_card_images_table(db_path=db_path)
    except Exception as exc:
        logger.exception("card_images schema migration failed")
        raise RuntimeError("card_images schema migration failed") from exc

    logger.info("card_images schema migration OK")


@app.get("/health")
def health():
    try:
        latest_snapshot_id = _latest_snapshot_id()
    except Exception:
        latest_snapshot_id = ""

    git_commit = (
        os.getenv("MTG_ENGINE_GIT_COMMIT", "").strip()
        or os.getenv("GIT_COMMIT", "").strip()
        or os.getenv("VITE_GIT_SHA", "").strip()
        or os.getenv("UI_COMMIT", "").strip()
    )

    image_cache_dir = resolve_image_cache_dir()
    image_cache_exists = Path(image_cache_dir).is_dir()
    card_images_schema_ok = False
    cached_image_count = 0
    sample_oracle_id = ""
    sample_exists_on_disk = False

    try:
        with cards_db_connect() as con:
            card_images_schema_ok = _card_images_schema_ok(con)
            cached_image_count = _health_cached_image_count(con)
            sample_oracle_id = _health_sample_oracle_id(con)
    except Exception:
        card_images_schema_ok = False
        cached_image_count = 0
        sample_oracle_id = ""

    if sample_oracle_id != "":
        sample_exists_on_disk = (
            Path(image_cache_dir)
            .joinpath("normal", f"{sample_oracle_id}.jpg")
            .is_file()
        )

    payload = {
        "status": "OK",
        "engine_version": ENGINE_VERSION,
        "db_snapshot_id": latest_snapshot_id,
        "ruleset_version": RULESET_VERSION,
        "bracket_definition_version": BRACKET_DEFINITION_VERSION,
        "server_time": _UTC_NOW(timezone.utc).isoformat().replace("+00:00", "Z"),
        "image_cache_dir": image_cache_dir,
        "image_cache_exists": bool(image_cache_exists),
        "card_images_schema_ok": bool(card_images_schema_ok),
        "cached_image_count": int(cached_image_count),
        "image_sample_test": {
            "oracle_id": sample_oracle_id,
            "exists_on_disk": bool(sample_exists_on_disk),
        },
    }
    if git_commit != "":
        payload["git_commit"] = git_commit
    return payload


@app.get("/snapshots")
def snapshots(limit: int = 20):
    return {"snapshots": list_snapshots(limit=limit)}


@app.get("/snapshots/active")
def snapshots_active():
    """Mega-task v5 Phase 2: return the active (latest) snapshot id.

    The UI previously required users to know + paste a snapshot id (an
    internal DB identifier with no user-facing meaning); today's live
    session caught a user pasting a Scryfall card URL into that field.
    This endpoint lets the UI auto-default the field so the user never
    has to know it exists. Returns an empty string if no snapshots are
    registered yet (so the UI can fall back to disabled-Build).
    """
    return {"snapshot_id": _latest_snapshot_id()}


@app.get("/snapshot/preflight/{snapshot_id}")
def snapshot_preflight_v1(snapshot_id: str):
    with cards_db_connect() as con:
        return run_snapshot_preflight_v1(db=con, snapshot_id=snapshot_id)


@app.get("/cards/suggest")
def cards_suggest(q: str, snapshot_id: Optional[str] = None, limit: int = 20, commander_only: bool = False):
    query = _coerce_nonempty_str(q).lower()
    normalized_snapshot_id = _coerce_nonempty_str(snapshot_id)
    safe_limit = min(max(_coerce_positive_int(limit, default=20), 1), 20)
    require_legal_commander = bool(commander_only)
    candidate_limit = safe_limit if not require_legal_commander else min(max(safe_limit * 8, safe_limit), 200)

    if len(query) < 2:
        return {
            "query": query,
            "snapshot_id": normalized_snapshot_id,
            "limit": safe_limit,
            "results": [],
        }

    if normalized_snapshot_id == "":
        normalized_snapshot_id = _latest_snapshot_id()

    if normalized_snapshot_id == "":
        return {
            "query": query,
            "snapshot_id": "",
            "limit": safe_limit,
            "results": [],
        }

    def _to_result_row(row: Any) -> Dict[str, Any] | None:
        if not isinstance(row, (dict,)):
            try:
                row_dict = dict(row)
            except Exception:
                return None
        else:
            row_dict = row

        name = _coerce_nonempty_str(row_dict.get("name"))
        oracle_id = _coerce_nonempty_str(row_dict.get("oracle_id"))
        if name == "":
            return None

        mana_cost_raw = row_dict.get("mana_cost")
        type_line_raw = row_dict.get("type_line")
        image_uri = _extract_image_uri_from_card_row(row_dict)

        return {
            "oracle_id": oracle_id,
            "name": name,
            "mana_cost": mana_cost_raw if isinstance(mana_cost_raw, str) and mana_cost_raw != "" else None,
            "type_line": type_line_raw if isinstance(type_line_raw, str) and type_line_raw != "" else None,
            "image_uri": image_uri,
        }

    try:
        with cards_db_connect() as con:
            pragma_rows = con.execute("PRAGMA table_info(cards)").fetchall()
            available_columns: set[str] = set()
            for pragma_row in pragma_rows:
                try:
                    pragma_row_dict = dict(pragma_row)
                except Exception:
                    continue
                col_name = pragma_row_dict.get("name")
                if isinstance(col_name, str) and col_name != "":
                    available_columns.add(col_name)

            select_fields = ["oracle_id", "name", "mana_cost", "type_line"]
            for optional_field in [
                "image_uri",
                "image_url",
                "art_uri",
                "art_url",
                "image_uris_json",
                "card_faces_json",
            ]:
                if optional_field in available_columns:
                    select_fields.append(optional_field)

            select_clause = ", ".join(select_fields)

            prefix_rows = con.execute(
                f"SELECT {select_clause} FROM cards "
                "WHERE snapshot_id = ? AND LOWER(name) LIKE ? "
                "ORDER BY name ASC LIMIT ?",
                (normalized_snapshot_id, query + "%", candidate_limit),
            ).fetchall()

            results: List[Dict[str, Any]] = []
            dedupe_keys: set[str] = set()
            commander_eligibility_cache: Dict[str, bool] = {}

            def _is_legal_commander_suggestion(card_name: str) -> bool:
                cache_key = _coerce_nonempty_str(card_name).lower()
                if cache_key in commander_eligibility_cache:
                    return commander_eligibility_cache[cache_key]

                try:
                    commander_ok, _, resolved_card = commander_legality(normalized_snapshot_id, card_name)
                    format_ok = False
                    if commander_ok and isinstance(resolved_card, dict):
                        format_ok, _ = is_legal_in_format(resolved_card, "commander")
                    is_legal = bool(commander_ok and format_ok)
                except Exception:
                    is_legal = False

                commander_eligibility_cache[cache_key] = is_legal
                return is_legal

            def _append_rows(rows: Any) -> None:
                for row in rows:
                    result_row = _to_result_row(row)
                    if result_row is None:
                        continue
                    oracle_id_key = result_row.get("oracle_id")
                    if isinstance(oracle_id_key, str) and oracle_id_key != "":
                        dedupe_key = f"oracle:{oracle_id_key}"
                    else:
                        dedupe_key = f"name:{str(result_row.get('name') or '').lower()}"
                    if dedupe_key in dedupe_keys:
                        continue
                    dedupe_keys.add(dedupe_key)

                    if require_legal_commander and not _is_legal_commander_suggestion(str(result_row.get("name") or "")):
                        continue

                    results.append(result_row)
                    if len(results) >= safe_limit:
                        return

            _append_rows(prefix_rows)

            if len(results) < safe_limit:
                remaining = candidate_limit if require_legal_commander else safe_limit - len(results)
                contains_rows = con.execute(
                    f"SELECT {select_clause} FROM cards "
                    "WHERE snapshot_id = ? AND LOWER(name) LIKE ? AND LOWER(name) NOT LIKE ? "
                    "ORDER BY name ASC LIMIT ?",
                    (normalized_snapshot_id, "%" + query + "%", query + "%", remaining),
                ).fetchall()
                _append_rows(contains_rows)

    except Exception:
        return {
            "query": query,
            "snapshot_id": normalized_snapshot_id,
            "limit": safe_limit,
            "results": [],
        }

    return {
        "query": query,
        "snapshot_id": normalized_snapshot_id,
        "limit": safe_limit,
        "results": results,
    }


@app.get("/cards/image/{oracle_id}")
def cards_image(oracle_id: str, size: str = "normal"):
    normalized_size = _normalize_card_image_size(size)
    normalized_oracle_id = _normalize_oracle_id(oracle_id)
    cache_dir = _resolve_card_image_cache_dir()
    with cards_db_connect() as con:
        current_snapshot_id = _latest_snapshot_id_from_connection(con)
        fetch_result = ensure_card_image_cached(
            con=con,
            cache_dir=str(cache_dir),
            oracle_id=normalized_oracle_id,
            size=normalized_size,
            current_snapshot_id=current_snapshot_id,
        )

    result_status = _coerce_nonempty_str(fetch_result.get("status"))
    result_path = Path(_coerce_nonempty_str(fetch_result.get("image_path")))

    if result_status == "IMAGE_URI_MISSING":
        logger.warning("IMAGE_URI_MISSING %s %s", normalized_oracle_id, normalized_size)
        return JSONResponse(
            status_code=404,
            content={
                "code": "IMAGE_URI_MISSING",
                "oracle_id": normalized_oracle_id,
                "size": normalized_size,
            },
        )

    if result_status == "IMAGE_FETCH_FAILED":
        logger.error(
            "IMAGE_FETCH_FAILED %s %s error=%s",
            normalized_oracle_id,
            normalized_size,
            _coerce_nonempty_str(fetch_result.get("error")),
        )
        return JSONResponse(
            status_code=502,
            content={
                "code": "IMAGE_FETCH_FAILED",
                "oracle_id": normalized_oracle_id,
                "size": normalized_size,
            },
        )

    if result_status == "CACHE_HIT":
        logger.info("IMAGE_CACHE_HIT %s %s", normalized_oracle_id, normalized_size)
    elif result_status == "IMAGE_FETCHED":
        logger.info("IMAGE_FETCHED %s %s", normalized_oracle_id, normalized_size)

    if result_path == Path("") or not result_path.is_file():
        raise HTTPException(status_code=500, detail="Image cache resolution failed.")

    response = FileResponse(path=str(result_path), media_type="image/jpeg")
    response.headers["Cache-Control"] = CARD_IMAGE_CACHE_CONTROL
    return response


@app.post("/cards/resolve_names", response_model=CardsResolveNamesResponse)
def cards_resolve_names(req: CardsResolveNamesRequest):
    if len(req.names) > 200:
        raise HTTPException(status_code=400, detail="names supports at most 200 entries.")

    normalized_inputs: List[str] = []
    for index, raw_name in enumerate(req.names):
        token = _coerce_nonempty_str(raw_name)
        if token == "":
            raise HTTPException(status_code=400, detail=f"names[{index}] must be a non-empty string.")
        normalized_inputs.append(token)

    normalized_snapshot_id = _coerce_nonempty_str(req.snapshot_id)
    if normalized_snapshot_id == "":
        normalized_snapshot_id = _latest_snapshot_id()

    if len(normalized_inputs) == 0:
        return {
            "status": "OK",
            "snapshot_id": normalized_snapshot_id,
            "results": [],
            "missing": [],
        }

    if normalized_snapshot_id == "":
        return {
            "status": "OK",
            "snapshot_id": "",
            "results": [],
            "missing": normalized_inputs,
        }

    requested_name_keys: List[str] = []
    seen_keys: set[str] = set()
    for token in normalized_inputs:
        name_key = token.lower()
        if name_key in seen_keys:
            continue
        seen_keys.add(name_key)
        requested_name_keys.append(name_key)

    rows_by_name_key: Dict[str, Dict[str, Any]] = {}

    with cards_db_connect() as con:
        pragma_rows = con.execute("PRAGMA table_info(cards)").fetchall()
        available_columns: set[str] = set()
        for pragma_row in pragma_rows:
            try:
                pragma_row_dict = dict(pragma_row)
            except Exception:
                continue
            col_name = pragma_row_dict.get("name")
            if isinstance(col_name, str) and col_name != "":
                available_columns.add(col_name)

        select_fields = ["oracle_id", "name", "mana_cost", "type_line"]
        for optional_field in [
            "image_uri",
            "image_url",
            "art_uri",
            "art_url",
            "image_uris_json",
            "card_faces_json",
        ]:
            if optional_field in available_columns:
                select_fields.append(optional_field)
        select_clause = ", ".join(select_fields)

        placeholders = ", ".join(["?"] * len(requested_name_keys))
        rows = con.execute(
            f"SELECT {select_clause} FROM cards "
            f"WHERE snapshot_id = ? AND LOWER(name) IN ({placeholders}) "
            "ORDER BY LOWER(name) ASC, name ASC, oracle_id ASC",
            [normalized_snapshot_id, *requested_name_keys],
        ).fetchall()

    for row in rows:
        try:
            row_dict = dict(row)
        except Exception:
            continue

        resolved_name = _coerce_nonempty_str(row_dict.get("name"))
        if resolved_name == "":
            continue

        key = resolved_name.lower()
        if key in rows_by_name_key:
            continue

        mana_cost_raw = row_dict.get("mana_cost")
        type_line_raw = row_dict.get("type_line")
        rows_by_name_key[key] = {
            "name": resolved_name,
            "oracle_id": _coerce_nonempty_str(row_dict.get("oracle_id")),
            "mana_cost": mana_cost_raw if isinstance(mana_cost_raw, str) and mana_cost_raw != "" else None,
            "type_line": type_line_raw if isinstance(type_line_raw, str) and type_line_raw != "" else None,
            "image_uri": _extract_image_uri_from_card_row(row_dict),
        }

    results: List[Dict[str, Any]] = []
    missing: List[str] = []
    for input_name in normalized_inputs:
        resolved = rows_by_name_key.get(input_name.lower())
        if not isinstance(resolved, dict):
            missing.append(input_name)
            continue
        results.append(
            {
                "input": input_name,
                "name": resolved.get("name"),
                "oracle_id": resolved.get("oracle_id"),
                "mana_cost": resolved.get("mana_cost"),
                "type_line": resolved.get("type_line"),
                "image_uri": resolved.get("image_uri"),
            }
        )

    return {
        "status": "OK",
        "snapshot_id": normalized_snapshot_id,
        "results": results,
        "missing": missing,
    }


@app.post("/build", response_model=BuildResponse)
def build(req: BuildRequest):
    canonical_request = build_canonical_deck_input_v1(
        db_snapshot_id=req.db_snapshot_id,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        format=req.format,
        commander=req.commander if isinstance(req.commander, str) else "",
        cards=req.cards,
        engine_patches_v0=req.engine_patches_v0,
    )
    request_hash_v1 = compute_request_hash_v1(canonical_request)

    payload = run_build_pipeline(req=req, conn=None, repo_root_path=REPO_ROOT)
    payload["request_hash_v1"] = request_hash_v1
    return BuildResponse(**payload)


# Engine-4A: POST /build/partial — fast incremental engine call.
# Per HARD safety: this endpoint does NOT modify pipeline_build or any engine
# layer. It runs the full pipeline (existing behavior) and filters the
# response down to the requested layers + their transitive deps via the
# layer_registry topological resolver. A future Phase 6 sub-task may
# short-circuit pipeline_build for true incremental performance; Engine-4A
# ships the API surface so the UI seed-builder (Phase 4.4) can consume it.
@app.post("/build/partial")
def build_partial(req: BuildRequest, request: Request):
    layers_param = request.query_params.get("layers", "")
    requested_layer_ids = [s.strip() for s in layers_param.split(",") if s.strip()]
    resolution = _resolve_partial(requested_layer_ids)

    if resolution["unknown"]:
        return JSONResponse(
            status_code=400,
            content={
                "status": "INVALID_REQUEST",
                "reason_code": "unknown_layer_ids",
                "unknown_layer_ids": resolution["unknown"],
                "resolution": resolution,
            },
        )

    canonical_request = build_canonical_deck_input_v1(
        db_snapshot_id=req.db_snapshot_id,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        format=req.format,
        commander=req.commander if isinstance(req.commander, str) else "",
        cards=req.cards,
        engine_patches_v0=req.engine_patches_v0,
    )
    request_hash_v1 = compute_request_hash_v1(canonical_request)
    payload = run_build_pipeline(req=req, conn=None, repo_root_path=REPO_ROOT)

    full_result = payload.get("result") if isinstance(payload, dict) else None
    filtered_result = _filter_response_to_resolved(
        full_result if isinstance(full_result, dict) else {},
        resolution["resolved"],
    )

    return JSONResponse(
        status_code=200,
        content={
            "status": "OK",
            "request_hash_v1": request_hash_v1,
            "resolution": resolution,
            "result": filtered_result,
            "engine_version": payload.get("engine_version") if isinstance(payload, dict) else None,
        },
    )


# Engine-4A: POST /import/url — server-side CORS proxy + parser registry.
class ImportUrlRequest(BaseModel):
    url: str = Field(..., description="Public deck-source URL to import")


@app.post("/import/url")
def import_url(req: ImportUrlRequest):
    result = _import_from_url(req.url)
    # Per Engine-4A: status_code reflects outcome semantics.
    if result.get("status") == "OK":
        return JSONResponse(status_code=200, content=result)
    if result.get("status") == "DEFERRED":
        return JSONResponse(status_code=200, content=result)  # 200 + deferred body
    if result.get("status") == "INVALID_REQUEST":
        return JSONResponse(status_code=400, content=result)
    if result.get("status") in ("PARSE_ERROR", "FETCH_ERROR"):
        return JSONResponse(status_code=502, content=result)  # bad gateway-ish
    return JSONResponse(status_code=500, content=result)


def _coerce_nonnegative_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return int(default)
    if int(value) < 0:
        return int(default)
    return int(value)


def _coerce_positive_int(value: Any, *, default: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return int(default)
    if int(value) < 1:
        return int(default)
    return int(value)


def _coerce_nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def _looks_like_wrapped_deck_complete_payload(raw_json: Any) -> bool:
    if not isinstance(raw_json, dict):
        return False
    endpoint_payload = raw_json.get("endpoint_payload")
    if not isinstance(endpoint_payload, dict):
        return False

    expected_keys = {
        "db_snapshot_id",
        "raw_decklist_text",
        "format",
        "profile_id",
        "bracket_id",
        "mulligan_model_id",
        "target_deck_size",
        "max_adds",
        "allow_basic_lands",
        "land_target_mode",
        "commander",
        "name_overrides_v1",
    }
    return any(key in endpoint_payload for key in expected_keys)


def _clean_unique_strings_in_order(values: Any, *, limit: int = 0) -> List[str]:
    if not isinstance(values, list):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        token = _coerce_nonempty_str(value)
        if token == "" or token in seen:
            continue
        seen.add(token)
        out.append(token)
        if int(limit) > 0 and len(out) >= int(limit):
            break
    return out


def _coerce_float(value: Any, *, default: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return float(default)
    return float(value)


def _coerce_nonnegative_float(value: Any, *, default: float = 0.0) -> float:
    numeric = _coerce_float(value, default=default)
    if numeric < 0.0:
        return float(default)
    return float(numeric)


def _round6(value: float) -> float:
    return float(f"{float(value):.6f}")


def _parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _latest_snapshot_id() -> str:
    snapshots_payload = list_snapshots(limit=1)
    if not isinstance(snapshots_payload, list) or len(snapshots_payload) == 0:
        return ""
    first_row = snapshots_payload[0]
    if not isinstance(first_row, dict):
        return ""
    return _coerce_nonempty_str(first_row.get("snapshot_id"))


def _runtime_db_path_str() -> str:
    return str(resolve_db_path())


def _latest_snapshot_id_from_connection(con) -> str:
    try:
        row = con.execute(
            """
            SELECT snapshot_id
            FROM snapshots
            ORDER BY created_at DESC, snapshot_id DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        return ""

    if row is None:
        return ""

    try:
        row_dict = dict(row)
        value = row_dict.get("snapshot_id")
    except Exception:
        value = row[0] if isinstance(row, (tuple, list)) and len(row) > 0 else ""

    return _coerce_nonempty_str(value)


def _resolve_card_image_cache_dir() -> Path:
    return Path(resolve_image_cache_dir()).resolve()


def _normalize_card_image_size(size: Any) -> str:
    try:
        return normalize_image_size(size)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid size '{size}'. Allowed: {sorted(IMAGE_CACHE_ALLOWED_SIZES)}",
        )


def _normalize_oracle_id(oracle_id: Any) -> str:
    token = _coerce_nonempty_str(oracle_id)
    if token == "":
        raise HTTPException(status_code=400, detail="Invalid oracle_id.")
    try:
        return str(UUID(token))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid oracle_id.") from exc


def _extract_image_uri_from_card_row(card_row: Dict[str, Any]) -> str | None:
    for direct_key in ["image_uri", "image_url", "art_uri", "art_url"]:
        value = card_row.get(direct_key)
        if isinstance(value, str) and value.strip() != "":
            return value.strip()

    image_uris = _parse_json_object(card_row.get("image_uris_json"))
    for image_key in ["normal", "large", "png", "art_crop", "small"]:
        value = image_uris.get(image_key)
        if isinstance(value, str) and value.strip() != "":
            return value.strip()

    card_faces = _parse_json_list(card_row.get("card_faces_json"))
    for face in card_faces:
        if not isinstance(face, dict):
            continue
        face_image_uris = face.get("image_uris") if isinstance(face.get("image_uris"), dict) else {}
        for image_key in ["normal", "large", "png", "art_crop", "small"]:
            value = face_image_uris.get(image_key)
            if isinstance(value, str) and value.strip() != "":
                return value.strip()

    return None


def _build_commander_decklist_text_v1(*, commander: str, cards: List[str]) -> str:
    commander_clean = _coerce_nonempty_str(commander)
    if commander_clean == "":
        return ""

    lines: List[str] = ["Commander", f"1 {commander_clean}", "Deck"]
    for card_name in cards:
        token = _coerce_nonempty_str(card_name)
        if token == "":
            continue
        lines.append(f"1 {token}")
    return "\n".join(lines)


@app.post("/deck/validate", response_model=DeckValidateResponse)
def deck_validate(req: DeckValidateRequest):
    name_overrides_v1 = [
        row.model_dump(mode="python")
        for row in req.name_overrides_v1
        if isinstance(row, DeckValidateNameOverrideV1)
    ]

    ingest_payload = ingest_decklist(
        raw_text=req.raw_decklist_text,
        db_snapshot_id=req.db_snapshot_id,
        format=req.format,
        commander_name_override=req.commander,
        name_overrides_v1=name_overrides_v1,
    )

    canonical_from_ingest = (
        ingest_payload.get("canonical_deck_input")
        if isinstance(ingest_payload.get("canonical_deck_input"), dict)
        else {}
    )
    canonical_commander = (
        canonical_from_ingest.get("commander")
        if isinstance(canonical_from_ingest.get("commander"), str)
        else ""
    )
    canonical_cards = (
        canonical_from_ingest.get("cards")
        if isinstance(canonical_from_ingest.get("cards"), list)
        else []
    )

    canonical_deck_input_dict = build_canonical_deck_input_v1(
        db_snapshot_id=req.db_snapshot_id,
        profile_id=req.profile_id if isinstance(req.profile_id, str) else "",
        bracket_id=req.bracket_id if isinstance(req.bracket_id, str) else "",
        format=ingest_payload.get("format") if isinstance(ingest_payload.get("format"), str) else "commander",
        commander=canonical_commander,
        cards=[name for name in canonical_cards if isinstance(name, str)],
        engine_patches_v0=[],
        name_overrides_v1=name_overrides_v1,
    )
    request_hash_v1 = compute_request_hash_v1(canonical_deck_input_dict)

    canonical_deck_input = CanonicalDeckInputV1(
        db_snapshot_id=canonical_deck_input_dict["db_snapshot_id"],
        profile_id=canonical_deck_input_dict["profile_id"],
        bracket_id=canonical_deck_input_dict["bracket_id"],
        format=canonical_deck_input_dict["format"],
        commander=canonical_deck_input_dict["commander"],
        cards=list(canonical_deck_input_dict["cards"]),
        engine_patches_v0=list(canonical_deck_input_dict["engine_patches_v0"]),
    )

    unknowns_raw = ingest_payload.get("unknowns") if isinstance(ingest_payload.get("unknowns"), list) else []
    unknowns: List[DecklistUnknownV1] = []
    for row in unknowns_raw:
        if not isinstance(row, dict):
            continue
        candidates_raw = row.get("candidates") if isinstance(row.get("candidates"), list) else []
        candidates: List[DecklistUnknownCandidateV1] = []
        for candidate in candidates_raw:
            if not isinstance(candidate, dict):
                continue
            oracle_id = _coerce_nonempty_str(candidate.get("oracle_id"))
            name = _coerce_nonempty_str(candidate.get("name"))
            if oracle_id == "" or name == "":
                continue
            candidates.append(
                DecklistUnknownCandidateV1(
                    oracle_id=oracle_id,
                    name=name,
                )
            )

        unknowns.append(
            DecklistUnknownV1(
                name_raw=_coerce_nonempty_str(row.get("name_raw")),
                name_norm=_coerce_nonempty_str(row.get("name_norm")),
                count=_coerce_positive_int(row.get("count"), default=1),
                line_no=_coerce_nonnegative_int(row.get("line_no"), default=0),
                reason_code=_coerce_nonempty_str(row.get("reason_code")),
                candidates=candidates,
            )
        )

    violations_raw = ingest_payload.get("violations_v1") if isinstance(ingest_payload.get("violations_v1"), list) else []
    violations_v1: List[DeckValidateViolationV1] = []
    for row in violations_raw:
        if not isinstance(row, dict):
            continue
        line_nos_raw = row.get("line_nos") if isinstance(row.get("line_nos"), list) else []
        line_nos = sorted(
            {
                _coerce_nonnegative_int(value, default=0)
                for value in line_nos_raw
                if isinstance(value, int) and not isinstance(value, bool)
            }
        )
        violations_v1.append(
            DeckValidateViolationV1(
                code=_coerce_nonempty_str(row.get("code")),
                card_name=_coerce_nonempty_str(row.get("card_name")),
                count=_coerce_positive_int(row.get("count"), default=1),
                line_nos=line_nos,
                message=_coerce_nonempty_str(row.get("message")),
            )
        )

    return DeckValidateResponse(
        status=ingest_payload.get("status") if isinstance(ingest_payload.get("status"), str) else "UNKNOWN_PRESENT",
        db_snapshot_id=req.db_snapshot_id,
        format=canonical_deck_input.format,
        canonical_deck_input=canonical_deck_input,
        unknowns=unknowns,
        violations_v1=violations_v1,
        request_hash_v1=request_hash_v1,
        parse_version=ingest_payload.get("parse_version") if isinstance(ingest_payload.get("parse_version"), str) else "",
        resolve_version=ingest_payload.get("resolve_version") if isinstance(ingest_payload.get("resolve_version"), str) else "",
        ingest_version=ingest_payload.get("ingest_version") if isinstance(ingest_payload.get("ingest_version"), str) else "",
    )


@app.post("/deck/tune_v1", response_model=DeckTuneResponse)
def deck_tune_v1(req: DeckTuneRequest):
    dev_metrics_enabled = os.getenv("MTG_ENGINE_DEV_METRICS") == "1"
    start_total_timer = perf_counter()

    name_overrides_v1 = [
        row.model_dump(mode="python")
        for row in req.name_overrides_v1
        if isinstance(row, DeckValidateNameOverrideV1)
    ]

    ingest_payload = ingest_decklist(
        raw_text=req.raw_decklist_text,
        db_snapshot_id=req.db_snapshot_id,
        format=req.format,
        commander_name_override=req.commander,
        name_overrides_v1=name_overrides_v1,
    )

    canonical_from_ingest = (
        ingest_payload.get("canonical_deck_input")
        if isinstance(ingest_payload.get("canonical_deck_input"), dict)
        else {}
    )
    canonical_commander = (
        canonical_from_ingest.get("commander")
        if isinstance(canonical_from_ingest.get("commander"), str)
        else ""
    )
    canonical_cards = (
        canonical_from_ingest.get("cards")
        if isinstance(canonical_from_ingest.get("cards"), list)
        else []
    )
    tune_engine_patches_v0 = [dict(row) for row in req.engine_patches_v0 if isinstance(row, dict)]

    canonical_deck_input_dict = build_canonical_deck_input_v1(
        db_snapshot_id=req.db_snapshot_id,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        format=ingest_payload.get("format") if isinstance(ingest_payload.get("format"), str) else "commander",
        commander=canonical_commander,
        cards=[name for name in canonical_cards if isinstance(name, str)],
        engine_patches_v0=tune_engine_patches_v0,
        name_overrides_v1=name_overrides_v1,
    )
    request_hash_v1 = compute_request_hash_v1(canonical_deck_input_dict)

    unknowns_raw = ingest_payload.get("unknowns") if isinstance(ingest_payload.get("unknowns"), list) else []
    unknowns: List[DecklistUnknownV1] = []
    for row in unknowns_raw:
        if not isinstance(row, dict):
            continue
        candidates_raw = row.get("candidates") if isinstance(row.get("candidates"), list) else []
        candidates: List[DecklistUnknownCandidateV1] = []
        for candidate in candidates_raw:
            if not isinstance(candidate, dict):
                continue
            oracle_id = _coerce_nonempty_str(candidate.get("oracle_id"))
            name = _coerce_nonempty_str(candidate.get("name"))
            if oracle_id == "" or name == "":
                continue
            candidates.append(
                DecklistUnknownCandidateV1(
                    oracle_id=oracle_id,
                    name=name,
                )
            )

        unknowns.append(
            DecklistUnknownV1(
                name_raw=_coerce_nonempty_str(row.get("name_raw")),
                name_norm=_coerce_nonempty_str(row.get("name_norm")),
                count=_coerce_positive_int(row.get("count"), default=1),
                line_no=_coerce_nonnegative_int(row.get("line_no"), default=0),
                reason_code=_coerce_nonempty_str(row.get("reason_code")),
                candidates=candidates,
            )
        )

    violations_raw = ingest_payload.get("violations_v1") if isinstance(ingest_payload.get("violations_v1"), list) else []
    violations_v1: List[DeckValidateViolationV1] = []
    for row in violations_raw:
        if not isinstance(row, dict):
            continue
        line_nos_raw = row.get("line_nos") if isinstance(row.get("line_nos"), list) else []
        line_nos = sorted(
            {
                _coerce_nonnegative_int(value, default=0)
                for value in line_nos_raw
                if isinstance(value, int) and not isinstance(value, bool)
            }
        )
        violations_v1.append(
            DeckValidateViolationV1(
                code=_coerce_nonempty_str(row.get("code")),
                card_name=_coerce_nonempty_str(row.get("card_name")),
                count=_coerce_positive_int(row.get("count"), default=1),
                line_nos=line_nos,
                message=_coerce_nonempty_str(row.get("message")),
            )
        )

    if len(unknowns) > 0:
        dev_metrics_v1 = None
        if dev_metrics_enabled:
            total_ms = _round6(max((perf_counter() - start_total_timer) * 1000.0, 0.0))
            dev_metrics_v1 = {
                "baseline_build_ms": 0.0,
                "candidate_pool_ms": 0.0,
                "candidate_pool_breakdown_v1": {
                    "sql_query_ms": 0.0,
                    "python_filter_ms": 0.0,
                    "color_check_ms": 0.0,
                    "gc_check_ms": 0.0,
                    "total_candidates_seen": 0,
                    "total_candidates_returned": 0,
                },
                "swap_eval_count": 0,
                "swap_eval_ms_total": 0.0,
                "total_ms": total_ms,
            }
            print("TUNE DEBUG:")
            print(f"baseline_build_ms={dev_metrics_v1['baseline_build_ms']}")
            print(f"candidate_pool_ms={dev_metrics_v1['candidate_pool_ms']}")
            print(f"swap_eval_count={dev_metrics_v1['swap_eval_count']}")
            print(f"swap_eval_ms_total={dev_metrics_v1['swap_eval_ms_total']}")
            print(f"total_ms={dev_metrics_v1['total_ms']}")

        response = DeckTuneResponse(
            status="UNKNOWN_PRESENT",
            db_snapshot_id=req.db_snapshot_id,
            format=canonical_deck_input_dict.get("format") if isinstance(canonical_deck_input_dict.get("format"), str) else "commander",
            baseline_summary_v1={},
            recommended_swaps_v1=[],
            request_hash_v1=request_hash_v1,
            unknowns=unknowns,
            violations_v1=violations_v1,
            parse_version=ingest_payload.get("parse_version") if isinstance(ingest_payload.get("parse_version"), str) else "",
            resolve_version=ingest_payload.get("resolve_version") if isinstance(ingest_payload.get("resolve_version"), str) else "",
            ingest_version=ingest_payload.get("ingest_version") if isinstance(ingest_payload.get("ingest_version"), str) else "",
            tune_engine_version=DECK_TUNE_ENGINE_V1_VERSION,
        )

        if dev_metrics_enabled and isinstance(dev_metrics_v1, dict):
            payload = response.model_dump(mode="python")
            payload["dev_metrics_v1"] = dev_metrics_v1
            return JSONResponse(content=payload)

        return response

    build_req = BuildRequest(
        db_snapshot_id=canonical_deck_input_dict.get("db_snapshot_id") if isinstance(canonical_deck_input_dict.get("db_snapshot_id"), str) else req.db_snapshot_id,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        taxonomy_version=None,
        format=canonical_deck_input_dict.get("format") if isinstance(canonical_deck_input_dict.get("format"), str) else "commander",
        commander=canonical_deck_input_dict.get("commander") if isinstance(canonical_deck_input_dict.get("commander"), str) else "",
        cards=[name for name in canonical_deck_input_dict.get("cards", []) if isinstance(name, str)],
        engine_patches_v0=[],
    )
    baseline_build_started_at = perf_counter()
    baseline_build_payload = run_build_pipeline(req=build_req, conn=None, repo_root_path=REPO_ROOT)
    baseline_build_ms = _round6(max((perf_counter() - baseline_build_started_at) * 1000.0, 0.0))

    tune_payload = run_deck_tune_with_bracket_aware_recommendations_v1(
        canonical_deck_input=canonical_deck_input_dict,
        baseline_build_result=baseline_build_payload,
        db_snapshot_id=build_req.db_snapshot_id,
        bracket_id=req.bracket_id,
        profile_id=req.profile_id,
        mulligan_model_id=req.mulligan_model_id,
        max_swaps=req.max_swaps,
        collect_dev_metrics=dev_metrics_enabled,
    )

    tune_dev_metrics = tune_payload.get("dev_metrics_v1") if isinstance(tune_payload.get("dev_metrics_v1"), dict) else {}
    candidate_pool_breakdown_raw = (
        tune_dev_metrics.get("candidate_pool_breakdown_v1")
        if isinstance(tune_dev_metrics.get("candidate_pool_breakdown_v1"), dict)
        else {}
    )
    candidate_pool_breakdown_v1 = {
        "sql_query_ms": _round6(_coerce_nonnegative_float(candidate_pool_breakdown_raw.get("sql_query_ms"), default=0.0)),
        "python_filter_ms": _round6(
            _coerce_nonnegative_float(candidate_pool_breakdown_raw.get("python_filter_ms"), default=0.0)
        ),
        "color_check_ms": _round6(_coerce_nonnegative_float(candidate_pool_breakdown_raw.get("color_check_ms"), default=0.0)),
        "gc_check_ms": _round6(_coerce_nonnegative_float(candidate_pool_breakdown_raw.get("gc_check_ms"), default=0.0)),
        "total_candidates_seen": _coerce_nonnegative_int(candidate_pool_breakdown_raw.get("total_candidates_seen"), default=0),
        "total_candidates_returned": _coerce_nonnegative_int(
            candidate_pool_breakdown_raw.get("total_candidates_returned"),
            default=0,
        ),
    }
    swap_eval_count = _coerce_nonnegative_int(
        tune_dev_metrics.get("swap_eval_count"),
        default=_coerce_nonnegative_int(
            (tune_payload.get("evaluation_summary_v1") if isinstance(tune_payload.get("evaluation_summary_v1"), dict) else {}).get(
                "swap_evaluations_total"
            ),
            default=0,
        ),
    )
    protected_cut_names_top10 = _clean_unique_strings_in_order(
        tune_dev_metrics.get("protected_cut_names_top10"),
        limit=10,
    )
    swap_eval_ms_total = _round6(_coerce_nonnegative_float(tune_dev_metrics.get("swap_eval_ms_total"), default=0.0))
    candidate_pool_ms = _round6(_coerce_nonnegative_float(tune_dev_metrics.get("candidate_pool_ms"), default=0.0))
    total_ms = _round6(max((perf_counter() - start_total_timer) * 1000.0, 0.0))

    dev_metrics_v1 = None
    if dev_metrics_enabled:
        dev_metrics_v1 = {
            "baseline_build_ms": baseline_build_ms,
            "candidate_pool_ms": candidate_pool_ms,
            "candidate_pool_breakdown_v1": candidate_pool_breakdown_v1,
            "swap_eval_count": swap_eval_count,
            "swap_eval_ms_total": swap_eval_ms_total,
            "protected_cut_count": _coerce_nonnegative_int(tune_dev_metrics.get("protected_cut_count"), default=0),
            "protected_cut_names_top10": protected_cut_names_top10,
            "total_ms": total_ms,
        }
        print("TUNE DEBUG:")
        print(f"baseline_build_ms={baseline_build_ms}")
        print(f"candidate_pool_ms={candidate_pool_ms}")
        print(f"swap_eval_count={swap_eval_count}")
        print(f"swap_eval_ms_total={swap_eval_ms_total}")
        print(f"total_ms={total_ms}")

    tune_status = tune_payload.get("status") if isinstance(tune_payload.get("status"), str) else "WARN"
    baseline_summary_v1 = tune_payload.get("baseline_summary_v1") if isinstance(tune_payload.get("baseline_summary_v1"), dict) else {}
    swaps_raw = tune_payload.get("recommended_swaps_v1") if isinstance(tune_payload.get("recommended_swaps_v1"), list) else []

    recommended_swaps_v1: List[DeckTuneSwapV1] = []
    for row in swaps_raw:
        if not isinstance(row, dict):
            continue
        delta_raw = row.get("delta_summary_v1") if isinstance(row.get("delta_summary_v1"), dict) else {}
        reasons_raw = row.get("reasons_v1") if isinstance(row.get("reasons_v1"), list) else []
        reasons = sorted(
            {
                _coerce_nonempty_str(reason)
                for reason in reasons_raw
                if isinstance(reason, str) and _coerce_nonempty_str(reason) != ""
            }
        )
        cut_name = _coerce_nonempty_str(row.get("cut_name"))
        add_name = _coerce_nonempty_str(row.get("add_name"))
        if cut_name == "" or add_name == "":
            continue

        recommended_swaps_v1.append(
            DeckTuneSwapV1(
                cut_name=cut_name,
                add_name=add_name,
                reasons_v1=reasons,
                delta_summary_v1=DeckTuneSwapDeltaSummaryV1(
                    total_score_delta_v1=_coerce_float(delta_raw.get("total_score_delta_v1"), default=0.0),
                    coherence_delta_v1=_coerce_float(delta_raw.get("coherence_delta_v1"), default=0.0),
                    primitive_coverage_delta_v1=_coerce_nonnegative_int(
                        delta_raw.get("primitive_coverage_delta_v1"),
                        default=0,
                    ),
                    gc_compliance_preserved_v1=bool(delta_raw.get("gc_compliance_preserved_v1")),
                ),
            )
        )

    response = DeckTuneResponse(
        status=tune_status,
        db_snapshot_id=build_req.db_snapshot_id,
        format=build_req.format,
        baseline_summary_v1=baseline_summary_v1,
        recommended_swaps_v1=recommended_swaps_v1,
        request_hash_v1=request_hash_v1,
        unknowns=unknowns,
        violations_v1=violations_v1,
        parse_version=ingest_payload.get("parse_version") if isinstance(ingest_payload.get("parse_version"), str) else "",
        resolve_version=ingest_payload.get("resolve_version") if isinstance(ingest_payload.get("resolve_version"), str) else "",
        ingest_version=ingest_payload.get("ingest_version") if isinstance(ingest_payload.get("ingest_version"), str) else "",
        tune_engine_version=(
            tune_payload.get("version")
            if isinstance(tune_payload.get("version"), str)
            else DECK_TUNE_ENGINE_V1_VERSION
        ),
    )

    if dev_metrics_enabled and isinstance(dev_metrics_v1, dict):
        payload = response.model_dump(mode="python")
        payload["dev_metrics_v1"] = dev_metrics_v1
        return JSONResponse(content=payload)

    return response


@app.post("/deck/complete_v1", response_model=DeckCompleteV1Response)
async def deck_complete_v1(req: DeckCompleteV1Request, request: Request):
    dev_metrics_enabled = os.getenv("MTG_ENGINE_DEV_METRICS") == "1"
    start_total_timer = perf_counter()

    if _coerce_nonempty_str(req.raw_decklist_text) == "":
        detail = "raw_decklist_text missing."
        raw_json: Any = None
        try:
            raw_json = await request.json()
        except Exception:
            raw_json = None

        if _looks_like_wrapped_deck_complete_payload(raw_json):
            detail = "raw_decklist_text missing. Did you wrap the payload in endpoint_payload?"
        raise HTTPException(status_code=422, detail=detail)

    name_overrides_v1 = [
        row.model_dump(mode="python")
        for row in req.name_overrides_v1
        if isinstance(row, DeckValidateNameOverrideV1)
    ]

    ingest_payload = ingest_decklist(
        raw_text=req.raw_decklist_text,
        db_snapshot_id=req.db_snapshot_id,
        format=req.format,
        commander_name_override=req.commander,
        name_overrides_v1=name_overrides_v1,
    )

    parsed_payload = ingest_payload.get("parsed") if isinstance(ingest_payload.get("parsed"), dict) else {}
    parse_totals_raw = parsed_payload.get("totals") if isinstance(parsed_payload.get("totals"), dict) else {}
    parse_totals = {
        "items_total": _coerce_nonnegative_int(parse_totals_raw.get("items_total"), default=0),
        "card_count_total": _coerce_nonnegative_int(parse_totals_raw.get("card_count_total"), default=0),
        "ignored_line_total": _coerce_nonnegative_int(parse_totals_raw.get("ignored_line_total"), default=0),
    }
    if parse_totals["items_total"] == 0:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "EMPTY_INGEST",
                "message": "No deck lines parsed from raw_decklist_text",
                "raw_first120": req.raw_decklist_text[:120],
                "parse_totals": parse_totals,
            },
        )

    canonical_from_ingest = (
        ingest_payload.get("canonical_deck_input")
        if isinstance(ingest_payload.get("canonical_deck_input"), dict)
        else {}
    )
    canonical_commander = (
        canonical_from_ingest.get("commander")
        if isinstance(canonical_from_ingest.get("commander"), str)
        else ""
    )
    canonical_cards = (
        canonical_from_ingest.get("cards")
        if isinstance(canonical_from_ingest.get("cards"), list)
        else []
    )
    canonical_commander_list_raw = (
        canonical_from_ingest.get("commander_list_v1")
        if isinstance(canonical_from_ingest.get("commander_list_v1"), list)
        else []
    )
    canonical_commander_list = _clean_unique_strings_in_order(canonical_commander_list_raw)

    canonical_deck_input_dict = build_canonical_deck_input_v1(
        db_snapshot_id=req.db_snapshot_id,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        format=ingest_payload.get("format") if isinstance(ingest_payload.get("format"), str) else "commander",
        commander=canonical_commander,
        cards=[name for name in canonical_cards if isinstance(name, str)],
        engine_patches_v0=[],
        name_overrides_v1=name_overrides_v1,
    )
    if len(canonical_commander_list) > 0:
        canonical_deck_input_dict["commander_list_v1"] = list(canonical_commander_list)
    request_hash_v1 = compute_request_hash_v1(canonical_deck_input_dict)

    unknowns_raw = ingest_payload.get("unknowns") if isinstance(ingest_payload.get("unknowns"), list) else []
    unknowns: List[DecklistUnknownV1] = []
    for row in unknowns_raw:
        if not isinstance(row, dict):
            continue
        candidates_raw = row.get("candidates") if isinstance(row.get("candidates"), list) else []
        candidates: List[DecklistUnknownCandidateV1] = []
        for candidate in candidates_raw:
            if not isinstance(candidate, dict):
                continue
            oracle_id = _coerce_nonempty_str(candidate.get("oracle_id"))
            name = _coerce_nonempty_str(candidate.get("name"))
            if oracle_id == "" or name == "":
                continue
            candidates.append(
                DecklistUnknownCandidateV1(
                    oracle_id=oracle_id,
                    name=name,
                )
            )

        unknowns.append(
            DecklistUnknownV1(
                name_raw=_coerce_nonempty_str(row.get("name_raw")),
                name_norm=_coerce_nonempty_str(row.get("name_norm")),
                count=_coerce_positive_int(row.get("count"), default=1),
                line_no=_coerce_nonnegative_int(row.get("line_no"), default=0),
                reason_code=_coerce_nonempty_str(row.get("reason_code")),
                candidates=candidates,
            )
        )

    violations_raw = ingest_payload.get("violations_v1") if isinstance(ingest_payload.get("violations_v1"), list) else []
    violations_v1: List[DeckValidateViolationV1] = []
    for row in violations_raw:
        if not isinstance(row, dict):
            continue
        line_nos_raw = row.get("line_nos") if isinstance(row.get("line_nos"), list) else []
        line_nos = sorted(
            {
                _coerce_nonnegative_int(value, default=0)
                for value in line_nos_raw
                if isinstance(value, int) and not isinstance(value, bool)
            }
        )
        violations_v1.append(
            DeckValidateViolationV1(
                code=_coerce_nonempty_str(row.get("code")),
                card_name=_coerce_nonempty_str(row.get("card_name")),
                count=_coerce_positive_int(row.get("count"), default=1),
                line_nos=line_nos,
                message=_coerce_nonempty_str(row.get("message")),
            )
        )

    if len(unknowns) > 0:
        return DeckCompleteV1Response(
            status="UNKNOWN_PRESENT",
            codes_v1=[],
            db_snapshot_id=req.db_snapshot_id,
            format=canonical_deck_input_dict.get("format") if isinstance(canonical_deck_input_dict.get("format"), str) else "commander",
            baseline_summary_v1={},
            baseline_build_status_v1="",
            baseline_unknowns_v1=[],
            snapshot_preflight_v1=None,
            added_cards_v1=[],
            completed_decklist_text_v1=_build_commander_decklist_text_v1(
                commander=canonical_deck_input_dict.get("commander")
                if isinstance(canonical_deck_input_dict.get("commander"), str)
                else "",
                cards=[name for name in canonical_deck_input_dict.get("cards", []) if isinstance(name, str)],
            ),
            request_hash_v1=request_hash_v1,
            unknowns=unknowns,
            violations_v1=violations_v1,
            parse_version=ingest_payload.get("parse_version") if isinstance(ingest_payload.get("parse_version"), str) else "",
            resolve_version=ingest_payload.get("resolve_version") if isinstance(ingest_payload.get("resolve_version"), str) else "",
            ingest_version=ingest_payload.get("ingest_version") if isinstance(ingest_payload.get("ingest_version"), str) else "",
            complete_engine_version=DECK_COMPLETE_ENGINE_V1_VERSION,
        )

    build_req = BuildRequest(
        db_snapshot_id=(
            canonical_deck_input_dict.get("db_snapshot_id")
            if isinstance(canonical_deck_input_dict.get("db_snapshot_id"), str)
            else req.db_snapshot_id
        ),
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        taxonomy_version=None,
        format=canonical_deck_input_dict.get("format") if isinstance(canonical_deck_input_dict.get("format"), str) else "commander",
        commander=canonical_deck_input_dict.get("commander") if isinstance(canonical_deck_input_dict.get("commander"), str) else "",
        cards=[name for name in canonical_deck_input_dict.get("cards", []) if isinstance(name, str)],
        engine_patches_v0=[],
    )
    baseline_build_started_at = perf_counter()
    baseline_build_payload = run_build_pipeline(req=build_req, conn=None, repo_root_path=REPO_ROOT)
    baseline_build_ms = _round6(max((perf_counter() - baseline_build_started_at) * 1000.0, 0.0))

    baseline_build_status_v1 = _coerce_nonempty_str(baseline_build_payload.get("status"))
    if baseline_build_status_v1 == "":
        baseline_build_status_v1 = "UNKNOWN"
    baseline_unknowns_raw = baseline_build_payload.get("unknowns") if isinstance(baseline_build_payload.get("unknowns"), list) else []
    baseline_unknowns_v1 = [dict(row) for row in baseline_unknowns_raw if isinstance(row, dict)]
    baseline_result_payload = baseline_build_payload.get("result") if isinstance(baseline_build_payload.get("result"), dict) else {}
    snapshot_preflight_v1 = (
        dict(baseline_result_payload.get("snapshot_preflight_v1"))
        if isinstance(baseline_result_payload.get("snapshot_preflight_v1"), dict)
        else None
    )

    # v1.2 Stage 1: accept OK_WITH_UNKNOWNS as a valid baseline status.
    # Mirrors the engine-layer fix at deck_complete_engine_v1.py:508. This
    # api/main.py guard runs BEFORE the engine layer is invoked — without
    # the matching expansion here, the engine fix has no effect. The two
    # guards must move in lockstep; per autonomous_repair_log this 1-line
    # mirror is the smallest possible additive edit consistent with the
    # spec's intent (populate added_cards_v1 on the imported-deck-with-
    # unresolved-cards path that live-retest 2026-05-10 exercised).
    # Response shape preserved BYTE-IDENTICAL — only the gate's accept set
    # widens. The completion-success path was already populating
    # added_cards_v1 correctly; this lift unblocks it for the OK_WITH_UNKNOWNS
    # baseline status that imports routinely return.
    if baseline_build_status_v1 not in {"OK", "WARN", "OK_WITH_UNKNOWNS"}:
        return DeckCompleteV1Response(
            status="ERROR",
            codes_v1=["BASELINE_BUILD_UNAVAILABLE"],
            db_snapshot_id=build_req.db_snapshot_id,
            format=build_req.format,
            baseline_summary_v1={},
            baseline_build_status_v1=baseline_build_status_v1,
            baseline_unknowns_v1=baseline_unknowns_v1,
            snapshot_preflight_v1=snapshot_preflight_v1,
            added_cards_v1=[],
            completed_decklist_text_v1=_build_commander_decklist_text_v1(
                commander=build_req.commander if isinstance(build_req.commander, str) else "",
                cards=[name for name in canonical_deck_input_dict.get("cards", []) if isinstance(name, str)],
            ),
            request_hash_v1=request_hash_v1,
            unknowns=unknowns,
            violations_v1=violations_v1,
            parse_version=ingest_payload.get("parse_version") if isinstance(ingest_payload.get("parse_version"), str) else "",
            resolve_version=ingest_payload.get("resolve_version") if isinstance(ingest_payload.get("resolve_version"), str) else "",
            ingest_version=ingest_payload.get("ingest_version") if isinstance(ingest_payload.get("ingest_version"), str) else "",
            complete_engine_version=DECK_COMPLETE_ENGINE_V1_VERSION,
        )

    complete_payload = run_deck_complete_engine_v1(
        canonical_deck_input=canonical_deck_input_dict,
        baseline_build_result=baseline_build_payload,
        db_snapshot_id=build_req.db_snapshot_id,
        bracket_id=req.bracket_id,
        profile_id=req.profile_id,
        mulligan_model_id=req.mulligan_model_id,
        target_deck_size=_coerce_positive_int(req.target_deck_size, default=100),
        max_adds=_coerce_positive_int(req.max_adds, default=30),
        allow_basic_lands=bool(req.allow_basic_lands),
        land_target_mode=_coerce_nonempty_str(req.land_target_mode) if _coerce_nonempty_str(req.land_target_mode) != "" else "AUTO",
        collect_dev_metrics=dev_metrics_enabled,
    )

    added_cards_raw = complete_payload.get("added_cards_v1") if isinstance(complete_payload.get("added_cards_v1"), list) else []
    added_cards_v1: List[DeckCompleteAddedCardV1] = []
    for row in added_cards_raw:
        if not isinstance(row, dict):
            continue
        name = _coerce_nonempty_str(row.get("name"))
        if name == "":
            continue
        reasons = sorted(
            {
                _coerce_nonempty_str(reason)
                for reason in (row.get("reasons_v1") if isinstance(row.get("reasons_v1"), list) else [])
                if _coerce_nonempty_str(reason) != ""
            }
        )
        primitives_added = sorted(
            {
                _coerce_nonempty_str(primitive)
                for primitive in (row.get("primitives_added_v1") if isinstance(row.get("primitives_added_v1"), list) else [])
                if _coerce_nonempty_str(primitive) != ""
            }
        )
        added_cards_v1.append(
            DeckCompleteAddedCardV1(
                name=name,
                reasons_v1=reasons,
                primitives_added_v1=primitives_added,
            )
        )

    complete_status = complete_payload.get("status") if isinstance(complete_payload.get("status"), str) else "WARN"
    complete_codes = sorted(
        {
            _coerce_nonempty_str(code)
            for code in (complete_payload.get("codes") if isinstance(complete_payload.get("codes"), list) else [])
            if _coerce_nonempty_str(code) != ""
        }
    )
    if complete_status == "WARN":
        if len(complete_codes) == 0:
            complete_codes = ["TARGET_SIZE_NOT_REACHED"]
        existing_codes = {row.code for row in violations_v1}
        for code in complete_codes:
            if code in existing_codes:
                continue
            violations_v1.append(
                DeckValidateViolationV1(
                    code=code,
                    card_name="",
                    count=1,
                    line_nos=[],
                    message=f"Deck completion warning: {code}",
                )
            )

    # v1.7.5 — adapt engine's bracket-combo violations into the strict
    # DeckValidateViolationV1 shape. The engine layer emits rich combo
    # metadata (card_a_name, card_b_name, combo_outcome_label, variant_id);
    # we pack card_a_name → card_name and let the message field carry the
    # full human-readable description. complete_status is already downgraded
    # to BRACKET_VIOLATION by the engine when these are present.
    engine_combo_violations = complete_payload.get("violations_v1")
    if isinstance(engine_combo_violations, list):
        for entry in engine_combo_violations:
            if not isinstance(entry, dict):
                continue
            code = _coerce_nonempty_str(entry.get("code"))
            if code == "":
                continue
            card_a = _coerce_nonempty_str(entry.get("card_a_name"))
            card_b = _coerce_nonempty_str(entry.get("card_b_name"))
            message = _coerce_nonempty_str(entry.get("message"))
            if message == "":
                message = f"Bracket violation: {code}"
            violations_v1.append(
                DeckValidateViolationV1(
                    code=code,
                    card_name=card_a,
                    count=1,
                    line_nos=[],
                    message=message,
                )
            )

    # Game-changer detection across (commander + cards + engine-added cards).
    # Mirrors pipeline_build's invocation pattern — names are coerced to a
    # flat string list before passing into detect_game_changers. The result
    # is a sorted unique list of card names from this deck that match the
    # GC userlist; UI uses it to badge AddedCardRow + deck overview rows.
    _gc_playable_names: List[str] = [
        name for name in canonical_deck_input_dict.get("cards", []) if isinstance(name, str)
    ]
    for _added_row in added_cards_v1:
        _added_name = _coerce_nonempty_str(_added_row.name)
        if _added_name != "":
            _gc_playable_names.append(_added_name)
    _gc_commander_name = canonical_deck_input_dict.get("commander") if isinstance(canonical_deck_input_dict.get("commander"), str) else None
    _game_changers_found, _ = detect_game_changers(
        playable_names=_gc_playable_names,
        commander_name=_gc_commander_name,
        gc_set=GAME_CHANGERS_SET,
    )

    response = DeckCompleteV1Response(
        status=complete_status,
        codes_v1=complete_codes,
        db_snapshot_id=build_req.db_snapshot_id,
        format=build_req.format,
        baseline_summary_v1=complete_payload.get("baseline_summary_v1") if isinstance(complete_payload.get("baseline_summary_v1"), dict) else {},
        baseline_build_status_v1=baseline_build_status_v1,
        baseline_unknowns_v1=baseline_unknowns_v1,
        snapshot_preflight_v1=snapshot_preflight_v1,
        added_cards_v1=added_cards_v1,
        game_changers_v1=_game_changers_found,
        completed_decklist_text_v1=(
            complete_payload.get("completed_decklist_text_v1")
            if isinstance(complete_payload.get("completed_decklist_text_v1"), str)
            else _build_commander_decklist_text_v1(
                commander=build_req.commander if isinstance(build_req.commander, str) else "",
                cards=[name for name in canonical_deck_input_dict.get("cards", []) if isinstance(name, str)],
            )
        ),
        # v1.7.2 Stage 1 — forward engine's deck-combo insight surfaces.
        detected_combos_v1=(
            complete_payload.get("detected_combos_v1")
            if isinstance(complete_payload.get("detected_combos_v1"), list)
            else []
        ),
        missing_partners_v1=(
            complete_payload.get("missing_partners_v1")
            if isinstance(complete_payload.get("missing_partners_v1"), list)
            else []
        ),
        # Phase 2.1a — forward engine's deck theme classification.
        deck_themes_v1=(
            complete_payload.get("deck_themes_v1")
            if isinstance(complete_payload.get("deck_themes_v1"), list)
            else []
        ),
        request_hash_v1=request_hash_v1,
        unknowns=unknowns,
        violations_v1=violations_v1,
        parse_version=ingest_payload.get("parse_version") if isinstance(ingest_payload.get("parse_version"), str) else "",
        resolve_version=ingest_payload.get("resolve_version") if isinstance(ingest_payload.get("resolve_version"), str) else "",
        ingest_version=ingest_payload.get("ingest_version") if isinstance(ingest_payload.get("ingest_version"), str) else "",
        complete_engine_version=(
            complete_payload.get("version")
            if isinstance(complete_payload.get("version"), str)
            else DECK_COMPLETE_ENGINE_V1_VERSION
        ),
    )

    if dev_metrics_enabled:
        complete_dev_metrics_raw = complete_payload.get("dev_metrics_v1") if isinstance(complete_payload.get("dev_metrics_v1"), dict) else {}
        stop_reason = _coerce_nonempty_str(complete_dev_metrics_raw.get("stop_reason_v1"))
        if stop_reason == "":
            stop_reason = "FILL_FAILED"

        dev_metrics_v1: Dict[str, Any] = {
            "stop_reason_v1": stop_reason,
            "nonland_added_count": _coerce_nonnegative_int(complete_dev_metrics_raw.get("nonland_added_count"), default=0),
            "land_fill_needed": _coerce_nonnegative_int(complete_dev_metrics_raw.get("land_fill_needed"), default=0),
            "land_fill_applied": _coerce_nonnegative_int(complete_dev_metrics_raw.get("land_fill_applied"), default=0),
            "candidate_pool_last_returned": _coerce_nonnegative_int(
                complete_dev_metrics_raw.get("candidate_pool_last_returned"),
                default=0,
            ),
            "baseline_build_ms": baseline_build_ms,
            "total_ms": _round6(max((perf_counter() - start_total_timer) * 1000.0, 0.0)),
        }
        filtered_illegal_count = complete_dev_metrics_raw.get("candidate_pool_filtered_illegal_count")
        if isinstance(filtered_illegal_count, int) and not isinstance(filtered_illegal_count, bool):
            dev_metrics_v1["candidate_pool_filtered_illegal_count"] = _coerce_nonnegative_int(filtered_illegal_count, default=0)

        print("COMPLETE DEBUG:")
        print(f"stop_reason_v1={dev_metrics_v1['stop_reason_v1']}")
        print(f"nonland_added_count={dev_metrics_v1['nonland_added_count']}")
        print(f"land_fill_needed={dev_metrics_v1['land_fill_needed']}")
        print(f"land_fill_applied={dev_metrics_v1['land_fill_applied']}")
        print(f"candidate_pool_last_returned={dev_metrics_v1['candidate_pool_last_returned']}")
        if "candidate_pool_filtered_illegal_count" in dev_metrics_v1:
            print(f"candidate_pool_filtered_illegal_count={dev_metrics_v1['candidate_pool_filtered_illegal_count']}")
        print(f"baseline_build_ms={dev_metrics_v1['baseline_build_ms']}")
        print(f"total_ms={dev_metrics_v1['total_ms']}")

        payload = response.model_dump(mode="python")
        payload["dev_metrics_v1"] = dev_metrics_v1
        return JSONResponse(content=payload)

    return response


@app.post("/agent/context_bundle_v1", response_model=AgentContextBundleV1Response)
async def agent_context_bundle_v1(req: AgentContextBundleV1Request):
    """Pillar A.5 — one-round-trip composite for AI agent kickoff.

    Composes /deck/analyze_v1 + /deck/candidate_pool_v1 +
    /deck/strength_check_v1 + /corpus/similar_decks_v1 (configurable via
    `include`). Speed target: <1000ms warm.
    """
    from api.engine.layers.agent_endpoints_v1 import compute_agent_context_bundle_v1

    result = compute_agent_context_bundle_v1(
        db_snapshot_id=req.db_snapshot_id,
        commander=req.commander,
        raw_decklist_text=req.raw_decklist_text,
        intent=req.intent,
        include=req.include,
    )
    return AgentContextBundleV1Response(**result)


@app.get("/commander/archetype_brief_v1", response_model=ArchetypeBriefV1Response)
async def commander_archetype_brief_v1(commander: str, db_snapshot_id: str):
    """Pillar A.5 — common archetypes + theme distribution + staples for a commander.

    Derived from corpus. Speed: <300ms warm.
    """
    from api.engine.layers.agent_endpoints_v1 import compute_archetype_brief_v1

    result = compute_archetype_brief_v1(
        db_snapshot_id=db_snapshot_id,
        commander=commander,
    )
    return ArchetypeBriefV1Response(**result)


@app.get("/theme/top_cards_v1", response_model=ThemeTopCardsV1Response)
async def theme_top_cards_v1(
    theme_id: str,
    db_snapshot_id: str,
    color_identity: Optional[str] = None,
    limit: int = 50,
):
    """Pillar A.5 — highest-signal cards for a theme.

    `color_identity` is a comma-separated string like "R,W". Speed: <300ms warm.
    """
    from api.engine.layers.agent_endpoints_v1 import compute_theme_top_cards_v1

    ci_list: Optional[List[str]] = None
    if isinstance(color_identity, str) and color_identity.strip():
        ci_list = [c.strip().upper() for c in color_identity.split(",") if c.strip()]

    result = compute_theme_top_cards_v1(
        db_snapshot_id=db_snapshot_id,
        theme_id=theme_id,
        color_identity=ci_list,
        limit=limit,
    )
    return ThemeTopCardsV1Response(**result)


@app.post("/agent/build_deck_v1", response_model=AgentBuildDeckV1Response)
async def agent_build_deck_v1(req: AgentBuildDeckV1Request):
    """Pillar D — AI deck-building agent.

    Builds a 99-card deck for `req.commander` honoring user intent (bracket,
    theme hints, must-include cards), respecting the creativity envelope.
    Phase A is a contract-stub (commander + 99 Wastes); Phases B-D add the
    candidate-pool / selection / validation algorithm.

    Speed budget: <15s end-to-end (Phase F target). Endpoint-call budget: <=30
    inter-layer calls per build, enforced by the layer.
    """
    from api.engine.layers.agent_build_deck_v1 import compute_agent_build_deck_v1

    result = compute_agent_build_deck_v1(
        db_snapshot_id=req.db_snapshot_id,
        commander=req.commander,
        bracket=req.bracket,
        theme_hints=req.theme_hints,
        must_include_cards=req.must_include_cards,
        max_iterations=req.max_iterations,
        seed=req.seed,
    )
    return AgentBuildDeckV1Response(**result)


@app.post("/agent/build_deck_v1/stream")
async def agent_build_deck_v1_stream(req: AgentBuildDeckV1Request):
    """Mega-task v5 Phase 3: SSE-streaming variant of /agent/build_deck_v1.

    Emits one Server-Sent Event per phase boundary so the UI can show the
    build's current phase + elapsed time + cumulative LLM cost during the
    110-150s wallclock. The final "complete" event carries the full
    AgentBuildDeckV1Response payload — clients render the deck from it.

    The non-streaming endpoint at /agent/build_deck_v1 still exists for
    Python tools / programmatic callers that don't need progress streaming.

    Event format (per SSE spec):
        data: {"phase": "...", "status": "started", "elapsed_s": 0.0, ...}\n\n
        data: {"phase": "complete", "status": "completed", "response": {...}}\n\n

    Cancellation: closing the connection aborts the SSE stream but the
    background build keeps running on the server (next iteration will add a
    cancel-token; this is acceptable for v5 since the build is bounded by
    ENDPOINT_CALL_BUDGET=30 anyway).
    """
    import asyncio
    import json
    import queue as _queue
    import threading

    from sse_starlette.sse import EventSourceResponse

    from api.engine.layers.agent_build_deck_v1 import compute_agent_build_deck_v1

    # Use a synchronous queue.Queue (NOT asyncio.Queue) because the build runs
    # in a thread via asyncio.to_thread; the producer is the sync callback in
    # that thread, the consumer is the async generator. A simple thread-safe
    # queue is the cleanest bridge.
    event_q: "_queue.Queue[dict]" = _queue.Queue()
    SENTINEL = object()
    error_holder: dict = {}

    def _on_progress(event: dict) -> None:
        event_q.put(event)

    def _run_build() -> None:
        try:
            compute_agent_build_deck_v1(
                db_snapshot_id=req.db_snapshot_id,
                commander=req.commander,
                bracket=req.bracket,
                theme_hints=req.theme_hints,
                must_include_cards=req.must_include_cards,
                max_iterations=req.max_iterations,
                seed=req.seed,
                progress_callback=_on_progress,
            )
        except Exception as exc:
            error_holder["error"] = f"{exc.__class__.__name__}: {exc}"
        finally:
            event_q.put(SENTINEL)

    # Kick off the build in a worker thread.
    build_task = asyncio.create_task(asyncio.to_thread(_run_build))

    async def event_generator():
        try:
            while True:
                # Drain the queue with a short timeout so the async loop stays
                # responsive (heartbeats / disconnect detection). We don't
                # currently emit heartbeats; uvicorn's idle timeout is generous
                # enough for 240s builds, but the loop structure leaves room.
                try:
                    item = await asyncio.to_thread(event_q.get, True, 1.0)
                except _queue.Empty:
                    # Could send a keep-alive comment here. For now just loop.
                    if build_task.done():
                        # The thread finished; drain anything left.
                        while True:
                            try:
                                item = event_q.get_nowait()
                            except _queue.Empty:
                                break
                            if item is SENTINEL:
                                break
                            yield {"event": "progress", "data": json.dumps(item)}
                        if error_holder.get("error"):
                            yield {"event": "error", "data": json.dumps({"error": error_holder["error"]})}
                        return
                    continue

                if item is SENTINEL:
                    # Build thread finished. If error, send error event.
                    if error_holder.get("error"):
                        yield {"event": "error", "data": json.dumps({"error": error_holder["error"]})}
                    return
                yield {"event": "progress", "data": json.dumps(item)}
        finally:
            # Best-effort: ensure the background task is awaited (it cannot be
            # cancelled mid-LLM-call, but at least we wait for it so the worker
            # thread doesn't leak).
            try:
                if not build_task.done():
                    await build_task
            except Exception:
                pass

    return EventSourceResponse(event_generator())


@app.post("/corpus/batch_ingest_v1", response_model=CorpusBatchIngestV1Response)
async def corpus_batch_ingest_v1(req: CorpusBatchIngestV1Request):
    """Phase 5a — batch-ingest external decks (EDHREC, cEDH-DB, etc.) into the
    strength oracle corpus.

    Each entry gets bracket cross-checked against engine analysis. Mismatches
    are written to `corpus_review_queue_v1.jsonl` for user review but are NOT
    rejected — they're flagged with `verification_status: "needs_review"` and
    still appended to the corpus so they're available with a warning.

    Hard rejections: missing commander, decklist < 50 cards, invalid bracket
    string.

    Audit-logged per DESIGN_DECISIONS rule 1.3 §4.
    """
    from api.engine.layers.corpus_batch_ingest_v1 import batch_ingest_external_decks
    entries = [e.model_dump(mode="python") for e in req.entries]
    result = batch_ingest_external_decks(
        db_snapshot_id=req.db_snapshot_id,
        entries=entries,
        skip_bracket_verification=req.skip_bracket_verification,
    )
    return CorpusBatchIngestV1Response(**result)


@app.post("/playtest/benchmark_v1", response_model=PlaytestBenchmarkV1Response)
async def playtest_benchmark_v1(req: PlaytestBenchmarkV1Request):
    """Phase 5b.3 benchmark runner.

    Runs N-pair anti-bias-mirrored games between two corpus decks, writes
    the result to calibration_log_v1.jsonl, returns aggregate winrate +
    per-game log + assertion check. Use with `benchmark_name="mirror_match"`
    + `expected_winrate_min=0.45 expected_winrate_max=0.55` to test anti-bias.
    """
    from api.engine.layers.deck_strength_check_v1 import _load_corpus
    from api.engine.playtest.mpa_calibration import run_match_anti_bias, MatchResult
    from api.engine.playtest.mpa_card_hydration import new_commander_game_hydrated
    from api.engine.playtest.mpa_runner import run_game, RUNNER_VERSION
    from api.engine.playtest.mpa_policy import POLICY_VERSION
    from api.engine.playtest.mpa_calibration_log import log_calibration_run, overall_calibration_status

    warnings: List[Dict[str, str]] = []

    # Resolve corpus decks. Access via module attribute, NOT direct import:
    # `_load_corpus()` rebinds the module-level `_CORPUS_RAW` to a fresh
    # dict, so `from ... import _CORPUS_RAW` would capture the empty pre-
    # load value. See corpus_batch_ingest_v1.py / agent_endpoints_v1.py
    # for the same pattern.
    _load_corpus()
    from api.engine.layers import deck_strength_check_v1 as _sc
    deck_a_entry = None
    deck_b_entry = None
    for entry in _sc._CORPUS_RAW.get("decks", []) or []:
        cid = entry.get("corpus_id")
        if cid == req.deck_a_corpus_id:
            deck_a_entry = entry
        if cid == req.deck_b_corpus_id:
            deck_b_entry = entry
    if not deck_a_entry or not deck_b_entry:
        warnings.append({
            "code": "CORPUS_ID_NOT_FOUND",
            "message": f"deck_a={bool(deck_a_entry)}, deck_b={bool(deck_b_entry)}",
        })
        return PlaytestBenchmarkV1Response(
            benchmark_name=req.benchmark_name,
            deck_a_label=req.deck_a_corpus_id, deck_b_label=req.deck_b_corpus_id,
            n_games=0, deck_a_winrate=0.0,
            overall_calibration_status=overall_calibration_status(),
            warnings=warnings,
        )

    # Run anti-bias mirrored games
    match = MatchResult()
    for pair_idx in range(max(1, int(req.n_game_pairs))):
        for swap in (False, True):
            if swap:
                state = new_commander_game_hydrated(
                    db_snapshot_id=req.db_snapshot_id,
                    commander_a_name=deck_b_entry.get("commander"),
                    deck_a_names=deck_b_entry.get("decklist", []),
                    commander_b_name=deck_a_entry.get("commander"),
                    deck_b_names=deck_a_entry.get("decklist", []),
                    seed=400 + pair_idx * 2 + 1,
                )
                deck_a_seat = 1
            else:
                state = new_commander_game_hydrated(
                    db_snapshot_id=req.db_snapshot_id,
                    commander_a_name=deck_a_entry.get("commander"),
                    deck_a_names=deck_a_entry.get("decklist", []),
                    commander_b_name=deck_b_entry.get("commander"),
                    deck_b_names=deck_b_entry.get("decklist", []),
                    seed=400 + pair_idx * 2,
                )
                deck_a_seat = 0
            game = run_game(state, max_turns=req.max_turns_per_game)
            match.games_played += 1
            if game.winner_seat is None:
                match.draws += 1
                outcome = "draw"
            elif game.winner_seat == deck_a_seat:
                match.deck_a_wins += 1
                outcome = "A_wins"
            else:
                match.deck_b_wins += 1
                outcome = "B_wins"
            match.games_log.append({
                "pair": pair_idx, "swap": swap, "outcome": outcome,
                "turns": game.final_turn, "loss_reason": game.loss_reason,
            })

    # Write to calibration log
    log_entry = log_calibration_run(
        benchmark_name=req.benchmark_name,
        deck_a_label=req.deck_a_corpus_id,
        deck_b_label=req.deck_b_corpus_id,
        n_games=match.games_played,
        deck_a_winrate=match.deck_a_winrate,
        expected_winrate_min=req.expected_winrate_min,
        expected_winrate_max=req.expected_winrate_max,
        mpa_version=POLICY_VERSION,
        runner_version=RUNNER_VERSION,
        extra={"draws": match.draws, "deck_b_wins": match.deck_b_wins},
    )

    return PlaytestBenchmarkV1Response(
        benchmark_name=req.benchmark_name,
        deck_a_label=req.deck_a_corpus_id,
        deck_b_label=req.deck_b_corpus_id,
        n_games=match.games_played,
        deck_a_winrate=round(match.deck_a_winrate, 4),
        expected_winrate_min=req.expected_winrate_min,
        expected_winrate_max=req.expected_winrate_max,
        passes_assertion=log_entry.get("passes_assertion"),
        overall_calibration_status=overall_calibration_status(),
        per_game_log=match.games_log,
        warnings=warnings,
    )


@app.get("/playtest/opposition_decks_v1")
async def playtest_opposition_decks_v1(
    bracket: Optional[str] = None,
    role_tag: Optional[str] = None,
):
    """Phase 5b.5 — curated opposition deck registry.

    Returns the canonical opposition decks the calibration suite uses as
    standardized opponents per bracket. Each entry pins a corpus deck via
    `corpus_id` + a stable `role_tag` that calibration runs reference.

    Query params (optional):
      - bracket=B1..B5 — filter to a single bracket
      - role_tag=<tag> — return only the entry matching that role_tag
        (returns 404 if not found). When `role_tag` is given, `bracket` is
        ignored.
    """
    from api.engine.playtest.opposition_decks_v1 import (
        load_registry, filter_by_bracket, get_by_role_tag, registry_summary,
    )

    if isinstance(role_tag, str) and role_tag.strip() != "":
        entry = get_by_role_tag(role_tag.strip())
        if entry is None:
            return {
                "version": "opposition_decks_v1",
                "summary": registry_summary(),
                "entries": [],
                "warnings": [{"code": "ROLE_TAG_NOT_FOUND", "message": f"No entry with role_tag={role_tag!r}"}],
            }
        return {
            "version": "opposition_decks_v1",
            "summary": registry_summary(),
            "entries": [entry],
            "warnings": [],
        }

    if isinstance(bracket, str) and bracket.strip() != "":
        norm = bracket.strip().upper()
        if norm not in {"B1", "B2", "B3", "B4", "B5"}:
            return {
                "version": "opposition_decks_v1",
                "summary": registry_summary(),
                "entries": [],
                "warnings": [{"code": "INVALID_BRACKET", "message": f"bracket must be one of B1..B5, got {bracket!r}"}],
            }
        return {
            "version": "opposition_decks_v1",
            "summary": registry_summary(),
            "entries": filter_by_bracket(norm),
            "warnings": [],
        }

    reg = load_registry()
    return {
        "version": reg.get("version", "opposition_decks_v1"),
        "summary": registry_summary(),
        "entries": reg.get("entries", []),
        "warnings": [],
    }


@app.post("/deck/save_to_library_v1", response_model=DeckSaveToLibraryV1Response)
async def deck_save_to_library_v1(req: DeckSaveToLibraryV1Request):
    """Pillar C.2 — save deck as Obsidian DECK_LIBRARY page.

    Writes a markdown page under `Mtg deck building brain/20_DECK_LIBRARY/`
    with themes, gaps, bracket, strength check, and full decklist. Optionally
    ingests the deck into the strength-oracle corpus (self-learning) when
    `ingest_to_corpus=true`.

    Library path can be overridden via env var `MTG_DECK_LIBRARY_PATH`.
    """
    from api.engine.layers.deck_library_v1 import save_deck_to_library_v1

    result = save_deck_to_library_v1(
        db_snapshot_id=req.db_snapshot_id,
        commander=req.commander,
        raw_decklist_text=req.raw_decklist_text,
        deck_name=req.deck_name,
        archetype=req.archetype,
        bracket=req.bracket,
        user_id=req.user_id,
        ingest_to_corpus=req.ingest_to_corpus,
    )
    return DeckSaveToLibraryV1Response(**result)


@app.post("/corpus/similar_decks_v1", response_model=CorpusSimilarDecksV1Response)
async def corpus_similar_decks_v1(req: CorpusSimilarDecksV1Request):
    """Pillar A.5 — k corpus decks most similar to the given deck.

    Optionally embeds the full decklist for each. Speed: <500ms warm.
    """
    from api.engine.layers.agent_endpoints_v1 import compute_corpus_similar_decks_v1

    result = compute_corpus_similar_decks_v1(
        db_snapshot_id=req.db_snapshot_id,
        commander=req.commander,
        raw_decklist_text=req.raw_decklist_text,
        k=req.k,
        include_decklists=req.include_decklists,
    )
    return CorpusSimilarDecksV1Response(**result)


@app.post("/deck/strength_check_v1", response_model=DeckStrengthCheckV1Response)
async def deck_strength_check_v1(req: DeckStrengthCheckV1Request):
    """Pillar A.4 — strength oracle (Measurement A: corpus similarity).

    Returns cosine-similarity-ranked nearest corpus neighbors + axis-deviation
    flags + archetype consensus + interpretation. Measurement B (playtest
    win rate) wires in after Phase 5b.

    Speed target: <500ms warm. First call vectorizes the corpus
    (one-time cost per snapshot); subsequent calls hit the cached vectors.
    """
    from api.engine.layers.deck_strength_check_v1 import compute_deck_strength_check_v1

    result = compute_deck_strength_check_v1(
        db_snapshot_id=req.db_snapshot_id,
        commander=req.commander,
        raw_decklist_text=req.raw_decklist_text,
        include_measurement_b=req.include_measurement_b,
        k_nearest=req.k_nearest,
    )
    return DeckStrengthCheckV1Response(**result)


@app.post("/deck/candidate_pool_v1", response_model=DeckCandidatePoolV1Response)
async def deck_candidate_pool_v1(req: DeckCandidatePoolV1Request):
    """Pillar A.3 — clustered wide candidate pool.

    Returns ≥100 candidate cards clustered by which axis they shore up
    (ramp/draw/removal/protection + per-theme deepening). Each cluster
    has rationale + annotated candidates. NEVER returns top-N — AI ranks
    within each cluster.

    Speed target: <500ms warm (composes analyze + 5-8 card_search calls).
    """
    from api.engine.layers.deck_candidate_pool_v1 import compute_candidate_pool_v1

    result = compute_candidate_pool_v1(
        db_snapshot_id=req.db_snapshot_id,
        commander=req.commander,
        raw_decklist_text=req.raw_decklist_text,
        intent=req.intent,
        extra_filters=req.extra_filters,
        min_pool_size=req.min_pool_size,
        max_pool_size=req.max_pool_size,
    )
    return DeckCandidatePoolV1Response(**result)


@app.post("/card/search_v1", response_model=CardSearchV1Response)
async def card_search_v1(req: CardSearchV1Request):
    """Pillar A.2 — rich-filter card lookup over the snapshot.

    Returns wide annotated pools. Sort options are structural (name, cmc,
    color_identity_size) — never a quality score, per the creativity envelope
    rule in DESIGN_DECISIONS.md.

    Speed target: <500ms warm. Uses the primitive_to_cards inverted index
    table for primitive filters; SQL handles cmc/type/subtype/name filters
    inline; color_identity does post-fetch JSON parse.
    """
    from api.engine.layers.card_search_v1 import search_cards_v1

    result = search_cards_v1(
        db_snapshot_id=req.db_snapshot_id,
        taxonomy_version=req.taxonomy_version,
        filters=req.filters.model_dump(mode="python"),
        limit=req.limit,
        offset=req.offset,
        sort_by=req.sort_by,
        include=req.include,
    )
    return CardSearchV1Response(**result)


@app.post("/deck/analyze_v1", response_model=DeckAnalyzeV1Response)
async def deck_analyze_v1(req: DeckAnalyzeV1Request):
    """Pillar A.1 — AI-facing analyze endpoint.

    Returns a structured diagnostic snapshot of the deck. Designed for
    AI agents to call once at the start of a build/improve session.
    No bracket_id input — the endpoint estimates the natural bracket.

    Architectural rules served (see DESIGN_DECISIONS.md):
      - 1.1 Creativity envelope: returns structural facts only, no
        candidate cards or rankings.
      - 1.2 Speed budget: <500ms warm response target.
      - 1.3 Strength oracle: gap_signal returns [] until Pillar A.4
        corpus ships (calibration-honest, no fabricated axis norms).
    """
    from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1

    result = compute_deck_analyze_v1(
        db_snapshot_id=req.db_snapshot_id,
        commander=req.commander,
        raw_decklist_text=req.raw_decklist_text,
        include_debug=req.include_debug,
    )

    # Pop the optional debug payload into the strict response field
    debug = result.pop("_debug", None) if isinstance(result, dict) else None
    if debug is not None:
        result["debug"] = debug

    return DeckAnalyzeV1Response(**result)


@app.post("/strategy_hypothesis_v0")
def strategy_hypothesis_v0(req: StrategyHypothesisRequest):
    return generate_strategy_hypotheses_v0(
        anchor_cards=req.anchor_cards,
        commander=req.commander,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        max_packages_per_hypothesis=req.max_packages_per_hypothesis,
        max_cards_per_package=req.max_cards_per_package,
        validate_packages=req.validate_packages,
    )


@app.post("/deck_complete_v0")
def deck_complete_v0(req: DeckCompleteRequest):
    response_payload = generate_deck_completion_v0(
        commander=req.commander,
        anchors=req.anchors,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        max_iters=req.max_iters,
        target_deck_size=req.target_deck_size,
        seed_package=req.seed_package,
        validate_each_iter=req.validate_each_iter,
        db_snapshot_id=req.db_snapshot_id,
        refine=req.refine,
        max_refine_iters=req.max_refine_iters,
        swap_batch_size=req.swap_batch_size,
        validate_each_refine_iter=req.validate_each_refine_iter,
    )

    if req.save_run:
        try:
            request_payload = req.model_dump() if hasattr(req, "model_dump") else dict(req)
        except Exception:
            request_payload = {
                "commander": req.commander,
                "anchors": req.anchors,
                "profile_id": req.profile_id,
                "bracket_id": req.bracket_id,
                "max_iters": req.max_iters,
                "target_deck_size": req.target_deck_size,
                "seed_package": req.seed_package,
                "validate_each_iter": req.validate_each_iter,
                "db_snapshot_id": req.db_snapshot_id,
                "refine": req.refine,
                "max_refine_iters": req.max_refine_iters,
                "swap_batch_size": req.swap_batch_size,
                "validate_each_refine_iter": req.validate_each_refine_iter,
                "save_run": req.save_run,
            }

        save_run_v0(
            db_path=_runtime_db_path_str(),
            endpoint="deck_complete_v0",
            request=request_payload,
            response=response_payload,
            meta={
                "engine_version": ENGINE_VERSION,
            },
        )

    return response_payload


@app.get("/runs_v0")
def runs_v0(limit: int = 50, endpoint: Optional[str] = None):
    rows = list_runs_v0(
        db_path=_runtime_db_path_str(),
        limit=limit,
        endpoint=endpoint,
    )
    return {"runs": rows}


@app.get("/run_v0/{run_id}")
def run_v0(run_id: str):
    return get_run_v0(db_path=_runtime_db_path_str(), run_id=run_id)


@app.get("/run_diff_v0")
def run_diff_v0(run_id_a: str, run_id_b: str):
    return diff_runs_v0(
        db_path=_runtime_db_path_str(),
        run_id_a=run_id_a,
        run_id_b=run_id_b,
    )


def _run_bundle_error(message: str, run_id: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "status": "ERROR",
        "message": message,
    }
    if isinstance(run_id, str):
        payload["run_id"] = run_id
    return payload


def _attach_optional_run_diff(
    bundle_payload: Dict[str, Any],
    current_run_id: str,
    previous_run_id: Optional[str],
) -> None:
    if not isinstance(previous_run_id, str):
        return
    previous_run_id_clean = previous_run_id.strip()
    if previous_run_id_clean == "":
        return

    bundle_payload["previous_run_id"] = previous_run_id_clean
    bundle_payload["run_diff_v0"] = diff_runs_v0(
        db_path=_runtime_db_path_str(),
        run_id_a=previous_run_id_clean,
        run_id_b=current_run_id,
    )


@app.get("/run_bundle_v0")
def run_bundle_v0(run_id: str, previous_run_id: Optional[str] = None):
    run_obj = get_run_v0(db_path=_runtime_db_path_str(), run_id=run_id)
    if not isinstance(run_obj, dict):
        return _run_bundle_error(message="Run not found", run_id=run_id)

    bundle_payload = build_run_bundle_v0(run_obj)
    current_run_id = bundle_payload.get("run_id") if isinstance(bundle_payload.get("run_id"), str) else run_id
    _attach_optional_run_diff(
        bundle_payload=bundle_payload,
        current_run_id=current_run_id,
        previous_run_id=previous_run_id,
    )

    return {
        "status": "OK",
        "run_bundle_v0": bundle_payload,
    }


@app.get("/run_bundle_v0/latest")
def run_bundle_v0_latest(
    endpoint: str = "deck_complete_v0",
    profile_id: Optional[str] = None,
    bracket_id: Optional[str] = None,
    previous_run_id: Optional[str] = None,
):
    endpoint_value = endpoint.strip() if isinstance(endpoint, str) and endpoint.strip() != "" else "deck_complete_v0"
    profile_id_filter = profile_id.strip() if isinstance(profile_id, str) and profile_id.strip() != "" else None
    bracket_id_filter = bracket_id.strip() if isinstance(bracket_id, str) and bracket_id.strip() != "" else None

    rows = list_runs_v0(
        db_path=_runtime_db_path_str(),
        limit=500,
        endpoint=endpoint_value,
    )

    selected_row = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if profile_id_filter is not None and row.get("profile_id") != profile_id_filter:
            continue
        if bracket_id_filter is not None and row.get("bracket_id") != bracket_id_filter:
            continue
        selected_row = row
        break

    if not isinstance(selected_row, dict):
        return {
            "status": "ERROR",
            "message": "No saved run found for requested filters",
            "filters": {
                "endpoint": endpoint_value,
                "profile_id": profile_id_filter,
                "bracket_id": bracket_id_filter,
            },
        }

    selected_run_id = selected_row.get("run_id")
    if not isinstance(selected_run_id, str) or selected_run_id == "":
        return _run_bundle_error(message="Latest run row missing run_id")

    run_obj = get_run_v0(db_path=_runtime_db_path_str(), run_id=selected_run_id)
    if not isinstance(run_obj, dict):
        return _run_bundle_error(message="Run not found", run_id=selected_run_id)

    bundle_payload = build_run_bundle_v0(run_obj)
    current_run_id = (
        bundle_payload.get("run_id") if isinstance(bundle_payload.get("run_id"), str) else selected_run_id
    )
    _attach_optional_run_diff(
        bundle_payload=bundle_payload,
        current_run_id=current_run_id,
        previous_run_id=previous_run_id,
    )

    return {
        "status": "OK",
        "run_bundle_v0": bundle_payload,
        "latest_lookup": {
            "endpoint": endpoint_value,
            "profile_id": profile_id_filter,
            "bracket_id": bracket_id_filter,
            "created_at": selected_row.get("created_at"),
        },
    }


@app.get("/primitive_tag_index_v0/status")
def primitive_tag_index_v0_status():
    snapshots_latest = list_snapshots(limit=1)
    db_snapshot_id = (
        snapshots_latest[0].get("snapshot_id")
        if snapshots_latest and isinstance(snapshots_latest[0].get("snapshot_id"), str)
        else None
    )
    status_payload = get_primitive_tag_index_status_v0(
        db_path=_runtime_db_path_str(),
        db_snapshot_id=db_snapshot_id,
    )
    return {
        "status": "OK",
        "primitive_tag_index_v0": status_payload,
    }


@app.get("/primitive_tag_index_v0/primitive/{primitive_id}")
def primitive_tag_index_v0_primitive(
    primitive_id: str,
    ruleset_version: Optional[str] = None,
    limit: int = 50,
):
    ruleset_version_value = resolve_ruleset_version_v0(
        db_path=_runtime_db_path_str(),
        requested_ruleset_version=ruleset_version,
    )
    if not isinstance(ruleset_version_value, str) or ruleset_version_value == "":
        return {
            "status": "ERROR",
            "message": "No primitive tag index ruleset available",
        }

    rows = get_cards_for_primitive_v0(
        db_path=_runtime_db_path_str(),
        ruleset_version=ruleset_version_value,
        primitive_id=primitive_id,
        limit=limit,
    )
    return {
        "status": "OK",
        "primitive_id": primitive_id,
        "ruleset_version": ruleset_version_value,
        "cards": rows,
    }


if UI_STATIC_ENABLED:

    @app.get("/", include_in_schema=False)
    def serve_index():
        return FileResponse(UI_INDEX_PATH)


    @app.get("/{full_path:path}", include_in_schema=False)
    def serve_spa(full_path: str):
        normalized_full_path = full_path.strip().lstrip("/")
        if normalized_full_path.startswith("api") or normalized_full_path.startswith("cards"):
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(UI_INDEX_PATH)
