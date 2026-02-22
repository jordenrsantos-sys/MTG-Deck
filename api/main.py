import json
import os
from datetime import datetime
from time import perf_counter
from typing import Optional, Dict, Any, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from engine.db import DB_PATH as CARDS_DB_PATH, connect as cards_db_connect, list_snapshots
from api.engine.constants import (
    ENGINE_VERSION,
    RULESET_VERSION,
    BRACKET_DEFINITION_VERSION,
    GAME_CHANGERS_VERSION,
    REPO_ROOT,
)
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
from api.engine.pipeline_build import run_build_pipeline
from api.engine.run_history_v0 import diff_runs_v0, get_run_v0, list_runs_v0, save_run_v0
from api.engine.run_bundle_v0 import build_run_bundle_v0
from api.engine.strategy_hypothesis_v0 import generate_strategy_hypotheses_v0
from api.engine.tag_index_query_v0 import (
    get_cards_for_primitive_v0,
    get_primitive_tag_index_status_v0,
    resolve_ruleset_version_v0,
)


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
    db_snapshot_id: str
    format: str
    baseline_summary_v1: Dict[str, Any]
    added_cards_v1: List[DeckCompleteAddedCardV1]
    completed_decklist_text_v1: str
    request_hash_v1: str
    unknowns: List[DecklistUnknownV1]
    violations_v1: List[DeckValidateViolationV1]
    parse_version: str
    resolve_version: str
    ingest_version: str
    complete_engine_version: str


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


app = FastAPI(title="MTG Strategy Engine", version=ENGINE_VERSION)

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


@app.get("/health")
def health():
    return {
        "ok": True,
        "engine_version": ENGINE_VERSION,
        "time": datetime.utcnow().isoformat(),
    }


@app.get("/snapshots")
def snapshots(limit: int = 20):
    return {"snapshots": list_snapshots(limit=limit)}


@app.get("/cards/suggest")
def cards_suggest(q: str, snapshot_id: Optional[str] = None, limit: int = 20):
    query = _coerce_nonempty_str(q).lower()
    normalized_snapshot_id = _coerce_nonempty_str(snapshot_id)
    safe_limit = min(max(_coerce_positive_int(limit, default=20), 1), 20)

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
                (normalized_snapshot_id, query + "%", safe_limit),
            ).fetchall()

            results: List[Dict[str, Any]] = []
            dedupe_keys: set[str] = set()

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
                    results.append(result_row)
                    if len(results) >= safe_limit:
                        return

            _append_rows(prefix_rows)

            if len(results) < safe_limit:
                remaining = safe_limit - len(results)
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

    tune_payload = run_deck_tune_engine_v1(
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
def deck_complete_v1(req: DeckCompleteV1Request):
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
            db_snapshot_id=req.db_snapshot_id,
            format=canonical_deck_input_dict.get("format") if isinstance(canonical_deck_input_dict.get("format"), str) else "commander",
            baseline_summary_v1={},
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

    response = DeckCompleteV1Response(
        status=complete_status,
        db_snapshot_id=build_req.db_snapshot_id,
        format=build_req.format,
        baseline_summary_v1=complete_payload.get("baseline_summary_v1") if isinstance(complete_payload.get("baseline_summary_v1"), dict) else {},
        added_cards_v1=added_cards_v1,
        completed_decklist_text_v1=(
            complete_payload.get("completed_decklist_text_v1")
            if isinstance(complete_payload.get("completed_decklist_text_v1"), str)
            else _build_commander_decklist_text_v1(
                commander=build_req.commander if isinstance(build_req.commander, str) else "",
                cards=[name for name in canonical_deck_input_dict.get("cards", []) if isinstance(name, str)],
            )
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
            db_path=str(CARDS_DB_PATH),
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
        db_path=str(CARDS_DB_PATH),
        limit=limit,
        endpoint=endpoint,
    )
    return {"runs": rows}


@app.get("/run_v0/{run_id}")
def run_v0(run_id: str):
    return get_run_v0(db_path=str(CARDS_DB_PATH), run_id=run_id)


@app.get("/run_diff_v0")
def run_diff_v0(run_id_a: str, run_id_b: str):
    return diff_runs_v0(
        db_path=str(CARDS_DB_PATH),
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
        db_path=str(CARDS_DB_PATH),
        run_id_a=previous_run_id_clean,
        run_id_b=current_run_id,
    )


@app.get("/run_bundle_v0")
def run_bundle_v0(run_id: str, previous_run_id: Optional[str] = None):
    run_obj = get_run_v0(db_path=str(CARDS_DB_PATH), run_id=run_id)
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
        db_path=str(CARDS_DB_PATH),
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

    run_obj = get_run_v0(db_path=str(CARDS_DB_PATH), run_id=selected_run_id)
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
        db_path=str(CARDS_DB_PATH),
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
        db_path=str(CARDS_DB_PATH),
        requested_ruleset_version=ruleset_version,
    )
    if not isinstance(ruleset_version_value, str) or ruleset_version_value == "":
        return {
            "status": "ERROR",
            "message": "No primitive tag index ruleset available",
        }

    rows = get_cards_for_primitive_v0(
        db_path=str(CARDS_DB_PATH),
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
