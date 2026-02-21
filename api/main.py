import os
from datetime import datetime
from typing import Optional, Dict, Any, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from engine.db import DB_PATH as CARDS_DB_PATH, list_snapshots
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
from api.engine.deck_completion_v0 import generate_deck_completion_v0
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
