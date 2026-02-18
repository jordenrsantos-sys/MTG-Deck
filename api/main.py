from datetime import datetime
from typing import Optional, Dict, Any, List

from fastapi import FastAPI
from pydantic import BaseModel, Field

from engine.db import list_snapshots
from api.engine.constants import (
    ENGINE_VERSION,
    RULESET_VERSION,
    BRACKET_DEFINITION_VERSION,
    GAME_CHANGERS_VERSION,
    REPO_ROOT,
)
from api.engine.pipeline_build import run_build_pipeline


class BuildRequest(BaseModel):
    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    profile_id: str = Field(..., description="Profile ID")
    bracket_id: str = Field(..., description="Bracket definition ID")

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
    unknowns: List[Dict[str, Any]]
    result: Dict[str, Any]


app = FastAPI(title="MTG Strategy Engine", version=ENGINE_VERSION)


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
    payload = run_build_pipeline(req=req, conn=None, repo_root_path=REPO_ROOT)
    return BuildResponse(**payload)
