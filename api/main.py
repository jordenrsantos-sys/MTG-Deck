from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from engine.db import (
    snapshot_exists,
    find_card_by_name,
    suggest_card_names,
    list_snapshots,
    is_legal_commander_card,
)


ENGINE_VERSION = "0.1.0"


class BuildRequest(BaseModel):
    db_snapshot_id: str = Field(..., description="Required snapshot ID")
    profile_id: str = Field(..., description="Profile ID")
    bracket_id: str = Field(..., description="Bracket definition ID")

    format: str = "commander"
    commander: Optional[str] = None
    seed_cards: List[str] = Field(default_factory=list)


class BuildResponse(BaseModel):
    engine_version: str
    ruleset_version: str
    bracket_definition_version: str
    game_changers_version: str
    db_snapshot_id: str
    profile_id: str
    bracket_id: str

    status: str
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
    # 1) Snapshot gating
    if not snapshot_exists(req.db_snapshot_id):
        return BuildResponse(
            engine_version=ENGINE_VERSION,
            ruleset_version="UNSET",
            bracket_definition_version="UNSET",
            game_changers_version="UNSET",
            db_snapshot_id=req.db_snapshot_id,
            profile_id=req.profile_id,
            bracket_id=req.bracket_id,
            status="UNKNOWN_SNAPSHOT",
            unknowns=[
                {
                    "code": "UNKNOWN_SNAPSHOT",
                    "snapshot_id": req.db_snapshot_id,
                    "message": "Snapshot ID not found in local DB.",
                }
            ],
            result={},
        )

    # 2) Resolve commander (Commander format requires it)
    commander_resolved = None

    if req.format == "commander":
        if not req.commander:
            return BuildResponse(
                engine_version=ENGINE_VERSION,
                ruleset_version="UNSET",
                bracket_definition_version="UNSET",
                game_changers_version="UNSET",
                db_snapshot_id=req.db_snapshot_id,
                profile_id=req.profile_id,
                bracket_id=req.bracket_id,
                status="MISSING_COMMANDER",
                unknowns=[
                    {
                        "code": "MISSING_COMMANDER",
                        "message": "format=commander requires a commander name.",
                    }
                ],
                result={},
            )

        commander_resolved = find_card_by_name(req.db_snapshot_id, req.commander)

        # 2a) Unknown commander (not found in DB)
        if commander_resolved is None:
            return BuildResponse(
                engine_version=ENGINE_VERSION,
                ruleset_version="UNSET",
                bracket_definition_version="UNSET",
                game_changers_version="UNSET",
                db_snapshot_id=req.db_snapshot_id,
                profile_id=req.profile_id,
                bracket_id=req.bracket_id,
                status="UNKNOWN_COMMANDER",
                unknowns=[
                    {
                        "code": "UNKNOWN_COMMANDER",
                        "input": req.commander,
                        "message": "Commander not found in local snapshot by exact name match.",
                        "suggestions": suggest_card_names(
                            req.db_snapshot_id, req.commander, limit=5
                        ),
                    }
                ],
                result={},
            )

        # 2b) Commander legality check (only AFTER we found the card)
        legal, reason = is_legal_commander_card(commander_resolved)
        if not legal:
            return BuildResponse(
                engine_version=ENGINE_VERSION,
                ruleset_version="UNSET",
                bracket_definition_version="UNSET",
                game_changers_version="UNSET",
                db_snapshot_id=req.db_snapshot_id,
                profile_id=req.profile_id,
                bracket_id=req.bracket_id,
                status="ILLEGAL_COMMANDER",
                unknowns=[
                    {
                        "code": "ILLEGAL_COMMANDER",
                        "input": req.commander,
                        "message": (
                            "Card is not a legal Commander (must be Legendary Creature/Legend "
                            "Creature or explicitly allowed by oracle text)."
                        ),
                        "reason": reason,
                        "suggestions": suggest_card_names(
                            req.db_snapshot_id, req.commander, limit=5
                        ),
                    }
                ],
                result={},
            )

    # 3) Resolve seed cards
    unknowns: List[Dict[str, Any]] = []
    resolved_seeds: List[Dict[str, Any]] = []

    for name in req.seed_cards:
        card = find_card_by_name(req.db_snapshot_id, name)
        if card is None:
            unknowns.append(
                {
                    "code": "UNKNOWN_CARD",
                    "input": name,
                    "message": "Card not found in local snapshot by exact name match.",
                    "suggestions": suggest_card_names(req.db_snapshot_id, name, limit=5),
                }
            )
        else:
            resolved_seeds.append(card)

    status = "OK" if not unknowns else "OK_WITH_UNKNOWNS"

    return BuildResponse(
        engine_version=ENGINE_VERSION,
        ruleset_version="UNSET",
        bracket_definition_version="UNSET",
        game_changers_version="UNSET",
        db_snapshot_id=req.db_snapshot_id,
        profile_id=req.profile_id,
        bracket_id=req.bracket_id,
        status=status,
        unknowns=unknowns,
        result={
            "format": req.format,
            "commander": req.commander,
            "commander_resolved": commander_resolved,
            "seed_cards_input": req.seed_cards,
            "seed_cards_resolved": resolved_seeds,
            "note": "DB lookup v1: snapshot gating + exact-name resolution + suggestions.",
        },
    )
