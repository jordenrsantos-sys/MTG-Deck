"""
mtg_engine_mcp — MCP server wrapping the MTG engine's AI-facing surface.

Exposes the 7 Pillar A endpoints as MCP tools so Claude Code (and any other
MCP-aware AI agent) can call them as typed tool invocations rather than raw
HTTP requests.

Tools exposed:
  - analyze_deck            → POST /deck/analyze_v1
  - search_cards            → POST /card/search_v1
  - candidate_pool          → POST /deck/candidate_pool_v1
  - strength_check          → POST /deck/strength_check_v1
  - agent_context_bundle    → POST /agent/context_bundle_v1
  - commander_archetype_brief → GET  /commander/archetype_brief_v1
  - theme_top_cards         → GET  /theme/top_cards_v1
  - corpus_similar_decks    → POST /corpus/similar_decks_v1

Transport: stdio (local). Configured in Claude Code's .mcp.json with:

  {
    "mcpServers": {
      "mtg-engine": {
        "command": "python3",
        "args": ["/path/to/mtg-engine/repo/mcp_server/mtg_engine_mcp.py"],
        "env": {"MTG_ENGINE_URL": "http://localhost:8000"}
      }
    }
  }

Architecture rules served (DESIGN_DECISIONS.md):
  - 1.1 Creativity envelope: tool descriptions explicitly say "wide pool,
    not top-N" so the AI knows not to expect ranking.
  - 1.2 Speed budget: HTTP timeouts set conservatively (10s); engine endpoints
    respect their own <500ms/1000ms budgets.
"""
from __future__ import annotations

import os
import json
import asyncio
from typing import Any, Dict, List, Optional

import httpx
from mcp.server.fastmcp import FastMCP


ENGINE_URL = os.environ.get("MTG_ENGINE_URL", "http://localhost:8000")
DEFAULT_TIMEOUT = 10.0

mcp = FastMCP("mtg-engine")


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=ENGINE_URL, timeout=DEFAULT_TIMEOUT)


# ============================================================
# Pillar A.1 — analyze_deck
# ============================================================


@mcp.tool()
async def analyze_deck(
    db_snapshot_id: str,
    raw_decklist_text: str,
    commander: Optional[str] = None,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """Diagnostic snapshot of a deck.

    Returns themes, primitive density, subtype density, mana curve, color
    identity, bracket estimate, detected combos, and gap signal. The
    one-call kickoff for an AI agent starting a build or improve session.

    Args:
        db_snapshot_id: Required snapshot identifier (e.g., "20260217_190902_tagpass_20260222").
        raw_decklist_text: Decklist as text. Format: "1 Card Name" lines, optionally
            with "Commander" / "Deck" section headers.
        commander: Optional commander card name. Overrides any Commander section in text.
        include_debug: When True, includes diagnostic counts under response.debug.

    Returns:
        Dict with version, card_count, color_identity, mana_curve, subtype_density,
        primitive_density, deck_themes_v1, detected_combos_v1, bracket_estimate,
        bracket_envelope, gap_signal, warnings.
    """
    async with _client() as client:
        r = await client.post("/deck/analyze_v1", json={
            "db_snapshot_id": db_snapshot_id,
            "raw_decklist_text": raw_decklist_text,
            "commander": commander,
            "format": "commander",
            "include_debug": include_debug,
        })
        r.raise_for_status()
        return r.json()


# ============================================================
# Pillar A.2 — search_cards
# ============================================================


@mcp.tool()
async def search_cards(
    db_snapshot_id: str,
    primitives_any: Optional[List[str]] = None,
    primitives_all: Optional[List[str]] = None,
    primitives_none: Optional[List[str]] = None,
    subtypes_any: Optional[List[str]] = None,
    type_any: Optional[List[str]] = None,
    color_identity_subset_of: Optional[List[str]] = None,
    color_identity_includes_all: Optional[List[str]] = None,
    cmc_min: Optional[float] = None,
    cmc_max: Optional[float] = None,
    name_contains: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "name",
) -> Dict[str, Any]:
    """Rich-filter card lookup over the snapshot.

    Returns wide annotated pools — NEVER top-N. Sort options are structural
    (name, cmc, color_identity_size); never a quality score. AI ranks among
    the returned set by its own criteria (creativity envelope rule).

    Args:
        db_snapshot_id: Required snapshot ID.
        primitives_any: Match cards with ANY of these primitives (OR).
        primitives_all: Match cards with ALL of these primitives (AND).
        primitives_none: Exclude cards with any of these primitives.
        subtypes_any: Match cards whose type_line contains any of these subtypes.
        type_any: Match cards whose type_line contains any of these types.
        color_identity_subset_of: Restrict to cards whose color identity is a subset of these.
        color_identity_includes_all: Restrict to cards whose color identity includes all of these.
        cmc_min: Minimum converted mana cost.
        cmc_max: Maximum CMC.
        name_contains: Substring match on card name.
        limit: Max results (default 100, max 500).
        offset: Pagination offset.
        sort_by: "name" | "cmc" | "color_identity_size".

    Returns:
        Dict with matched_count, returned_count, offset, limit, sort_by, results.
        Each result has oracle_id, name, type_line, cmc, color_identity, primitives.
    """
    filters: Dict[str, Any] = {}
    if primitives_any: filters["primitives_any"] = primitives_any
    if primitives_all: filters["primitives_all"] = primitives_all
    if primitives_none: filters["primitives_none"] = primitives_none
    if subtypes_any: filters["subtypes_any"] = subtypes_any
    if type_any: filters["type_any"] = type_any
    if color_identity_subset_of: filters["color_identity_subset_of"] = color_identity_subset_of
    if color_identity_includes_all: filters["color_identity_includes_all"] = color_identity_includes_all
    if cmc_min is not None: filters["cmc_min"] = cmc_min
    if cmc_max is not None: filters["cmc_max"] = cmc_max
    if name_contains: filters["name_contains"] = name_contains

    async with _client() as client:
        r = await client.post("/card/search_v1", json={
            "db_snapshot_id": db_snapshot_id,
            "filters": filters,
            "limit": limit,
            "offset": offset,
            "sort_by": sort_by,
        })
        r.raise_for_status()
        return r.json()


# ============================================================
# Pillar A.3 — candidate_pool
# ============================================================


@mcp.tool()
async def candidate_pool(
    db_snapshot_id: str,
    raw_decklist_text: str,
    commander: Optional[str] = None,
    intent: Optional[str] = None,
) -> Dict[str, Any]:
    """Clustered wide candidate pool for a deck.

    Returns ≥100 candidates clustered by axis (ramp/draw/removal/protection/
    tutors + per-theme deepening). Each cluster has rationale + annotated
    candidates with synergy-with-existing counts. NEVER top-N — AI ranks
    within each cluster.

    Use this when the AI is improving an existing deck and needs a wide
    pool of cards to consider, organized by what each one would shore up.

    Args:
        db_snapshot_id: Required snapshot ID.
        raw_decklist_text: Existing decklist.
        commander: Commander name.
        intent: Optional natural-language hint (e.g., "more lifegain").

    Returns:
        Dict with pool_size, clusters[]. Each cluster has axis, current,
        target, gap, rationale, candidates[].
    """
    async with _client() as client:
        r = await client.post("/deck/candidate_pool_v1", json={
            "db_snapshot_id": db_snapshot_id,
            "raw_decklist_text": raw_decklist_text,
            "commander": commander,
            "intent": intent,
        })
        r.raise_for_status()
        return r.json()


# ============================================================
# Pillar A.4 — strength_check
# ============================================================


@mcp.tool()
async def strength_check(
    db_snapshot_id: str,
    raw_decklist_text: str,
    commander: Optional[str] = None,
    k_nearest: int = 5,
) -> Dict[str, Any]:
    """Strength oracle — corpus similarity (Measurement A).

    Cosine-similarity ranks the deck against the corpus of successful
    decks. Returns nearest neighbors + axis-deviation flags + archetype
    consensus + combined strength band + plain-language interpretation.

    Measurement B (playtest win rate) wires in after Phase 5b — currently
    returns null.

    Args:
        db_snapshot_id: Required snapshot ID.
        raw_decklist_text: Decklist to evaluate.
        commander: Commander name.
        k_nearest: How many corpus neighbors to return (default 5).

    Returns:
        Dict with measurement_a (corpus_similarity), measurement_b (null until
        Phase 5b), combined_strength_band (STRONG_AND_ALIGNED / STRONG_BUT_GAPS
        / PROXY_OF_ARCHETYPE / DIVERGENT / UNKNOWN), interpretation.
    """
    async with _client() as client:
        r = await client.post("/deck/strength_check_v1", json={
            "db_snapshot_id": db_snapshot_id,
            "raw_decklist_text": raw_decklist_text,
            "commander": commander,
            "k_nearest": k_nearest,
        })
        r.raise_for_status()
        return r.json()


# ============================================================
# Pillar A.5 — context_bundle + convenience
# ============================================================


@mcp.tool()
async def agent_context_bundle(
    db_snapshot_id: str,
    raw_decklist_text: str,
    commander: Optional[str] = None,
    intent: Optional[str] = None,
    include: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """One-round-trip composite for AI agent kickoff.

    Composes analyze + candidate_pool + strength_check + reference_decks
    into a single response. Use this as the FIRST call when starting a
    build or improve session — gets everything the AI needs in one trip.

    Args:
        db_snapshot_id: Required snapshot ID.
        raw_decklist_text: Decklist (can be empty for from-scratch builds).
        commander: Commander name.
        intent: Optional natural-language hint.
        include: Which sub-bundles to fetch. Default: all four.

    Returns:
        Dict with analyze, candidate_pool, strength_check, reference_decks
        keys (each may be null if not in `include`).
    """
    async with _client() as client:
        body: Dict[str, Any] = {
            "db_snapshot_id": db_snapshot_id,
            "raw_decklist_text": raw_decklist_text,
            "commander": commander,
            "intent": intent,
        }
        if include is not None:
            body["include"] = include
        r = await client.post("/agent/context_bundle_v1", json=body)
        r.raise_for_status()
        return r.json()


@mcp.tool()
async def commander_archetype_brief(
    db_snapshot_id: str,
    commander: str,
) -> Dict[str, Any]:
    """Common archetypes + theme distribution + staple cards for a commander.

    Derived from the strength-oracle corpus. Use this BEFORE starting a
    from-scratch build to understand the prior-art shape for a commander.

    Args:
        db_snapshot_id: Required snapshot ID.
        commander: Commander card name.

    Returns:
        Dict with corpus_deck_count, common_archetypes, bracket_distribution,
        staple_cards, color_identity.
    """
    async with _client() as client:
        r = await client.get("/commander/archetype_brief_v1", params={
            "commander": commander,
            "db_snapshot_id": db_snapshot_id,
        })
        r.raise_for_status()
        return r.json()


@mcp.tool()
async def theme_top_cards(
    db_snapshot_id: str,
    theme_id: str,
    color_identity: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """Highest-signal cards for a theme.

    Given a theme_id (e.g., "TYPAL_GOBLINS", "THEME_CONTROL"), return cards
    whose primitives most heavily match that theme's score formula.

    Args:
        db_snapshot_id: Required snapshot ID.
        theme_id: Theme identifier from the taxonomy. See
            Mtg deck building brain/13_AI_AGENT_SURFACE/ARCHETYPE_CATALOG.md.
        color_identity: Comma-separated colors to restrict to (e.g., "R,W").
        limit: Max results (default 50).

    Returns:
        Dict with theme_id, subtype, primitives_used_for_match, matched_count,
        results[]. Each result has oracle_id, name, type_line, cmc, primitives,
        theme_signal_count.
    """
    async with _client() as client:
        params: Dict[str, Any] = {
            "theme_id": theme_id,
            "db_snapshot_id": db_snapshot_id,
            "limit": limit,
        }
        if color_identity:
            params["color_identity"] = color_identity
        r = await client.get("/theme/top_cards_v1", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool()
async def corpus_similar_decks(
    db_snapshot_id: str,
    raw_decklist_text: str,
    commander: Optional[str] = None,
    k: int = 5,
    include_decklists: bool = False,
) -> Dict[str, Any]:
    """k corpus decks most similar to the given deck.

    Optionally embeds the full decklist of each. Use this to read prior-art
    decks directly when designing or evaluating a build.

    Args:
        db_snapshot_id: Required snapshot ID.
        raw_decklist_text: Reference deck.
        commander: Commander name.
        k: How many corpus decks to return.
        include_decklists: When True, embed full decklist of each. Larger payload.

    Returns:
        Dict with k_returned, decks[].
    """
    async with _client() as client:
        r = await client.post("/corpus/similar_decks_v1", json={
            "db_snapshot_id": db_snapshot_id,
            "raw_decklist_text": raw_decklist_text,
            "commander": commander,
            "k": k,
            "include_decklists": include_decklists,
        })
        r.raise_for_status()
        return r.json()


# ============================================================
# Engine health (read-only diagnostic)
# ============================================================


@mcp.tool()
async def corpus_batch_ingest(
    db_snapshot_id: str,
    entries: List[Dict[str, Any]],
    skip_bracket_verification: bool = False,
) -> Dict[str, Any]:
    """Phase 5a — batch-ingest external decks (EDHREC scrapes etc.) into the
    strength oracle corpus.

    Each entry must include: commander (str), decklist (list[str], 50+ cards),
    claimed_bracket (B1..B5). Optional: source_url, source_label, archetype_hint.

    Bracket cross-check: every entry is checked against engine analysis. If the
    claimed bracket disagrees with engine read by 2+ brackets, OR claimed B1/B2
    has detected 2-card combos, the entry is flagged `needs_review` and written
    to corpus_review_queue_v1.jsonl. Flagged entries still ingest (with the
    warning attached) so they're available to strength_check with caveat.

    Args:
        db_snapshot_id: Snapshot ID.
        entries: List of {commander, decklist, claimed_bracket, ...} dicts.
        skip_bracket_verification: When True, skip the cross-check (faster but
            no review-queue protection). Default False — verification on.

    Returns:
        {version, status, total_submitted, accepted, flagged_for_review,
         rejected, results[]}. Each result has its own status + corpus_id +
         verification_notes.
    """
    async with _client() as client:
        r = await client.post("/corpus/batch_ingest_v1", json={
            "db_snapshot_id": db_snapshot_id,
            "entries": entries,
            "skip_bracket_verification": skip_bracket_verification,
        })
        r.raise_for_status()
        return r.json()


@mcp.tool()
async def engine_health() -> Dict[str, Any]:
    """Engine health check — confirms connectivity and reports active snapshot.

    Returns engine_version, active db_snapshot_id, server_time. Use this if
    deck/card calls return errors; confirms the engine is running and which
    snapshot is loaded.
    """
    async with _client() as client:
        r = await client.get("/health")
        r.raise_for_status()
        return r.json()


if __name__ == "__main__":
    mcp.run()
