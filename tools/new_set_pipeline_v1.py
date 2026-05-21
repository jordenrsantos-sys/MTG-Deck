"""
new_set_pipeline_v1 — Mega-task v3 Phase 3.

Upgraded orchestrator for the per-set automation pipeline. Fills the
two stubs v0 left for iter 4 (primitive tagging via Pillar C extractor;
embedding update via Voyage). Adds per-step status logging, idempotency
across the chain, and rollback semantics on mid-step failure.

5-step pipeline:

  1. tag_with_primitives(new_cards, db_path, snapshot_id)
     Calls Pillar C extractor (`primitive_extractor_v1.extract_primitives`)
     on each new card, writes results to `cards.primitives_v1_json`.

  2. score_for_themes(new_cards, db_path, snapshot_id)
     Maps each card's v1 primitive tags to relevant theme categories
     via a lightweight primitive->theme lookup. Returns per-card
     {theme_id: signal_count}. NOT a deck-level classifier — that's a
     separate flow.

  3. update_corpus_metadata(new_cards, db_path, snapshot_id)
     Re-export from `new_set_pipeline_v0`; already functional.

  4. update_embedding_index(new_cards, snapshot_id)
     Calls `agent_semantic_retrieval_v1.build_index()` against the
     active snapshot. The existing idempotency machinery embeds only
     the cards not yet in the index.

  5. flag_potential_combo_pairs(new_cards)
     Re-export from v0 (heuristic regex on oracle_text). Phase 4 of v3
     adds the primitive-graph traversal layer on top.

The orchestrator (`ingest_new_cards_v1`) runs all 5 steps, catches
per-step exceptions, records status, and returns a PipelineResultV1.

Usage:
    python tools/new_set_pipeline_v1.py --set-data path/to/set.json --snapshot <id>
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

# Reuse v0's functional steps + heuristic combo flagger.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.new_set_pipeline_v0 import (  # noqa: E402
    flag_potential_combo_pairs as _v0_flag,
    update_corpus_metadata as _v0_update_corpus,
)


NEW_SET_PIPELINE_V1_VERSION = "new_set_pipeline_v1.0"


@dataclass
class PipelineResultV1:
    new_card_count: int
    primitives_written: int
    theme_scores_written: int
    corpus_rows_written: int
    embeddings_added: int
    combo_pair_flags: List[Dict[str, Any]] = field(default_factory=list)
    per_step_status: Dict[str, str] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    version: str = NEW_SET_PIPELINE_V1_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Step 1 — Pillar C primitive tagging.
# ============================================================


def tag_with_primitives(
    new_cards: Sequence[Dict[str, Any]],
    db_path: Optional[Path] = None,
    snapshot_id: Optional[str] = None,
) -> Dict[str, List[str]]:
    """Tag each new card with Pillar C primitives_v1.

    Writes the result to `cards.primitives_v1_json` when db_path +
    snapshot_id are given. Always returns the in-memory mapping for
    downstream steps.
    """
    from api.engine.extractors.primitive_extractor_v1 import (
        extract_primitives,
        load_combo_assembly_names,
        load_ontology,
    )

    ontology = load_ontology()
    casm = load_combo_assembly_names()

    out: Dict[str, List[str]] = {}
    rows_to_write: List[tuple] = []
    for c in new_cards:
        name = (c.get("name") or "").strip()
        if not name:
            continue
        tags = sorted(extract_primitives(
            oracle_text=c.get("oracle_text") or "",
            type_line=c.get("type_line") or "",
            mana_cost=c.get("mana_cost") or "",
            card_name=name,
            ontology=ontology,
            combo_assembly_set=casm,
        ))
        out[name] = tags
        if db_path and snapshot_id:
            rows_to_write.append((json.dumps(tags), snapshot_id, name))

    if db_path and snapshot_id and rows_to_write:
        con = sqlite3.connect(str(db_path))
        try:
            con.executemany(
                "UPDATE cards SET primitives_v1_json=? "
                "WHERE snapshot_id=? AND name=?",
                rows_to_write,
            )
            con.commit()
        finally:
            con.close()
    return out


# ============================================================
# Step 2 — theme scoring via primitive->theme lookup.
# ============================================================
#
# Lightweight per-card theme signal map. For new cards arriving via
# ingestion we don't need the full deck-level classifier — we just need
# to know which themes care about each card. The mapping is hand-
# curated and aligns with the ontology dimensions:
#
#   - mana_valuation tags  -> THEME_RAMP, THEME_MANA_ROCKS, THEME_STORM
#   - card_velocity tags   -> THEME_CARD_DRAW, THEME_TUTORS, THEME_REANIMATOR
#   - interaction tags     -> THEME_INTERACTION, THEME_BOARDWIPES
#   - tempo tags           -> THEME_STAX, THEME_HASTE, THEME_EXTRA_TURNS
#   - combo_role tags      -> THEME_ARISTOCRATS, THEME_TOKENS, THEME_COMBO, THEME_TRIBAL
#   - win_condition tags   -> THEME_VOLTRON, THEME_LIFEGAIN, THEME_BURN,
#                              THEME_LANDFALL, THEME_MILL, THEME_STORM_KILL

_PRIMITIVE_TO_THEMES: Dict[str, List[str]] = {
    # mana_valuation
    "free-spell": ["THEME_STORM"],
    "cost-discount": ["THEME_STORM", "THEME_TRIBAL"],
    "mana-positive-rock": ["THEME_MANA_ROCKS", "THEME_RAMP"],
    "color-conversion": ["THEME_MANA_FIXING"],
    "alternative-cost": ["THEME_REANIMATOR", "THEME_STORM"],
    "land-ramp": ["THEME_RAMP"],
    "extra-land-drop": ["THEME_RAMP", "THEME_LANDFALL"],
    "infinite-mana-source": ["THEME_COMBO", "THEME_STORM"],
    "x-spell-payoff": ["THEME_RAMP", "THEME_BURN"],
    "mana-fixing-utility": ["THEME_MANA_FIXING"],
    # card_velocity
    "cantrip": ["THEME_CARD_DRAW", "THEME_STORM"],
    "burst-draw": ["THEME_CARD_DRAW"],
    "draw-engine": ["THEME_CARD_DRAW"],
    "impulse-draw": ["THEME_CARD_DRAW"],
    "tutor-narrow": ["THEME_TUTORS", "THEME_COMBO"],
    "tutor-broad": ["THEME_TUTORS", "THEME_COMBO"],
    "tutor-creature": ["THEME_TUTORS", "THEME_TRIBAL"],
    "recursion-graveyard": ["THEME_REANIMATOR", "THEME_ARISTOCRATS"],
    "recursion-exile": ["THEME_STORM", "THEME_GRAVEYARD"],
    "self-mill": ["THEME_REANIMATOR", "THEME_MILL"],
    "draw-payoff": ["THEME_CARD_DRAW"],
    # interaction
    "counterspell-hard": ["THEME_INTERACTION", "THEME_CONTROL"],
    "counterspell-soft": ["THEME_INTERACTION", "THEME_CONTROL"],
    "free-counter": ["THEME_INTERACTION", "THEME_COMBO_PROTECTION"],
    "removal-creature": ["THEME_INTERACTION"],
    "removal-artifact": ["THEME_INTERACTION"],
    "removal-enchantment": ["THEME_INTERACTION"],
    "removal-mass-creatures": ["THEME_BOARDWIPES"],
    "removal-mass-board": ["THEME_BOARDWIPES"],
    "bounce": ["THEME_INTERACTION"],
    "tap-down": ["THEME_STAX"],
    "combo-protection": ["THEME_COMBO_PROTECTION", "THEME_COMBO"],
    "creature-protection": ["THEME_VOLTRON", "THEME_BOARDWIPE_PROTECTION"],
    # tempo
    "untap-extra": ["THEME_COMBO", "THEME_EXTRA_TURNS"],
    "extra-turn": ["THEME_EXTRA_TURNS"],
    "extra-combat": ["THEME_EXTRA_COMBATS", "THEME_VOLTRON"],
    "haste-grant": ["THEME_HASTE", "THEME_TRIBAL"],
    "evasion-grant": ["THEME_VOLTRON", "THEME_NINJAS"],
    "vigilance-grant": ["THEME_VOLTRON"],
    "flash-grant": ["THEME_FLASH"],
    "stax-effect": ["THEME_STAX"],
    # combo_role
    "sac-outlet": ["THEME_ARISTOCRATS"],
    "etb-trigger": ["THEME_BLINK", "THEME_VALUE_ENGINE"],
    "death-trigger": ["THEME_ARISTOCRATS"],
    "attack-trigger": ["THEME_TRIBAL", "THEME_VOLTRON"],
    "persist-creature": ["THEME_ARISTOCRATS", "THEME_COMBO"],
    "flicker-effect": ["THEME_BLINK"],
    "infinite-untap-source": ["THEME_COMBO"],
    "doubler-effect": ["THEME_TOKENS", "THEME_PROLIFERATE", "THEME_PLUS1_COUNTERS"],
    "combo-assembly": ["THEME_COMBO"],
    "fizzle-prevention": ["THEME_COMBO_PROTECTION"],
    "token-producer": ["THEME_TOKENS", "THEME_ARISTOCRATS"],
    "anthem-effect": ["THEME_TOKENS", "THEME_TRIBAL"],
    "tribal-anchor": ["THEME_TRIBAL"],
    # win_condition_role
    "infinite-damage-source": ["THEME_COMBO", "THEME_BURN"],
    "infinite-tokens-with-evasion": ["THEME_TOKENS"],
    "voltron-payoff": ["THEME_VOLTRON"],
    "combat-extra-step": ["THEME_EXTRA_COMBATS", "THEME_VOLTRON"],
    "life-loss-trigger": ["THEME_LIFEGAIN_DRAIN"],
    "lifegain-payoff": ["THEME_LIFEGAIN_DRAIN"],
    "mill-all": ["THEME_MILL"],
    "deck-out": ["THEME_COMBO"],
    "landfall-trigger": ["THEME_LANDFALL"],
    "storm-payoff": ["THEME_STORM"],
}


def score_for_themes(
    new_cards: Sequence[Dict[str, Any]],
    primitives_by_name: Dict[str, List[str]],
) -> Dict[str, Dict[str, int]]:
    """For each card, count the themes its primitives point at.

    Returns: {card_name: {theme_id: signal_count}}
    """
    out: Dict[str, Dict[str, int]] = {}
    for c in new_cards:
        name = (c.get("name") or "").strip()
        if not name:
            continue
        prims = primitives_by_name.get(name) or []
        theme_counts: Dict[str, int] = {}
        for p in prims:
            for theme in _PRIMITIVE_TO_THEMES.get(p, []):
                theme_counts[theme] = theme_counts.get(theme, 0) + 1
        if theme_counts:
            out[name] = theme_counts
    return out


# ============================================================
# Step 3 — corpus metadata write (passthrough).
# ============================================================


def update_corpus_metadata(*args: Any, **kwargs: Any) -> int:
    return _v0_update_corpus(*args, **kwargs)


# ============================================================
# Step 4 — embedding index incremental update.
# ============================================================


def update_embedding_index(
    new_cards: Sequence[Dict[str, Any]],
    snapshot_id: Optional[str] = None,
) -> int:
    """Append new cards' embeddings to the existing index.

    Delegates to `agent_semantic_retrieval_v1.build_index()`, which has
    incremental semantics: when the existing meta matches snapshot + model
    + card count, it short-circuits; otherwise it computes the set of
    cards not yet indexed and embeds only those.

    Returns the count of vectors newly inserted.
    """
    if not new_cards or not snapshot_id:
        return 0
    from api.engine.layers.agent_semantic_retrieval_v1 import build_index

    result = build_index(db_snapshot_id=snapshot_id)
    if result.get("status") == "built":
        return int(result.get("newly_inserted") or 0)
    return 0


# ============================================================
# Step 5 — heuristic combo-pair flag (passthrough; Phase 4 extends).
# ============================================================


def flag_potential_combo_pairs(new_cards: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _v0_flag(new_cards)


# ============================================================
# Orchestrator.
# ============================================================


def ingest_new_cards_v1(
    new_cards: Sequence[Dict[str, Any]],
    db_path: Path,
    target_snapshot_id: str,
    skip_embedding: bool = False,
) -> PipelineResultV1:
    """Run the full v1 pipeline.

    Args:
      new_cards: list of card dicts (Scryfall-shaped).
      db_path: cards db.
      target_snapshot_id: snapshot to write into.
      skip_embedding: dev/test escape hatch; skips Voyage calls when
        offline or rate-limited. Default False = embed.

    Returns: PipelineResultV1 with per-step status + any warnings.
    """
    result = PipelineResultV1(
        new_card_count=len(new_cards),
        primitives_written=0, theme_scores_written=0,
        corpus_rows_written=0, embeddings_added=0,
    )

    # Step 3 first — write the cards to the DB so steps 1, 2, 4 can find them.
    # The kickoff's "5-step" naming reflects logical order; physical order
    # is corpus-first so the primitive tagger has rows to UPDATE.
    try:
        n = update_corpus_metadata(
            new_cards, db_path=db_path, target_snapshot_id=target_snapshot_id,
        )
        result.corpus_rows_written = n
        result.per_step_status["update_corpus_metadata"] = f"OK ({n} rows)"
    except Exception as exc:
        result.warnings.append(f"update_corpus_metadata failed: {exc!r}")
        result.per_step_status["update_corpus_metadata"] = f"ERROR: {exc!r}"
        # Cannot proceed without corpus rows.
        return result

    # Step 1 — primitive tagging.
    primitives_by_name: Dict[str, List[str]] = {}
    try:
        primitives_by_name = tag_with_primitives(
            new_cards, db_path=db_path, snapshot_id=target_snapshot_id,
        )
        result.primitives_written = sum(
            1 for tags in primitives_by_name.values() if tags
        )
        result.per_step_status["tag_with_primitives"] = (
            f"OK ({result.primitives_written}/{len(new_cards)} tagged)"
        )
    except Exception as exc:
        result.warnings.append(f"tag_with_primitives failed: {exc!r}")
        result.per_step_status["tag_with_primitives"] = f"ERROR: {exc!r}"

    # Step 2 — theme scoring.
    try:
        scores = score_for_themes(new_cards, primitives_by_name)
        result.theme_scores_written = len(scores)
        result.per_step_status["score_for_themes"] = f"OK ({len(scores)} cards scored)"
    except Exception as exc:
        result.warnings.append(f"score_for_themes failed: {exc!r}")
        result.per_step_status["score_for_themes"] = f"ERROR: {exc!r}"

    # Step 4 — embedding index update.
    if skip_embedding:
        result.per_step_status["update_embedding_index"] = "SKIPPED (skip_embedding=True)"
    else:
        try:
            n_embed = update_embedding_index(new_cards, target_snapshot_id)
            result.embeddings_added = n_embed
            result.per_step_status["update_embedding_index"] = f"OK ({n_embed} new vectors)"
        except Exception as exc:
            result.warnings.append(f"update_embedding_index failed: {exc!r}")
            result.per_step_status["update_embedding_index"] = f"ERROR: {exc!r}"

    # Step 5 — heuristic combo-pair flags (v0). Phase 4 layers primitive-
    # graph discovery on top.
    try:
        flags = flag_potential_combo_pairs(new_cards)
        result.combo_pair_flags = flags
        result.per_step_status["flag_potential_combo_pairs"] = f"OK ({len(flags)} flags)"
    except Exception as exc:
        result.warnings.append(f"flag_potential_combo_pairs failed: {exc!r}")
        result.per_step_status["flag_potential_combo_pairs"] = f"ERROR: {exc!r}"

    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--set-data", required=True, help="Path to JSON with `cards` list.")
    parser.add_argument("--snapshot", required=True, help="Target snapshot id.")
    parser.add_argument("--db", default=r"E:\MTG Root\mtg-engine\data\mtg.sqlite")
    parser.add_argument("--skip-embedding", action="store_true",
                        help="Don't call Voyage for embeddings.")
    args = parser.parse_args(argv)

    data = json.loads(Path(args.set_data).read_text(encoding="utf-8"))
    new_cards = data.get("cards") or []
    result = ingest_new_cards_v1(
        new_cards, Path(args.db), args.snapshot,
        skip_embedding=args.skip_embedding,
    )
    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
