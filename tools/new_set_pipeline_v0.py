"""
new_set_pipeline_v0 — Track 5 scaffolding (Iter 3 Phase 13).

Pipeline for ingesting a new set's worth of cards into the corpus +
primitive tags + theme scoring + embedding index + combo-pair scan.

Iter 3 ships the orchestrator + stubs for each step. Iter 4 ships the
actual Pillar C primitive extractor (which is the load-bearing piece
the rest of the pipeline depends on).

Pipeline steps:

  1. tag_with_primitives(new_cards) → tag dict
     STUB: returns empty list per card. Iter 4 wires in the Pillar C
     extractor from `repo/api/engine/data/primitives/ontology_v0.md`.

  2. score_for_themes(new_cards) → theme score map
     STUB: defers to the existing theme classifier
     (compute_deck_theme_classifier_v1). Already exists from Phase 2.1a.

  3. update_corpus_metadata(new_cards, db_path) → row count
     Writes rows to the cards table (snapshot_id, oracle_id, name,
     mana_cost, cmc, type_line, oracle_text, color_identity, ...).
     Idempotent via ON CONFLICT.

  4. update_embedding_index(new_cards) → vectors added
     STUB: calls agent_semantic_retrieval_v1.build_index() — itself a
     stub in iter 3. Iter 4 wires Voyage AI here.

  5. flag_potential_combo_pairs(new_cards) → flag list
     Heuristic: any new card with combo-relevant primitives
     (sac-outlet, etb-trigger, infinite-mana-related text) gets
     flagged for combo-pair scan against existing cards.

Usage:
    python tools/new_set_pipeline_v0.py --set-data path/to/new_set.json

For iter 3 testing, the input is a small Scryfall-shaped JSON with
3-5 cards. See repo/tests/test_new_set_pipeline_v0.py for fixtures.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


NEW_SET_PIPELINE_VERSION = "new_set_pipeline_v0.0"


@dataclass
class PipelineResult:
    new_card_count: int
    tagged_count: int
    theme_scored_count: int
    corpus_rows_written: int
    embeddings_added: int
    combo_pair_flags: List[Dict[str, Any]] = field(default_factory=list)
    per_step_status: Dict[str, str] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    version: str = NEW_SET_PIPELINE_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Step 1 — primitive tagging.
# ============================================================


def tag_with_primitives(new_cards: Sequence[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Map new card oracle_text → primitive tag list.

    STUB for iter 3 — returns empty list per card. Iter 4 wires in the
    Pillar C extractor (regex patterns from ontology_v0.md).

    Args:
        new_cards: list of card dicts with at least `name` and `oracle_text`.

    Returns:
        {card_name: [tag_id, ...]} — empty lists in iter 3.
    """
    result: Dict[str, List[str]] = {}
    for card in new_cards:
        name = card.get("name") or "?"
        result[name] = []  # iter 4: actual extraction
    return result


# ============================================================
# Step 2 — theme scoring.
# ============================================================


def score_for_themes(new_cards: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """Score each new card against the theme classifier.

    Defers to compute_card_theme_score_v1 if available; falls back to
    empty score dict in iter 3 (the classifier exists from Phase 2.1a
    but may need a specific snapshot to be initialized).

    Returns:
        {card_name: {theme_id: score, ...}}
    """
    result: Dict[str, Dict[str, float]] = {}
    for card in new_cards:
        name = card.get("name") or "?"
        # iter 3: skip the real classifier call — it requires a primed
        # snapshot. Stub with empty dict; iter 4 wires it.
        result[name] = {}
    return result


# ============================================================
# Step 3 — corpus metadata write.
# ============================================================


def update_corpus_metadata(
    new_cards: Sequence[Dict[str, Any]],
    db_path: Optional[Path] = None,
    target_snapshot_id: Optional[str] = None,
) -> int:
    """Write new card rows to the cards table.

    Args:
        new_cards: Scryfall-shaped card dicts.
        db_path: path to mtg.sqlite. Default: production DB.
        target_snapshot_id: snapshot to write into. If None, dry-run.

    Returns:
        Count of rows written.
    """
    if not new_cards or not target_snapshot_id:
        return 0
    if db_path is None:
        db_path = Path(r"E:\MTG Root\mtg-engine\data\mtg.sqlite")
    if not db_path.is_file():
        return 0
    con = sqlite3.connect(str(db_path))
    try:
        rows_to_insert = []
        for card in new_cards:
            oracle_id = card.get("oracle_id")
            name = card.get("name")
            if not (oracle_id and name):
                continue
            rows_to_insert.append((
                target_snapshot_id,
                oracle_id,
                name,
                card.get("mana_cost") or "",
                float(card.get("cmc") or 0),
                card.get("type_line") or "",
                card.get("oracle_text") or "",
                _serialize_list(card.get("colors")),
                _serialize_list(card.get("color_identity")),
                _serialize_list(card.get("produced_mana")),
                _serialize_list(card.get("keywords")),
                json.dumps(card.get("legalities") or {}),
                "[]",  # primitives_json — iter 4 populates
                json.dumps(card.get("image_uris") or {}),
                json.dumps(card.get("card_faces") or []),
                card.get("image_status") or "",
                card.get("released_at") or "",
            ))
        if rows_to_insert:
            con.executemany(
                """
                INSERT OR REPLACE INTO cards (
                    snapshot_id, oracle_id, name, mana_cost, cmc, type_line,
                    oracle_text, colors, color_identity, produced_mana,
                    keywords, legalities_json, primitives_json,
                    image_uris_json, card_faces_json, image_status,
                    released_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )
            con.commit()
        return len(rows_to_insert)
    finally:
        con.close()


def _serialize_list(value: Any) -> str:
    """Serialize a list/iterable value to a JSON-like string. Returns
    empty string for None or empty containers."""
    if not value:
        return ""
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return str(value)


# ============================================================
# Step 4 — embedding index update.
# ============================================================


def update_embedding_index(new_cards: Sequence[Dict[str, Any]]) -> int:
    """Add new cards to the semantic-retrieval embedding index.

    STUB in iter 3 — the embedding index isn't populated until iter 4
    (Voyage AI is the planned backend). Returns 0.

    Iter 4: this function will:
      1. Call Voyage AI's embed endpoint on each new card's
         name + type_line + oracle_text.
      2. Write vectors to card_embeddings_v1.sqlite via sqlite-vec.
      3. Return count of vectors added.
    """
    if not new_cards:
        return 0
    # iter 4 implementation hook.
    return 0


# ============================================================
# Step 5 — combo-pair flagging.
# ============================================================


# Heuristics on oracle_text to identify combo-relevant new cards.
_COMBO_RELEVANT_PATTERNS = [
    "sacrifice",
    "untap target",
    "when ~ enters",
    "deals damage equal to",
    "lifelink",
    "exile and return",
    "add {",  # mana production
]


def flag_potential_combo_pairs(new_cards: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Identify new cards that likely participate in combo lines.

    Heuristic-only — looks for telltale phrases in oracle_text. Iter 4
    will use Pillar C primitive tags for a more rigorous signal.

    Returns:
        List of {card_name, reason, candidate_partners} dicts. In iter 3,
        `candidate_partners` is empty (no semantic neighbor lookup yet).
    """
    flags: List[Dict[str, Any]] = []
    for card in new_cards:
        oracle_text = (card.get("oracle_text") or "").lower()
        if not oracle_text:
            continue
        matched_patterns = [p for p in _COMBO_RELEVANT_PATTERNS if p in oracle_text]
        if matched_patterns:
            flags.append({
                "card_name": card.get("name") or "?",
                "reason": f"oracle_text matches combo-relevant patterns: {matched_patterns}",
                "candidate_partners": [],  # iter 4: semantic-neighbor scan
            })
    return flags


# ============================================================
# Orchestrator.
# ============================================================


def ingest_new_cards(
    new_cards_json: Dict[str, Any],
    db_path: Optional[Path] = None,
    target_snapshot_id: Optional[str] = None,
) -> PipelineResult:
    """Run the full pipeline. `new_cards_json` is a dict with a
    `cards` list (Scryfall-shaped). Each step's outcome is recorded
    in the result.
    """
    cards = new_cards_json.get("cards") if isinstance(new_cards_json, dict) else None
    if not isinstance(cards, list):
        return PipelineResult(
            new_card_count=0, tagged_count=0, theme_scored_count=0,
            corpus_rows_written=0, embeddings_added=0,
            warnings=["Input missing 'cards' list."],
        )

    result = PipelineResult(
        new_card_count=len(cards), tagged_count=0, theme_scored_count=0,
        corpus_rows_written=0, embeddings_added=0,
    )

    # Step 1.
    try:
        tags = tag_with_primitives(cards)
        result.tagged_count = len(tags)
        result.per_step_status["tag_with_primitives"] = "STUB (iter 4 ships Pillar C extractor)"
    except Exception as exc:
        result.warnings.append(f"tag_with_primitives failed: {exc!r}")
        result.per_step_status["tag_with_primitives"] = f"ERROR: {exc!r}"

    # Step 2.
    try:
        scores = score_for_themes(cards)
        result.theme_scored_count = len(scores)
        result.per_step_status["score_for_themes"] = "STUB (classifier exists but needs primed snapshot)"
    except Exception as exc:
        result.warnings.append(f"score_for_themes failed: {exc!r}")
        result.per_step_status["score_for_themes"] = f"ERROR: {exc!r}"

    # Step 3.
    try:
        rows = update_corpus_metadata(cards, db_path=db_path,
                                      target_snapshot_id=target_snapshot_id)
        result.corpus_rows_written = rows
        if not target_snapshot_id:
            result.per_step_status["update_corpus_metadata"] = "DRY-RUN (no target_snapshot_id given)"
        else:
            result.per_step_status["update_corpus_metadata"] = f"OK ({rows} rows)"
    except Exception as exc:
        result.warnings.append(f"update_corpus_metadata failed: {exc!r}")
        result.per_step_status["update_corpus_metadata"] = f"ERROR: {exc!r}"

    # Step 4.
    try:
        embeds = update_embedding_index(cards)
        result.embeddings_added = embeds
        result.per_step_status["update_embedding_index"] = "STUB (iter 4 wires Voyage AI)"
    except Exception as exc:
        result.warnings.append(f"update_embedding_index failed: {exc!r}")
        result.per_step_status["update_embedding_index"] = f"ERROR: {exc!r}"

    # Step 5.
    try:
        flags = flag_potential_combo_pairs(cards)
        result.combo_pair_flags = flags
        result.per_step_status["flag_potential_combo_pairs"] = f"OK ({len(flags)} flags)"
    except Exception as exc:
        result.warnings.append(f"flag_potential_combo_pairs failed: {exc!r}")
        result.per_step_status["flag_potential_combo_pairs"] = f"ERROR: {exc!r}"

    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--set-data", required=True,
                        help="Path to Scryfall-shaped set JSON.")
    parser.add_argument("--snapshot", default=None,
                        help="Target snapshot to write into. If omitted, dry-run.")
    parser.add_argument("--db", default=None, help="Path to mtg.sqlite.")
    args = parser.parse_args()

    p = Path(args.set_data)
    if not p.is_file():
        print(f"ERROR: --set-data file not found: {p}", file=sys.stderr)
        return 2
    data = json.loads(p.read_text(encoding="utf-8"))
    db_path = Path(args.db) if args.db else None

    result = ingest_new_cards(
        data,
        db_path=db_path,
        target_snapshot_id=args.snapshot,
    )
    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
