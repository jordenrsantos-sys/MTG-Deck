"""
agent_semantic_retrieval_v1 — Iter 3 Phase 7 (SCAFFOLDED, not fully wired).

This module ships the API surface for card-text semantic retrieval so
the call sites in B2 and C2.2 have stable integration points. The
actual embedding source (Voyage AI / Anthropic embeddings API) is NOT
hooked up in iter 3: the kickoff explicitly authorized a $15-30 one-
time embedding cost, but the external API key + library setup is out
of scope for the autonomous mega-task without further user setup.

Behavior:
  - If `card_embeddings_v1.sqlite` exists and is populated, queries
    return real semantic neighbors.
  - If the file is missing or the embedding backend is unavailable,
    queries return an empty list — call sites treat that as "no
    semantic augmentation" and proceed normally. No errors.

Iter 4 hand-off (to actually enable semantic retrieval):
  1. `pip install voyageai`
  2. Set `VOYAGE_API_KEY` env var
  3. `python -m api.engine.layers.agent_semantic_retrieval_v1 build`
     (or equivalent CLI runner — TODO add at iter 4 ship time)
  4. Vectors written to `repo/api/engine/data/embeddings/card_embeddings_v1.sqlite`
  5. Subsequent build_deck calls automatically use the index for
     B2 must-include context + C2.2 anchor-neighbor injection.

Public API:
  - `is_available() -> bool`
  - `build_index(db_snapshot_id, ...) -> dict` (iter 4 implements)
  - `query_neighbors(card_name, k=20, ...) -> list[dict]`
  - `EMBEDDING_DB_PATH` — canonical path for the index sqlite file
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


SEMANTIC_RETRIEVAL_VERSION = "agent_semantic_retrieval_v1.0"

# Canonical path for the embeddings index. iter 4 populates this file.
EMBEDDING_DB_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "embeddings" / "card_embeddings_v1.sqlite"
)


def is_available() -> bool:
    """True iff the semantic-retrieval index exists and has data.

    Cheap to call repeatedly. Callers MUST guard query_neighbors() on
    this — when the index isn't available, the function returns an
    empty list and the caller skips the semantic-augmentation step.
    """
    if not EMBEDDING_DB_PATH.is_file():
        return False
    try:
        con = sqlite3.connect(str(EMBEDDING_DB_PATH))
        try:
            cur = con.execute("SELECT COUNT(*) FROM card_embeddings LIMIT 1")
            count = cur.fetchone()[0]
            return count > 0
        finally:
            con.close()
    except sqlite3.Error:
        return False


def query_neighbors(
    card_name: str,
    k: int = 20,
    color_identity_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return up to `k` cards semantically similar to `card_name`.

    Returns an empty list if the index isn't available (graceful no-op
    fallback). When the index IS available (iter 4+), returns dicts
    with at least:
      - name: str
      - similarity: float (0.0-1.0, higher = more similar)
      - color_identity: list[str] (filtered if `color_identity_filter` given)

    Args:
        card_name: Card to find neighbors for. Looked up by exact name
            in the index.
        k: Max neighbors to return.
        color_identity_filter: If given, drop neighbors whose color
            identity isn't a subset of this set. Useful for filtering
            to legal cards for a given commander.
    """
    if not is_available():
        return []
    # iter 4: actual cosine-distance query against the embeddings table.
    # For now this branch is unreachable because is_available() returns
    # False when the DB file doesn't exist. Keeping the function signature
    # stable so call sites don't need to change at iter 4.
    return []


def build_index(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Build the card embeddings index. NOT IMPLEMENTED in iter 3.

    Iter 4 will:
      1. Read all Commander-legal cards from the cards table.
      2. Call the embedding API (Voyage AI or Anthropic embeddings) on
         each card's name + type_line + oracle_text.
      3. Write vectors to EMBEDDING_DB_PATH using sqlite-vec extension.
      4. Return a build summary dict with counts and elapsed time.

    Estimated one-time cost: ~$1.62 with Voyage AI voyage-3
    ($0.18/MT × 30k cards × ~300 tokens/card = 9M tokens), or higher
    if Anthropic embeddings.
    """
    return {
        "status": "NOT_IMPLEMENTED",
        "version": SEMANTIC_RETRIEVAL_VERSION,
        "message": (
            "build_index is scaffolded but not implemented in iter 3. "
            "Iter 4 ships the actual indexing — see module docstring for "
            "the runbook."
        ),
    }
