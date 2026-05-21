"""
agent_semantic_retrieval_v1 — Iter 4 Phase 1 (ACTIVATED).

Card-text semantic retrieval via Voyage AI embeddings. Iter 3 shipped
the API surface with no-op fallbacks; iter 4 (mega-task v2 Phase 1)
plugs in the real Voyage backend and the one-time index build.

Public API:
  - `is_available() -> bool`
  - `build_index(db_snapshot_id, model_name="voyage-3", batch_size=128) -> dict`
  - `query_neighbors(card_name, k=20, color_identity_filter=None) -> list[dict]`
  - `EMBEDDING_DB_PATH` — canonical path for the index sqlite file.

Storage: vectors are stored as float32 BLOBs in a vanilla sqlite table
(`card_embeddings`). At query time the whole vector matrix is loaded
into a numpy array and cosine similarity is computed in-memory (brute
force over ~30k vectors × 1024 dims = ~120 MB, ~50ms per top-k query).
This avoids the sqlite-vec extension dependency.

Idempotency: `build_index` skips rebuild if `embeddings_meta` records
the same active snapshot AND row count matches the Commander-legal
card count in the cards table.

Iter 4 hand-off (operator runbook):
  1. `pip install voyageai`
  2. `setx VOYAGE_API_KEY "<key>"` and reopen the shell
  3. `python -c "from api.engine.layers.agent_semantic_retrieval_v1 import build_index; print(build_index())"`
     (uses the active snapshot via engine.db)
  4. Vectors written to `repo/api/engine/data/embeddings/card_embeddings_v1.sqlite`
"""
from __future__ import annotations

import json
import os
import sqlite3
import struct
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SEMANTIC_RETRIEVAL_VERSION = "agent_semantic_retrieval_v1.1"

EMBEDDING_DB_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "embeddings" / "card_embeddings_v1.sqlite"
)

DEFAULT_MODEL = "voyage-3"
DEFAULT_BATCH = 128

_RETRY_BACKOFFS_S = (1.0, 2.0, 4.0)

# In-process cache for query path. Populated lazily on first query.
_CACHE: Dict[str, Any] = {
    "loaded_path": None,
    "names": None,            # list[str] aligned with matrix rows
    "name_to_row": None,      # dict[name.lower() -> row idx]
    "matrix": None,           # numpy.ndarray of shape (N, D), float32, L2-normalized
    "color_ids": None,        # list[list[str]]
    "type_lines": None,       # list[str]
    "oracle_texts": None,     # list[str]
    "cmcs": None,             # list[float | None]
    "released_ats": None,     # list[str]
}


# ============================================================
# Schema / connection helpers
# ============================================================


_SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS card_embeddings (
        name TEXT PRIMARY KEY,
        color_identity TEXT NOT NULL DEFAULT '',
        type_line TEXT NOT NULL DEFAULT '',
        oracle_text TEXT NOT NULL DEFAULT '',
        cmc REAL,
        released_at TEXT NOT NULL DEFAULT '',
        vec BLOB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS embeddings_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_card_embeddings_color
        ON card_embeddings(color_identity)
    """,
]


def _ensure_schema(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    for stmt in _SCHEMA_SQL:
        con.execute(stmt)
    con.commit()
    return con


def _meta_get(con: sqlite3.Connection, key: str) -> Optional[str]:
    cur = con.execute("SELECT value FROM embeddings_meta WHERE key=?", (key,))
    r = cur.fetchone()
    return r[0] if r else None


def _meta_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO embeddings_meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


# ============================================================
# Vector encoding
# ============================================================


def _pack_vec(vec: Iterable[float]) -> bytes:
    arr = list(vec)
    return struct.pack(f"<{len(arr)}f", *arr)


def _unpack_vec(blob: bytes) -> Tuple[float, ...]:
    n = len(blob) // 4
    return struct.unpack(f"<{n}f", blob)


# ============================================================
# Public API
# ============================================================


def is_available() -> bool:
    """True iff the semantic-retrieval index exists, has data, AND the
    VOYAGE_API_KEY env var is set (needed to embed novel query terms,
    though card-by-name lookups don't strictly require it).

    The env-var check is intentionally lenient — once an index is
    built, querying by an indexed card name works even without the
    env var; only ad-hoc text queries would need it. For the C2.2
    integration we always query by indexed card names, so the env var
    check is informational. The README test of "VOYAGE_API_KEY AND DB
    populated" lives at the agent integration point.
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


def _load_cache_if_needed() -> bool:
    """Lazy-load the entire embeddings table into a numpy matrix.

    Returns True on success, False if the index isn't populated.
    """
    if _CACHE.get("loaded_path") == str(EMBEDDING_DB_PATH):
        return _CACHE.get("matrix") is not None
    if not is_available():
        return False
    try:
        import numpy as np
    except ImportError:
        return False
    con = sqlite3.connect(str(EMBEDDING_DB_PATH))
    try:
        rows = list(
            con.execute(
                "SELECT name, color_identity, type_line, oracle_text, cmc, released_at, vec "
                "FROM card_embeddings"
            )
        )
    finally:
        con.close()
    if not rows:
        return False
    names: List[str] = []
    color_ids: List[List[str]] = []
    type_lines: List[str] = []
    oracle_texts: List[str] = []
    cmcs: List[Optional[float]] = []
    released_ats: List[str] = []
    vectors: List[Tuple[float, ...]] = []
    for name, ci, tl, ot, cmc, ra, blob in rows:
        names.append(name)
        color_ids.append([c for c in (ci or "").split(",") if c])
        type_lines.append(tl or "")
        oracle_texts.append(ot or "")
        cmcs.append(float(cmc) if cmc is not None else None)
        released_ats.append(ra or "")
        vectors.append(_unpack_vec(blob))
    matrix = np.array(vectors, dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    matrix /= norms
    _CACHE["loaded_path"] = str(EMBEDDING_DB_PATH)
    _CACHE["names"] = names
    _CACHE["name_to_row"] = {n.strip().lower(): i for i, n in enumerate(names)}
    _CACHE["matrix"] = matrix
    _CACHE["color_ids"] = color_ids
    _CACHE["type_lines"] = type_lines
    _CACHE["oracle_texts"] = oracle_texts
    _CACHE["cmcs"] = cmcs
    _CACHE["released_ats"] = released_ats
    return True


def query_neighbors(
    card_name: str,
    k: int = 20,
    color_identity_filter: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return up to `k` cards semantically similar to `card_name`.

    Returns an empty list if:
      - the index isn't populated,
      - `card_name` isn't an indexed card (we don't ad-hoc embed novel
        text in the query hot path — keeps query latency under 100ms),
      - numpy isn't importable.

    When `color_identity_filter` is given (e.g. ["W","B","R"]), drops
    neighbors whose color identity isn't a subset of that set.
    """
    if not _load_cache_if_needed():
        return []
    import numpy as np
    name_key = (card_name or "").strip().lower()
    row = _CACHE["name_to_row"].get(name_key)
    if row is None:
        return []
    q = _CACHE["matrix"][row]
    # Cosine = dot product of L2-normalized vectors.
    sims = _CACHE["matrix"] @ q
    # Sort descending; skip the self-match.
    if k <= 0:
        return []
    top = np.argpartition(-sims, min(k + 1, len(sims) - 1))[: k + 1]
    top = top[np.argsort(-sims[top])]
    filter_set = (
        {c.upper() for c in color_identity_filter} if color_identity_filter else None
    )
    out: List[Dict[str, Any]] = []
    for idx in top:
        if int(idx) == row:
            continue
        ci = _CACHE["color_ids"][int(idx)]
        if filter_set is not None and not set(c.upper() for c in ci).issubset(filter_set):
            continue
        out.append({
            "name": _CACHE["names"][int(idx)],
            "similarity": float(sims[int(idx)]),
            "color_identity": ci,
            "type_line": _CACHE["type_lines"][int(idx)],
            "oracle_text": _CACHE["oracle_texts"][int(idx)],
            "cmc": _CACHE["cmcs"][int(idx)],
            "released_at": _CACHE["released_ats"][int(idx)],
            "primitives": [],
        })
        if len(out) >= k:
            break
    return out


def _commander_legal_cards(snapshot_id: str) -> List[Dict[str, Any]]:
    """Load Commander-legal cards from the active snapshot."""
    from engine.db import resolve_db_path

    db_path = resolve_db_path()
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT name, type_line, oracle_text, cmc, color_identity, "
            "released_at, legalities_json "
            "FROM cards WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchall()
    finally:
        con.close()
    out: List[Dict[str, Any]] = []
    seen_names: set = set()
    for r in rows:
        leg_json = r["legalities_json"] or "{}"
        try:
            leg = json.loads(leg_json)
        except json.JSONDecodeError:
            continue
        if leg.get("commander") != "legal":
            continue
        name = (r["name"] or "").strip()
        if not name or name.lower() in seen_names:
            continue
        seen_names.add(name.lower())
        ci_raw = r["color_identity"] or ""
        ci_list: List[str]
        try:
            parsed = json.loads(ci_raw) if ci_raw else []
            ci_list = [c.upper() for c in parsed if isinstance(c, str)]
        except (json.JSONDecodeError, TypeError):
            ci_list = [c for c in ci_raw.split(",") if c]
        out.append({
            "name": name,
            "type_line": r["type_line"] or "",
            "oracle_text": r["oracle_text"] or "",
            "cmc": r["cmc"],
            "color_identity": ci_list,
            "released_at": r["released_at"] or "",
        })
    return out


def _compose_embedding_text(card: Dict[str, Any]) -> str:
    parts = [card["name"]]
    if card.get("type_line"):
        parts.append(card["type_line"])
    if card.get("oracle_text"):
        parts.append(card["oracle_text"])
    return "\n".join(parts)


def _embed_batch_with_backoff(client: Any, texts: List[str], model: str) -> List[List[float]]:
    """Embed a batch with exponential backoff on transient errors."""
    last_exc: Optional[Exception] = None
    for backoff in (0.0,) + _RETRY_BACKOFFS_S:
        if backoff > 0:
            time.sleep(backoff)
        try:
            resp = client.embed(texts, model=model, input_type="document")
            return list(resp.embeddings)
        except Exception as exc:
            last_exc = exc
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("embed_batch unreachable")


def build_index(
    db_snapshot_id: Optional[str] = None,
    model_name: str = DEFAULT_MODEL,
    batch_size: int = DEFAULT_BATCH,
    force_rebuild: bool = False,
) -> Dict[str, Any]:
    """Build the card embeddings index over Commander-legal cards.

    Idempotent: if the existing index records the same snapshot and the
    row count matches the Commander-legal cards in the current cards
    table, skip work and return a 'skipped' status.

    Returns:
      dict with status ('built' | 'skipped' | 'failed'), version,
      card_count, model, snapshot_id, elapsed_s, message.
    """
    t0 = time.perf_counter()
    if not os.environ.get("VOYAGE_API_KEY"):
        return {
            "status": "failed",
            "version": SEMANTIC_RETRIEVAL_VERSION,
            "message": (
                "VOYAGE_API_KEY env var not set. Run "
                "`setx VOYAGE_API_KEY \"<key>\"` and reopen the shell."
            ),
        }

    if db_snapshot_id is None:
        try:
            from engine.db import resolve_db_path
            con = sqlite3.connect(str(resolve_db_path()))
            try:
                row = con.execute(
                    "SELECT snapshot_id FROM snapshots "
                    "ORDER BY snapshot_id DESC LIMIT 1"
                ).fetchone()
                if row and row[0]:
                    db_snapshot_id = row[0]
            finally:
                con.close()
        except Exception as exc:
            return {
                "status": "failed",
                "version": SEMANTIC_RETRIEVAL_VERSION,
                "message": f"Could not resolve active snapshot: {exc}",
            }

    cards = _commander_legal_cards(db_snapshot_id)
    target_count = len(cards)

    con = _ensure_schema(EMBEDDING_DB_PATH)
    try:
        if not force_rebuild:
            existing_snapshot = _meta_get(con, "snapshot_id")
            existing_model = _meta_get(con, "model")
            existing_count_str = _meta_get(con, "card_count")
            existing_count = int(existing_count_str) if existing_count_str else 0
            current_rows = con.execute(
                "SELECT COUNT(*) FROM card_embeddings"
            ).fetchone()[0]
            if (
                existing_snapshot == db_snapshot_id
                and existing_model == model_name
                and existing_count == target_count
                and current_rows == target_count
            ):
                return {
                    "status": "skipped",
                    "version": SEMANTIC_RETRIEVAL_VERSION,
                    "card_count": current_rows,
                    "model": model_name,
                    "snapshot_id": db_snapshot_id,
                    "elapsed_s": round(time.perf_counter() - t0, 3),
                    "message": (
                        f"Index already current ({current_rows} rows, "
                        f"snapshot {db_snapshot_id}, model {model_name})."
                    ),
                }

        if force_rebuild:
            con.execute("DELETE FROM card_embeddings")
            con.commit()

        try:
            import voyageai
        except ImportError:
            return {
                "status": "failed",
                "version": SEMANTIC_RETRIEVAL_VERSION,
                "message": (
                    "voyageai package not installed. Run "
                    "`pip install voyageai`."
                ),
            }
        client = voyageai.Client()

        already_indexed = {
            r[0].strip().lower()
            for r in con.execute("SELECT name FROM card_embeddings").fetchall()
        }
        pending = [c for c in cards if c["name"].strip().lower() not in already_indexed]
        n_inserted = 0
        for i in range(0, len(pending), batch_size):
            batch = pending[i : i + batch_size]
            texts = [_compose_embedding_text(c) for c in batch]
            embeddings = _embed_batch_with_backoff(client, texts, model_name)
            with con:
                for card, emb in zip(batch, embeddings):
                    con.execute(
                        "INSERT OR REPLACE INTO card_embeddings "
                        "(name, color_identity, type_line, oracle_text, cmc, released_at, vec) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (
                            card["name"],
                            ",".join(card["color_identity"]),
                            card["type_line"],
                            card["oracle_text"],
                            card["cmc"],
                            card["released_at"],
                            _pack_vec(emb),
                        ),
                    )
                    n_inserted += 1

        final_count = con.execute(
            "SELECT COUNT(*) FROM card_embeddings"
        ).fetchone()[0]
        _meta_set(con, "snapshot_id", db_snapshot_id)
        _meta_set(con, "model", model_name)
        _meta_set(con, "card_count", str(final_count))
        _meta_set(con, "version", SEMANTIC_RETRIEVAL_VERSION)
        _meta_set(con, "built_at", str(int(time.time())))
        con.commit()
        # Invalidate cache so the new vectors are picked up next query.
        _CACHE["loaded_path"] = None
        return {
            "status": "built",
            "version": SEMANTIC_RETRIEVAL_VERSION,
            "card_count": final_count,
            "newly_inserted": n_inserted,
            "model": model_name,
            "snapshot_id": db_snapshot_id,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "message": (
                f"Built {final_count} embeddings for snapshot {db_snapshot_id} "
                f"using {model_name} ({n_inserted} newly inserted)."
            ),
        }
    finally:
        con.close()
