"""
voyage_rules_embedding_v1 — Mega-task v4 Phase 4.

Embed MTG Comprehensive Rules text + Scryfall per-card rulings into
the Voyage index so the agent can semantically retrieve rules
sections relevant to a card/combo at build time.

Architecturally additive to `agent_semantic_retrieval_v1`:
  - Card embeddings (existing): source_type="card", name PK.
  - Rule embeddings (new): source_type="rule", rule_id (e.g. "601.2a") PK.
  - Ruling embeddings (new): source_type="ruling", composite PK (card_name + ordinal).

Public API:
  - `embed_comprehensive_rules(rules_text, batch_size=128) -> dict`
    splits the text into per-section chunks, embeds, writes to
    `card_embeddings_v1.sqlite` with source_type="rule".
  - `embed_scryfall_rulings(rulings_json, batch_size=128) -> dict`
    embeds each ruling text, writes with source_type="ruling".
  - `query_rules(query_text, k=5, source_type=None) -> list[dict]`
    queries the index for rule/ruling sections matching a query.
    `source_type` filters by record type if given.

Cost estimate (one-time):
  - Comprehensive rules: ~75k lines × ~20 tokens × $0.18/MT ≈ $0.27
  - Scryfall rulings: ~150k rulings × ~30 tokens × $0.18/MT ≈ $0.81
  - Total: ~$1.10
"""
from __future__ import annotations

import json
import re
import sqlite3
import struct
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


VOYAGE_RULES_EMBEDDING_VERSION = "voyage_rules_embedding_v1.0"

# Re-use the same sqlite that card embeddings live in. Avoid creating
# a second index file — single source of truth for all Voyage vectors.
_RETRY_BACKOFFS_S = (1.0, 2.0, 4.0)


# ============================================================
# Schema migration — add source_type column if not present.
# ============================================================


def ensure_schema(db_path: Path) -> None:
    """Add the `source_type` column to `card_embeddings` (default `card`
    for existing rows) + the `rule_id` and `ruling_card` columns for
    rules/rulings rows. Idempotent."""
    con = sqlite3.connect(str(db_path))
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(card_embeddings)")}
        if "source_type" not in cols:
            con.execute(
                "ALTER TABLE card_embeddings ADD COLUMN source_type TEXT "
                "NOT NULL DEFAULT 'card'"
            )
        if "rule_id" not in cols:
            con.execute("ALTER TABLE card_embeddings ADD COLUMN rule_id TEXT")
        if "ruling_card" not in cols:
            con.execute("ALTER TABLE card_embeddings ADD COLUMN ruling_card TEXT")
        if "raw_text" not in cols:
            con.execute("ALTER TABLE card_embeddings ADD COLUMN raw_text TEXT")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_card_embeddings_source_type "
            "ON card_embeddings(source_type)"
        )
        con.commit()
    finally:
        con.close()


# ============================================================
# Comprehensive-rules splitting.
# ============================================================


_RULE_HEADER_RE = re.compile(
    r"^(\d{3}(?:\.\d{1,3}[a-z]?)?)\s*\.?\s*(.+?)$",
    re.MULTILINE,
)


def split_rules_into_sections(rules_text: str) -> List[Dict[str, str]]:
    """Split the WotC Comprehensive Rules text into per-section chunks.

    Heuristic: each rule starts with a "###.# Title" or "###.#a" pattern.
    We chunk by these boundaries; each chunk's `rule_id` is the leading
    section number and `text` is the body up to the next boundary.
    """
    sections: List[Dict[str, str]] = []
    headers = list(_RULE_HEADER_RE.finditer(rules_text))
    for i, h in enumerate(headers):
        rule_id = h.group(1).strip()
        title = h.group(2).strip()
        body_start = h.end()
        body_end = headers[i + 1].start() if i + 1 < len(headers) else len(rules_text)
        body = rules_text[body_start:body_end].strip()
        # Include the section even when body is empty — the title alone is
        # meaningful content for short rules like "601.2a When a player...".
        text = f"{rule_id} {title}"
        if body:
            text += f"\n{body}"
        if title:
            sections.append({
                "rule_id": rule_id, "title": title, "text": text,
            })
    return sections


# ============================================================
# Embedding helpers.
# ============================================================


def _pack_vec(vec) -> bytes:
    arr = list(vec)
    return struct.pack(f"<{len(arr)}f", *arr)


def _embed_batch(client: Any, texts: List[str], model: str) -> List[List[float]]:
    """Embed with exponential backoff on transient errors."""
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


# ============================================================
# Comprehensive rules embedding.
# ============================================================


def embed_comprehensive_rules(
    rules_text: str,
    db_path: Path,
    model: str = "voyage-3",
    batch_size: int = 128,
) -> Dict[str, Any]:
    """Split rules_text into sections, embed via Voyage, write to the
    `card_embeddings_v1.sqlite` file with source_type="rule".

    Returns: {status, sections, inserted, elapsed_s}.
    """
    t0 = time.perf_counter()
    sections = split_rules_into_sections(rules_text)
    if not sections:
        return {
            "status": "no_sections", "sections": 0,
            "inserted": 0, "elapsed_s": round(time.perf_counter() - t0, 2),
        }

    ensure_schema(db_path)

    try:
        import voyageai
    except ImportError:
        return {
            "status": "failed", "sections": len(sections),
            "inserted": 0, "elapsed_s": round(time.perf_counter() - t0, 2),
            "message": "voyageai not installed",
        }
    client = voyageai.Client()

    con = sqlite3.connect(str(db_path))
    inserted = 0
    try:
        # Find existing rule_ids to skip.
        existing = {
            r[0] for r in con.execute(
                "SELECT rule_id FROM card_embeddings WHERE source_type='rule'"
            )
        }
        pending = [s for s in sections if s["rule_id"] not in existing]
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            embeddings = _embed_batch(
                client, [s["text"] for s in batch], model,
            )
            with con:
                for s, emb in zip(batch, embeddings):
                    name_key = f"rule:{s['rule_id']}"
                    con.execute(
                        "INSERT OR REPLACE INTO card_embeddings "
                        "(name, source_type, rule_id, raw_text, vec, "
                        "color_identity, type_line, oracle_text, cmc, released_at) "
                        "VALUES (?, 'rule', ?, ?, ?, '', '', '', NULL, '')",
                        (name_key, s["rule_id"], s["text"], _pack_vec(emb)),
                    )
                    inserted += 1
    finally:
        con.close()

    return {
        "status": "ok", "sections": len(sections),
        "inserted": inserted, "elapsed_s": round(time.perf_counter() - t0, 2),
    }


# ============================================================
# Scryfall rulings embedding.
# ============================================================


def embed_scryfall_rulings(
    rulings_data: List[Dict[str, Any]],
    db_path: Path,
    model: str = "voyage-3",
    batch_size: int = 128,
) -> Dict[str, Any]:
    """Embed Scryfall rulings. Each `rulings_data` entry is
    {card_name, oracle_id, comment, published_at} (Scryfall's
    /cards/<id>/rulings endpoint output, augmented with card_name).

    Composite PK: f"ruling:{card_name}:{ordinal}" — ordinal within
    the card's ruling list (0-indexed).
    """
    t0 = time.perf_counter()
    if not rulings_data:
        return {"status": "no_data", "inserted": 0,
                "elapsed_s": round(time.perf_counter() - t0, 2)}

    ensure_schema(db_path)

    try:
        import voyageai
    except ImportError:
        return {
            "status": "failed", "inserted": 0,
            "elapsed_s": round(time.perf_counter() - t0, 2),
            "message": "voyageai not installed",
        }
    client = voyageai.Client()

    # Group rulings by card to compute ordinals.
    by_card: Dict[str, List[Dict[str, Any]]] = {}
    for r in rulings_data:
        cn = r.get("card_name") or ""
        if not cn:
            continue
        by_card.setdefault(cn, []).append(r)

    flat: List[Dict[str, Any]] = []
    for cn, rs in by_card.items():
        for i, r in enumerate(rs):
            comment = (r.get("comment") or "").strip()
            if not comment:
                continue
            flat.append({
                "card_name": cn, "ordinal": i, "text": comment,
                "name_key": f"ruling:{cn}:{i}",
            })

    if not flat:
        return {"status": "no_data", "inserted": 0,
                "elapsed_s": round(time.perf_counter() - t0, 2)}

    con = sqlite3.connect(str(db_path))
    inserted = 0
    try:
        existing = {
            r[0] for r in con.execute(
                "SELECT name FROM card_embeddings WHERE source_type='ruling'"
            )
        }
        pending = [r for r in flat if r["name_key"] not in existing]
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            embeddings = _embed_batch(
                client, [r["text"] for r in batch], model,
            )
            with con:
                for r, emb in zip(batch, embeddings):
                    con.execute(
                        "INSERT OR REPLACE INTO card_embeddings "
                        "(name, source_type, ruling_card, raw_text, vec, "
                        "color_identity, type_line, oracle_text, cmc, released_at) "
                        "VALUES (?, 'ruling', ?, ?, ?, '', '', '', NULL, '')",
                        (r["name_key"], r["card_name"], r["text"], _pack_vec(emb)),
                    )
                    inserted += 1
    finally:
        con.close()

    return {
        "status": "ok", "inserted": inserted,
        "elapsed_s": round(time.perf_counter() - t0, 2),
    }


# ============================================================
# Rules query.
# ============================================================


def query_rules(
    query_text: str,
    k: int = 5,
    source_type: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Semantic-search rules + rulings for `query_text`. Returns top-k
    matches as `[{name, source_type, rule_id, ruling_card, raw_text,
    similarity}]`.

    Embeds the query via Voyage, computes cosine similarity over the
    rules+rulings vectors (filtered by `source_type` if given).
    """
    from api.engine.layers.agent_semantic_retrieval_v1 import EMBEDDING_DB_PATH
    db_path = db_path or EMBEDDING_DB_PATH
    if not db_path.is_file():
        return []

    try:
        import numpy as np
        import voyageai
    except ImportError:
        return []

    # Embed the query.
    client = voyageai.Client()
    try:
        resp = client.embed([query_text], model="voyage-3", input_type="query")
        q_vec = list(resp.embeddings[0])
    except Exception:
        return []
    q_arr = np.array(q_vec, dtype=np.float32)
    q_norm = np.linalg.norm(q_arr)
    if q_norm > 0:
        q_arr = q_arr / q_norm

    con = sqlite3.connect(str(db_path))
    try:
        where = "WHERE source_type IN ('rule', 'ruling')"
        params: List[Any] = []
        if source_type:
            where = "WHERE source_type = ?"
            params = [source_type]
        rows = con.execute(
            f"SELECT name, source_type, rule_id, ruling_card, raw_text, vec "
            f"FROM card_embeddings {where}",
            params,
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return []

    # Compute cosine similarity for each row.
    vecs = np.array(
        [struct.unpack(f"<{len(row[5]) // 4}f", row[5]) for row in rows],
        dtype=np.float32,
    )
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    vecs = vecs / norms
    sims = vecs @ q_arr

    top = np.argpartition(-sims, min(k, len(sims) - 1))[:k]
    top = top[np.argsort(-sims[top])]

    return [
        {
            "name": rows[int(i)][0],
            "source_type": rows[int(i)][1],
            "rule_id": rows[int(i)][2],
            "ruling_card": rows[int(i)][3],
            "raw_text": rows[int(i)][4],
            "similarity": float(sims[int(i)]),
        }
        for i in top
    ]
