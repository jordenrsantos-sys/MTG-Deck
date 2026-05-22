"""Mega-task v5 Phase 5: one-time warmup of the deck_strength_check_v1
corpus vector cache, parallelized across CPU cores.

The corpus has ~13K entries. Single-process vectorization (the original
naive _ensure_vectors path) is ~110 min. This script splits the corpus
across N worker processes (default 16) and merges results, so a fresh box
warms in ~10-15 min instead.

Output: `api/engine/data/corpus/corpus_vectors_cache_v1.json`. Once that
file exists, every subsequent process loads it in <1s via the disk-cache
hook added to deck_strength_check_v1._load_corpus().

Usage:
    python tools/warm_corpus_vector_cache.py --snapshot SNAP_ID [--workers N]

Safe to interrupt and resume: the script checks the existing cache file
for each snapshot's already-vectorized corpus_ids and only computes the
delta. Resuming after interruption skips entries that completed in the
previous run.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Repo root on sys.path so `from engine.db import ...` and
# `from api.engine.layers.* import ...` resolve.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _vectorize_chunk(args: Tuple[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Worker entry point: vectorize a chunk of corpus entries.

    Returns the vector dicts in the same shape that _ensure_vectors
    appends to _CORPUS_VECTORS. Caller merges and writes the cache.

    Must be top-level (Windows multiprocessing uses spawn → pickles
    the function).
    """
    snapshot_id, chunk = args
    # Re-import inside worker so spawn-mode Windows doesn't pickle anything heavy.
    sys.path.insert(0, str(_REPO_ROOT))
    from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1

    out: List[Dict[str, Any]] = []
    for entry in chunk:
        decklist = entry.get("decklist", [])
        text = "Commander\n1 " + entry.get("commander", "") + "\nDeck\n"
        text += "\n".join("1 " + str(c) for c in decklist)
        try:
            an = compute_deck_analyze_v1(
                db_snapshot_id=snapshot_id,
                commander=entry.get("commander"),
                raw_decklist_text=text,
                include_debug=False,
            )
        except Exception:
            continue
        out.append({
            "_snapshot": snapshot_id,
            "corpus_id": entry.get("corpus_id"),
            "commander": entry.get("commander"),
            "archetype": entry.get("archetype"),
            "bracket": entry.get("bracket"),
            "source": entry.get("source"),
            "primitive_density": an.get("primitive_density", {}) or {},
            "subtype_density": an.get("subtype_density", {}) or {},
            "card_count": an.get("card_count", 0),
        })
    return out


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", default=None,
                        help="Snapshot id. Required for now; auto-resolve "
                             "will be added once we re-confirm the snapshots "
                             "table schema.")
    parser.add_argument("--workers", type=int, default=16,
                        help="Number of worker processes (default: 16).")
    args = parser.parse_args(argv)
    if not args.snapshot:
        raise SystemExit("--snapshot is required.")
    snapshot = args.snapshot

    from api.engine.layers.deck_strength_check_v1 import (
        _CORPUS_PATH, _CORPUS_VECTORS_PATH, _atomic_write_json,
    )

    print(f"Warming corpus vector cache for snapshot: {snapshot}", flush=True)
    print(f"Source corpus:  {_CORPUS_PATH}", flush=True)
    print(f"Cache file:     {_CORPUS_VECTORS_PATH}", flush=True)
    print(f"Worker count:   {args.workers}", flush=True)

    # Load corpus
    with open(_CORPUS_PATH, "r", encoding="utf-8") as f:
        corpus_raw = json.load(f)
    decks = [d for d in corpus_raw.get("decks", []) if isinstance(d, dict)]
    print(f"Total corpus entries: {len(decks)}", flush=True)

    # Load existing cache (resume support)
    existing_vectors: List[Dict[str, Any]] = []
    try:
        with open(_CORPUS_VECTORS_PATH, "r", encoding="utf-8") as f:
            persisted = json.load(f)
        if isinstance(persisted, list):
            existing_vectors = persisted
    except Exception:
        pass

    already_done = {
        v.get("corpus_id") for v in existing_vectors
        if v.get("_snapshot") == snapshot and v.get("corpus_id")
    }
    todo = [e for e in decks
            if e.get("corpus_id") and e.get("corpus_id") not in already_done]
    print(f"Already vectorized for this snapshot: {len(already_done)}", flush=True)
    print(f"Remaining to vectorize:                {len(todo)}", flush=True)
    print(f"Estimated wall time (16 workers):      {len(todo) * 0.5 / args.workers / 60:.1f} min", flush=True)

    if not todo:
        print("Nothing to do.", flush=True)
        return 0

    # Split into chunks — one chunk per worker, evenly sized
    n = args.workers
    chunk_size = (len(todo) + n - 1) // n
    chunks: List[List[Dict[str, Any]]] = [
        todo[i:i + chunk_size] for i in range(0, len(todo), chunk_size)
    ]
    print(f"Split into {len(chunks)} chunks of ~{chunk_size} entries each", flush=True)

    t0 = time.perf_counter()
    new_vectors: List[Dict[str, Any]] = []
    with mp.Pool(processes=args.workers) as pool:
        for i, result_chunk in enumerate(
            pool.imap_unordered(_vectorize_chunk,
                                [(snapshot, chunk) for chunk in chunks])
        ):
            new_vectors.extend(result_chunk)
            elapsed = time.perf_counter() - t0
            print(f"  chunk {i+1}/{len(chunks)} returned {len(result_chunk)} vectors "
                  f"(total so far: {len(new_vectors)}, elapsed: {elapsed:.1f}s)", flush=True)
            # Checkpoint after each chunk so a Ctrl-C doesn't lose work
            combined = existing_vectors + new_vectors
            try:
                _CORPUS_VECTORS_PATH.parent.mkdir(parents=True, exist_ok=True)
                _atomic_write_json(_CORPUS_VECTORS_PATH, combined, indent=0)
            except Exception as exc:
                print(f"  WARN: checkpoint write failed: {exc}", flush=True)

    elapsed_total = time.perf_counter() - t0
    final_count = len(existing_vectors) + len(new_vectors)
    print(f"DONE: vectorized {len(new_vectors)} new entries in "
          f"{elapsed_total:.1f}s ({elapsed_total/60:.1f} min)", flush=True)
    print(f"Final cache size: {final_count} entries", flush=True)
    print(f"Cache file: {_CORPUS_VECTORS_PATH} "
          f"({_CORPUS_VECTORS_PATH.stat().st_size / 1024 / 1024:.1f} MB)",
          flush=True)
    return 0


if __name__ == "__main__":
    # Windows multiprocessing requires this guard for spawn-mode safety.
    mp.freeze_support()
    sys.exit(main())
