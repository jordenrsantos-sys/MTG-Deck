"""
deck_strength_check_v1 — Pillar A.4 strength oracle.

Measurement A (corpus cosine similarity) live; Measurement B (playtest)
opt-in via include_measurement_b=True, returns calibration_status:"uncalibrated"
until Phase 5b.3 benchmark passes.
"""
from __future__ import annotations

import itertools
import json
import math
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Per-call counter so concurrent writers in the same process never share a
# tmp filename. Combined with pid + uuid4 the tmp file is uniquely owned.
_ATOMIC_WRITE_COUNTER = itertools.count()
_ATOMIC_WRITE_LOCK = threading.Lock()

STRENGTH_CHECK_VERSION = "strength_check_v1.4_persistent_vector_cache"
_CORPUS_PATH = Path(__file__).resolve().parents[2] / "engine" / "data" / "corpus" / "corpus_v1.json"
# Mega-task v5 Phase 5: persist _CORPUS_VECTORS to disk so a fresh process
# doesn't re-vectorize the 13K-entry corpus on first build (which was a
# ~110-minute cold-start before this cache existed). The file is rewritten
# atomically after _ensure_vectors finishes incremental vectorization.
_CORPUS_VECTORS_PATH = Path(__file__).resolve().parents[2] / "engine" / "data" / "corpus" / "corpus_vectors_cache_v1.json"

_CORPUS_RAW: Dict[str, Any] = {}
_CORPUS_VECTORS: List[Dict[str, Any]] = []


def _atomic_write_json(path, data, indent=2):
    """Write `data` as JSON atomically.

    Mechanism: serialize-in-process via an in-process lock, then write to a
    per-call unique tmp file (`<path>.<pid>.<counter>.<uuid>.tmp`), fsync,
    and `os.replace` onto the destination. Readers see either the old
    complete file or the new complete file, never a half-written one.

    The per-call unique tmp name is load-bearing: a shared `<path>.tmp`
    would let writer A's `os.replace` move writer B's half-written buffer
    into place. The in-process lock additionally protects against
    interleaved writes in the same process (uvicorn worker, test
    harnesses, scripted ingest), which would otherwise still race on the
    destination even with unique tmp files. Cross-process callers are
    serialized at the destination by `os.replace`'s atomicity — the last
    writer wins, but no reader sees a torn file.

    Fixes the concurrent-write corruption observed 2026-05-17 when
    overlapping batch ingests + user-approved saves left corpus_v1.json
    in unparseable state.
    """
    p = Path(path)
    counter = next(_ATOMIC_WRITE_COUNTER)
    tmp = p.with_name(
        f"{p.name}.{os.getpid()}.{counter}.{uuid.uuid4().hex[:8]}.tmp"
    )
    with _ATOMIC_WRITE_LOCK:
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=indent)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except (AttributeError, OSError):
                    pass
            os.replace(tmp, p)
        finally:
            # Belt-and-suspenders: if anything above raised after tmp was
            # created but before os.replace consumed it, clean up.
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass


def _load_corpus() -> None:
    global _CORPUS_RAW, _CORPUS_VECTORS
    if _CORPUS_VECTORS:
        return
    try:
        with open(_CORPUS_PATH, "r", encoding="utf-8") as f:
            _CORPUS_RAW = json.load(f)
    except Exception:
        _CORPUS_RAW = {}
        _CORPUS_VECTORS = []
        return
    # Load persisted vectors if available (mega-task v5 Phase 5).
    # Schema: list of vector dicts identical to what _ensure_vectors appends.
    # If the file is missing, malformed, or has the wrong shape, fall back
    # silently to empty — _ensure_vectors will rebuild as needed (slow path).
    try:
        with open(_CORPUS_VECTORS_PATH, "r", encoding="utf-8") as f:
            persisted = json.load(f)
        if isinstance(persisted, list):
            _CORPUS_VECTORS = persisted
        else:
            _CORPUS_VECTORS = []
    except Exception:
        _CORPUS_VECTORS = []


def _ensure_vectors(db_snapshot_id: str) -> List[Dict[str, Any]]:
    """Return cached vectors for db_snapshot_id, vectorizing only entries that
    haven't been vectorized yet (incremental).

    Previously this rebuilt the entire cache after any cache-invalidate (e.g.
    after every batch ingest), which scaled O(N) per ingest and caused the
    archetype_brief flake observed during the manual sweeps. The incremental
    version tracks which corpus_ids are already vectorized for the snapshot
    and only computes deltas.

    Multi-snapshot caching: vectors for old snapshots remain in _CORPUS_VECTORS
    but are filtered out of the return value. They cost ~negligible memory
    (snapshot rollover is rare) and avoid recomputation if a query later
    targets an older snapshot.
    """
    _load_corpus()
    if not _CORPUS_RAW:
        return []
    decks = _CORPUS_RAW.get("decks", [])

    # Already-vectorized corpus_ids for this snapshot
    existing_ids_for_snapshot = {
        v.get("corpus_id") for v in _CORPUS_VECTORS
        if v.get("_snapshot") == db_snapshot_id and v.get("corpus_id")
    }

    # Identify entries needing vectorization (new since last call)
    todo: List[Dict[str, Any]] = []
    for entry in decks:
        if not isinstance(entry, dict):
            continue
        cid = entry.get("corpus_id")
        if cid and cid in existing_ids_for_snapshot:
            continue
        decklist = entry.get("decklist", [])
        if not isinstance(decklist, list):
            continue
        todo.append(entry)

    if todo:
        from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1
        # Mega-task v5 Phase 5: checkpoint every N entries so a long
        # vectorization run (~110 min for the full 13K-entry corpus) can be
        # killed and resumed without losing work. Each checkpoint writes the
        # full _CORPUS_VECTORS list atomically.
        CHECKPOINT_EVERY = 250
        appended_since_checkpoint = 0
        for entry in todo:
            decklist = entry.get("decklist", [])
            text = "Commander\n1 " + entry.get("commander", "") + "\nDeck\n"
            text += "\n".join("1 " + str(c) for c in decklist)
            try:
                an = compute_deck_analyze_v1(
                    db_snapshot_id=db_snapshot_id,
                    commander=entry.get("commander"),
                    raw_decklist_text=text, include_debug=False,
                )
            except Exception:
                continue
            _CORPUS_VECTORS.append({
                "_snapshot": db_snapshot_id,
                "corpus_id": entry.get("corpus_id"),
                "commander": entry.get("commander"),
                "archetype": entry.get("archetype"),
                "bracket": entry.get("bracket"),
                "source": entry.get("source"),
                "primitive_density": an.get("primitive_density", {}) or {},
                "subtype_density": an.get("subtype_density", {}) or {},
                "card_count": an.get("card_count", 0),
            })
            appended_since_checkpoint += 1
            if appended_since_checkpoint >= CHECKPOINT_EVERY:
                try:
                    _CORPUS_VECTORS_PATH.parent.mkdir(parents=True, exist_ok=True)
                    _atomic_write_json(_CORPUS_VECTORS_PATH, _CORPUS_VECTORS, indent=0)
                except Exception:
                    pass  # cache miss is recoverable; never fatal
                appended_since_checkpoint = 0

        # Final flush of any partial-batch progress.
        if appended_since_checkpoint > 0:
            try:
                _CORPUS_VECTORS_PATH.parent.mkdir(parents=True, exist_ok=True)
                _atomic_write_json(_CORPUS_VECTORS_PATH, _CORPUS_VECTORS, indent=0)
            except Exception:
                pass

    return [v for v in _CORPUS_VECTORS if v.get("_snapshot") == db_snapshot_id]


def compute_deck_strength_check_v1(*, db_snapshot_id, commander, raw_decklist_text,
                                   include_measurement_b=False, k_nearest=5):
    from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1
    warnings = []
    try:
        an = compute_deck_analyze_v1(
            db_snapshot_id=db_snapshot_id, commander=commander,
            raw_decklist_text=raw_decklist_text, include_debug=False,
        )
    except Exception as exc:
        warnings.append({"code": "ANALYZE_FAILED", "message": str(exc)})
        return _empty_response(warnings)

    candidate_prim = an.get("primitive_density", {}) or {}
    candidate_sub = an.get("subtype_density", {}) or {}
    corpus = _ensure_vectors(db_snapshot_id)
    if not corpus:
        warnings.append({"code": "CORPUS_EMPTY", "message": "No corpus entries loaded."})
        return _empty_response(warnings, candidate_analyze=an)

    neighbors = []
    for entry in corpus:
        sim = _cosine_combined(candidate_prim, candidate_sub,
                               entry["primitive_density"], entry["subtype_density"])
        devs = _axis_deviations(candidate_prim, entry["primitive_density"])
        neighbors.append({
            "corpus_id": entry["corpus_id"], "commander": entry["commander"],
            "archetype": entry["archetype"], "bracket": entry["bracket"],
            "source": entry["source"], "similarity": round(sim, 4),
            "deviations": devs[:8],
        })
    neighbors.sort(key=lambda n: -n["similarity"])
    top_k = neighbors[: max(1, int(k_nearest))]

    measurement_b = None
    if include_measurement_b:
        try:
            measurement_b = _run_measurement_b(
                db_snapshot_id=db_snapshot_id, commander=commander,
                raw_decklist_text=raw_decklist_text, corpus_neighbors=top_k,
            )
        except Exception as exc:
            warnings.append({"code": "MEASUREMENT_B_FAILED", "message": str(exc)})

    top = top_k[0] if top_k else None
    archetype_consensus = None
    archetype_breakdown = []
    flags = []
    if top is not None:
        scores = {}
        for n in top_k:
            arch = n.get("archetype") or "Unknown"
            sim = float(n.get("similarity", 0.0))
            scores[arch] = scores.get(arch, 0.0) + sim
        sorted_archs = sorted(scores.items(), key=lambda kv: -kv[1])
        total = sum(scores.values()) or 1.0
        archetype_breakdown = [{"archetype": a, "weighted_score": round(s, 4),
                                "weighted_share": round(s/total, 4)} for a, s in sorted_archs]
        archetype_consensus = sorted_archs[0][0]
        if len(sorted_archs) >= 2:
            top_share = sorted_archs[0][1]/total
            second_share = sorted_archs[1][1]/total
            if (top_share - second_share) < 0.15:
                archetype_consensus = sorted_archs[0][0] + " | ambiguous vs " + sorted_archs[1][0]
        for dev in top["deviations"]:
            delta = dev.get("delta", 0)
            if abs(delta) >= 3:
                flags.append({"axis": dev["axis"],
                              "severity": "HIGH" if abs(delta) >= 6 else "MEDIUM",
                              "your_count": dev["your_count"],
                              "corpus_count": dev["corpus_count"],
                              "delta": delta})

    if not top_k:
        band = "UNKNOWN"
    elif top_k[0]["similarity"] >= 0.85 and not flags:
        band = "STRONG_AND_ALIGNED"
    elif top_k[0]["similarity"] >= 0.7:
        band = "STRONG_BUT_GAPS"
    elif top_k[0]["similarity"] >= 0.5:
        band = "PROXY_OF_ARCHETYPE"
    else:
        band = "DIVERGENT"

    interpretation = _interpret(band, top_k[0] if top_k else None, flags)

    return {
        "version": STRENGTH_CHECK_VERSION, "db_snapshot_id": db_snapshot_id,
        "commander": commander, "candidate_card_count": an.get("card_count", 0),
        "measurement_a": {
            "kind": "corpus_similarity", "corpus_size": len(corpus),
            "k_nearest_returned": len(top_k), "nearest_neighbors": top_k,
            "archetype_consensus": archetype_consensus,
            "archetype_breakdown": archetype_breakdown,
            "axis_deviation_flags": flags,
        },
        "measurement_b": measurement_b,
        "combined_strength_band": band,
        "interpretation": interpretation,
        "warnings": warnings,
    }


def _empty_response(warnings, candidate_analyze=None):
    return {
        "version": STRENGTH_CHECK_VERSION, "db_snapshot_id": "", "commander": None,
        "candidate_card_count": (candidate_analyze or {}).get("card_count", 0),
        "measurement_a": {"kind": "corpus_similarity", "corpus_size": 0,
                          "k_nearest_returned": 0, "nearest_neighbors": [],
                          "archetype_consensus": None,
                          "archetype_breakdown": [],
                          "axis_deviation_flags": []},
        "measurement_b": None, "combined_strength_band": "UNKNOWN",
        "interpretation": "Corpus unavailable.", "warnings": warnings,
    }


def _cosine_combined(cand_prim, cand_sub, corp_prim, corp_sub):
    all_p = set(cand_prim) | set(corp_prim)
    all_s = set(cand_sub) | set(corp_sub)
    dot = cn = corn = 0.0
    for k in all_p:
        a = float(cand_prim.get(k, 0)); b = float(corp_prim.get(k, 0))
        dot += a*b; cn += a*a; corn += b*b
    for k in all_s:
        a = float(cand_sub.get(k, 0))*0.5; b = float(corp_sub.get(k, 0))*0.5
        dot += a*b; cn += a*a; corn += b*b
    if cn <= 0 or corn <= 0: return 0.0
    return dot / (math.sqrt(cn) * math.sqrt(corn))


def _axis_deviations(cand, corp):
    keys = set(cand) | set(corp)
    out = []
    for k in keys:
        a = int(cand.get(k, 0)); b = int(corp.get(k, 0))
        if a == 0 and b == 0: continue
        out.append({"axis": k, "your_count": a, "corpus_count": b, "delta": a-b})
    out.sort(key=lambda d: -abs(d["delta"]))
    return out


def _interpret(band, top, flags):
    if top is None: return "No corpus reference."
    if band == "STRONG_AND_ALIGNED":
        return "Composition aligns with successful " + str(top["archetype"]) + " builds (similarity " + str(top["similarity"]) + ")."
    if band == "STRONG_BUT_GAPS":
        gap = "; ".join(str(f["axis"]) + " " + ("+" if f["delta"] >= 0 else "") + str(f["delta"]) for f in flags[:3]) if flags else "minor differences"
        return "Resembles " + str(top["archetype"]) + " (similarity " + str(top["similarity"]) + ") with gaps: " + gap + "."
    if band == "PROXY_OF_ARCHETYPE":
        return "Loosely matches " + str(top["archetype"]) + " (similarity " + str(top["similarity"]) + ")."
    if band == "DIVERGENT":
        return "No close corpus match (top similarity " + str(top["similarity"]) + ")."
    return "Unknown band."


def _run_measurement_b(*, db_snapshot_id, commander, raw_decklist_text, corpus_neighbors):
    from api.engine.playtest.mpa_calibration import compute_measurement_b_hydrated
    cand_names, cand_cmdr = _decklist_to_card_names(commander, raw_decklist_text)
    if not cand_names or not cand_cmdr:
        return {"kind": "playtest_winrate", "calibration_status": "uncalibrated",
                "overall_winrate": None, "games_total": 0, "per_opponent": [],
                "notes": "Could not resolve candidate deck."}
    opposition = []
    for n in (corpus_neighbors or [])[:3]:
        cid = n.get("corpus_id")
        if not cid: continue
        opp_names, opp_cmdr = _corpus_entry_to_card_names(cid)
        if not opp_names or not opp_cmdr: continue
        opposition.append({"label": cid, "bracket": n.get("bracket"),
                           "commander_name": opp_cmdr, "deck_names": opp_names})
    if not opposition:
        return {"kind": "playtest_winrate", "calibration_status": "uncalibrated",
                "overall_winrate": None, "games_total": 0, "per_opponent": [],
                "notes": "No usable corpus opposition decks resolved."}
    return compute_measurement_b_hydrated(
        db_snapshot_id=db_snapshot_id, candidate_commander_name=cand_cmdr,
        candidate_deck_names=cand_names, opposition_panel=opposition,
        n_game_pairs_per_opponent=2, max_turns_per_game=20,
    )


def _decklist_to_card_names(commander, raw_decklist_text):
    from api.engine.decklist_parse_v1 import parse_decklist_text
    parsed = parse_decklist_text(raw_decklist_text)
    items = parsed.get("items", [])
    names = []
    cmdr_name = commander
    for it in items:
        section = it.get("section") or "mainboard"
        nm = it.get("name_raw")
        cnt = int(it.get("count", 1))
        if not isinstance(nm, str) or not nm.strip(): continue
        if section == "commander" and not cmdr_name:
            cmdr_name = nm.strip()
            continue
        for _ in range(max(1, cnt)):
            names.append(nm.strip())
    return names, cmdr_name


def _corpus_entry_to_card_names(corpus_id):
    _load_corpus()
    for entry in _CORPUS_RAW.get("decks", []) or []:
        if entry.get("corpus_id") != corpus_id: continue
        return list(entry.get("decklist", []) or []), entry.get("commander")
    return [], None


def ingest_user_approved_deck(*, db_snapshot_id, commander, decklist,
                              archetype=None, bracket=None, user_id=None):
    """Pillar C.2 hook. Per-event +/-0.03 caps per DESIGN_DECISIONS rule 1.3
    enforced in Phase 2.1.6. Audit-logged per rule 1.3 §4."""
    try:
        from api.engine.layers.self_learning_audit_v1 import log_event
    except Exception:
        log_event = None
    try:
        _load_corpus()
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        suffix = (user_id or "user")[:16].replace(" ", "_").lower()
        corpus_id = "user_" + suffix + "_" + commander.replace(" ", "_").lower()[:24] + "_" + ts
        new_entry = {
            "corpus_id": corpus_id,
            "commander": commander,
            "archetype": archetype or "User-approved",
            "bracket": bracket or "B3",
            "source": "user_approved",
            "source_url": "",
            "verification_status": "user_approved",
            "decklist": [str(c).strip() for c in decklist if isinstance(c, str) and str(c).strip()],
        }
        if "decks" not in _CORPUS_RAW or not isinstance(_CORPUS_RAW.get("decks"), list):
            _CORPUS_RAW["decks"] = []
        _CORPUS_RAW["decks"].append(new_entry)
        _atomic_write_json(_CORPUS_PATH, _CORPUS_RAW)
        # No cache invalidation needed: _ensure_vectors is incremental and will
        # vectorize this new entry on its next call via corpus_id diff.
        if log_event:
            try:
                log_event(
                    event_type="corpus_user_approved_ingest",
                    source="user_approved",
                    description="User approved deck ingested: " + commander + " (" + (bracket or "B3") + ")",
                    delta_summary=None,
                    within_caps=None,
                    is_architectural_shift=False,
                    requires_user_review=False,
                    extra={"corpus_id": corpus_id, "user_id": user_id, "archetype": archetype, "bracket": bracket},
                )
            except Exception:
                pass
        return {"status": "OK", "corpus_id": corpus_id, "bracket": bracket or "B3"}
    except Exception as exc:
        return {"status": "FAILED", "reason": exc.__class__.__name__ + ": " + str(exc)}
