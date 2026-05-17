"""
corpus_batch_ingest_v1 — Phase 5a EDHREC corpus expansion + auto-bump verification.

Per user rule (2026-05-17):
  - Compute min_legal_bracket from deck contents (game changers + 2-card combos
    + other high-power signatures).
  - If claimed_bracket >= min_legal_bracket: ACCEPT as-is (overlabeling is fine
    — user knows their deck is stronger than minimum and chose to label up).
  - If claimed_bracket < min_legal_bracket: AUTO-BUMP to min_legal_bracket
    and ACCEPT with corrected bracket. Log the correction in the audit trail.

Hard rejections only for malformed input (missing commander, decklist < 50, invalid bracket).
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


VERSION = "corpus_batch_ingest_v1.4_atomic_writes_and_module_attr"

_REPO_ROOT = Path(__file__).resolve().parents[3]  # api/engine/layers/THIS -> repo/
_REVIEW_QUEUE_PATH = _REPO_ROOT / "api" / "engine" / "data" / "corpus" / "corpus_review_queue_v1.jsonl"


# ============================================================
# Bracket order helpers
# ============================================================

_BRACKET_ORDER = ["B1", "B2", "B3", "B4", "B5"]


def _bracket_max(*brackets):
    """Return the highest bracket from the list, ignoring None."""
    valid = [b for b in brackets if b in _BRACKET_ORDER]
    if not valid:
        return None
    return max(valid, key=lambda b: _BRACKET_ORDER.index(b))


def _bracket_ge(a: str, b: str) -> bool:
    """True if bracket a is >= bracket b."""
    return _BRACKET_ORDER.index(a) >= _BRACKET_ORDER.index(b)


# ============================================================
# Main batch ingest
# ============================================================


def batch_ingest_external_decks(
    *,
    db_snapshot_id: str,
    entries: List[Dict[str, Any]],
    skip_bracket_verification: bool = False,
) -> Dict[str, Any]:
    """Ingest a batch of external decks with auto-bump bracket correction."""
    # Import the module, NOT the names. `_load_corpus()` REBINDS the
    # module-level `_CORPUS_RAW` to a fresh dict (see deck_strength_check_v1
    # line ~84: `_CORPUS_RAW = json.load(f)`). Importing the name by `from
    # ... import _CORPUS_RAW` captures the empty `{}` at import time and
    # never sees the loaded dict — causing every batch ingest to write a
    # single-entry file and wipe the rest of the corpus. The fix is
    # attribute access on the module (always reads the live binding).
    # Same pattern used by agent_endpoints_v1.py.
    from api.engine.layers import deck_strength_check_v1 as _sc
    _load_corpus = _sc._load_corpus
    _CORPUS_PATH = _sc._CORPUS_PATH
    _atomic_write_json = _sc._atomic_write_json
    from api.engine.layers.self_learning_audit_v1 import log_event

    _load_corpus()
    results: List[Dict[str, Any]] = []
    accepted = 0
    auto_bumped = 0
    rejected = 0
    needs_review = 0
    review_queue: List[Dict[str, Any]] = []

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    for idx, entry in enumerate(entries or []):
        commander = entry.get("commander")
        decklist = entry.get("decklist")
        claimed_bracket = entry.get("claimed_bracket")
        source_url = entry.get("source_url", "")
        source_label = entry.get("source_label") or "edhrec_import_" + str(idx)
        archetype_hint = entry.get("archetype_hint") or "External (EDHREC)"

        # --- Hard-rejection guards ---
        if not isinstance(commander, str) or not commander.strip():
            results.append({"index": idx, "status": "REJECTED_INVALID", "reason": "commander missing"})
            rejected += 1
            continue
        if not isinstance(decklist, list) or len(decklist) < 50:
            results.append({
                "index": idx, "status": "REJECTED_INVALID",
                "reason": "decklist must be list of >=50 cards; got " + str(len(decklist) if isinstance(decklist, list) else "non-list"),
                "commander": commander,
            })
            rejected += 1
            continue
        if claimed_bracket not in _BRACKET_ORDER:
            results.append({
                "index": idx, "status": "REJECTED_INVALID",
                "reason": "claimed_bracket must be B1..B5, got " + repr(claimed_bracket),
                "commander": commander,
            })
            rejected += 1
            continue

        # --- Compute min_legal_bracket ---
        if skip_bracket_verification:
            effective_bracket = claimed_bracket
            verification_status = "user_set_unverified"
            bump_reason: Optional[str] = None
            min_legal = claimed_bracket
            signatures: Dict[str, Any] = {}
        else:
            verdict = _compute_min_legal_bracket(
                db_snapshot_id=db_snapshot_id,
                commander=commander,
                decklist=decklist,
            )
            min_legal = verdict["min_legal_bracket"]
            signatures = verdict["signatures"]

            if _bracket_ge(claimed_bracket, min_legal):
                effective_bracket = claimed_bracket
                verification_status = "engine_verified"
                bump_reason = None
            else:
                effective_bracket = min_legal
                verification_status = "auto_bumped"
                bump_reason = verdict.get("reason_text") or "deck contents minimum > claimed"

        # --- Build corpus entry ---
        slug = commander.replace(" ", "_").replace(",", "").lower()[:30]
        corpus_id = "edhrec_" + slug + "_" + effective_bracket.lower() + "_" + source_label + "_" + timestamp + "_" + str(idx)
        new_entry = {
            "corpus_id": corpus_id,
            "commander": commander,
            "archetype": archetype_hint,
            "bracket": effective_bracket,
            "claimed_bracket": claimed_bracket,
            "source": "edhrec_external_" + timestamp,
            "source_url": source_url,
            "verification_status": verification_status,
            "verification_signatures": signatures,
            "bump_reason": bump_reason,
            "decklist": [str(c).strip() for c in decklist if isinstance(c, str) and str(c).strip()],
        }
        if "decks" not in _sc._CORPUS_RAW or not isinstance(_sc._CORPUS_RAW.get("decks"), list):
            _sc._CORPUS_RAW["decks"] = []
        _sc._CORPUS_RAW["decks"].append(new_entry)

        # --- Result + counts ---
        result_entry = {
            "index": idx,
            "corpus_id": corpus_id,
            "commander": commander,
            "claimed_bracket": claimed_bracket,
            "effective_bracket": effective_bracket,
            "verification_status": verification_status,
            "signatures": signatures,
        }
        if verification_status == "auto_bumped":
            result_entry["status"] = "AUTO_BUMPED"
            result_entry["bump_reason"] = bump_reason
            auto_bumped += 1
            review_queue.append({
                "corpus_id": corpus_id,
                "commander": commander,
                "claimed_bracket": claimed_bracket,
                "auto_bumped_to": effective_bracket,
                "bump_reason": bump_reason,
                "signatures": signatures,
                "source_url": source_url,
            })
        elif verification_status == "needs_review":
            result_entry["status"] = "NEEDS_REVIEW"
            needs_review += 1
            review_queue.append({
                "corpus_id": corpus_id,
                "commander": commander,
                "claimed_bracket": claimed_bracket,
                "needs_review_reason": bump_reason,
                "source_url": source_url,
            })
        else:
            result_entry["status"] = "ACCEPTED"
            accepted += 1
        results.append(result_entry)

    # --- Persist corpus (atomic: write to .tmp, fsync, os.replace) ---
    # Prevents concurrent-write corruption between batch_ingest_external_decks
    # and ingest_user_approved_deck — readers see either the old complete file
    # or the new complete file, never a partial write.
    try:
        _atomic_write_json(_CORPUS_PATH, _sc._CORPUS_RAW)
    except Exception as exc:
        return {"version": VERSION, "status": "FAILED",
                "reason": "corpus write: " + str(exc), "results": results}

    # Invalidate cached vectors so strength_check sees new entries
    try:
        from api.engine.layers import deck_strength_check_v1 as _sc
        _sc._CORPUS_VECTORS = []
    except Exception:
        pass

    # --- Review queue + audit log ---
    if review_queue:
        try:
            _REVIEW_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_REVIEW_QUEUE_PATH, "a", encoding="utf-8") as f:
                for r in review_queue:
                    r["queued_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    f.write(json.dumps(r) + "\n")
        except Exception:
            pass

    try:
        log_event(
            event_type="corpus_batch_ingest",
            source="edhrec_external",
            description=("Batch ingest: " + str(accepted) + " accepted, "
                         + str(auto_bumped) + " auto-bumped, "
                         + str(needs_review) + " needs-review, "
                         + str(rejected) + " rejected."),
            delta_summary=None,
            within_caps=None,
            is_architectural_shift=False,
            requires_user_review=False,  # auto-bump is automatic, not user-gated
            extra={
                "total_submitted": len(entries),
                "accepted": accepted,
                "auto_bumped": auto_bumped,
                "needs_review": needs_review,
                "rejected": rejected,
                "timestamp": timestamp,
            },
        )
    except Exception:
        pass

    return {
        "version": VERSION,
        "status": "OK",
        "total_submitted": len(entries),
        "accepted": accepted,
        "auto_bumped": auto_bumped,
        "needs_review": needs_review,
        "rejected": rejected,
        "results": results,
    }


# ============================================================
# Min legal bracket computation
# ============================================================


def _compute_min_legal_bracket(
    *,
    db_snapshot_id: str,
    commander: str,
    decklist: List[str],
) -> Dict[str, Any]:
    """Compute the minimum legal bracket for a deck based on its contents.

    Combines:
      1. Game Changers (engine/game_changers.py — 51-card CFP list, 1-3 GCs → B3, 4+ → B4)
      2. Two-card combo pairs (early → B4 floor, late → B3 floor)

    Returns:
      {
        "min_legal_bracket": "B1..B5",
        "reason_text": "human-readable",
        "signatures": {
          "game_changers": [...], "early_combo_pairs": [...], "late_combo_pairs": [...]
          
        }
      }
    """
    sigs: Dict[str, Any] = {}
    reasons: List[str] = []

    # Include commander in combo-pair detection. Many 2-card combos use
    # the commander as one half (Krenko + Thornbite Staff, Najeela + Bear Umbra,
    # Kiki-Jiki as commander + Conspicuous Snoop, etc.). Stripping the commander
    # would make commander-shell combos invisible to the verifier.
    deck_lower = {str(c).strip().lower() for c in decklist if isinstance(c, str)}
    if isinstance(commander, str) and commander.strip():
        deck_lower.add(commander.strip().lower())
    full_deck_names = [commander] + [c for c in decklist if isinstance(c, str)]

    # 1. Game Changers
    gc_floor = None
    gc_found: List[str] = []
    try:
        from engine.game_changers import load_game_changers, detect_game_changers, bracket_floor_from_count
        _gc_ver, _gc_path, gc_set = load_game_changers(_REPO_ROOT)
        gc_found, gc_count = detect_game_changers(
            playable_names=list(decklist),
            commander_name=commander,
            gc_set=gc_set,
        )
        if gc_count > 0:
            gc_floor = bracket_floor_from_count(gc_count)
            reasons.append(str(gc_count) + " Game Changer(s) → floor " + str(gc_floor) + ": " + ", ".join(gc_found[:5]) + (" ..." if len(gc_found) > 5 else ""))
    except Exception as exc:
        sigs["game_changers_error"] = exc.__class__.__name__ + ": " + str(exc)
    sigs["game_changers"] = gc_found

    # 2. 2-card combo pairs (hand-curated for now; replace with EDHREC scrape in Phase 5a.2)
    TWO_CARD_COMBO_PAIRS = [
        # EARLY (B4/B5 only)
        ("kiki-jiki, mirror breaker", "conspicuous snoop", "early"),
        ("kiki-jiki, mirror breaker", "pestermite", "early"),
        ("kiki-jiki, mirror breaker", "deceiver exarch", "early"),
        ("kiki-jiki, mirror breaker", "zealous conscripts", "early"),
        ("kiki-jiki, mirror breaker", "combat celebrant", "early"),
        ("kiki-jiki, mirror breaker", "felidar guardian", "early"),
        ("splinter twin", "pestermite", "early"),
        ("splinter twin", "deceiver exarch", "early"),
        ("thassa's oracle", "demonic consultation", "early"),
        ("thassa's oracle", "tainted pact", "early"),
        ("laboratory maniac", "demonic consultation", "early"),
        ("laboratory maniac", "tainted pact", "early"),
        ("jace, wielder of mysteries", "demonic consultation", "early"),
        ("jace, wielder of mysteries", "tainted pact", "early"),
        ("isochron scepter", "dramatic reversal", "early"),
        ("food chain", "eternal scourge", "early"),
        ("food chain", "misthollow griffin", "early"),
        ("food chain", "squee, the immortal", "early"),
        ("dockside extortionist", "temur sabertooth", "early"),
        ("underworld breach", "lion's eye diamond", "early"),
        ("underworld breach", "brain freeze", "early"),
        ("aetherflux reservoir", "bolas's citadel", "early"),
        ("aetherflux reservoir", "sensei's divining top", "early"),
        ("worldgorger dragon", "animate dead", "early"),
        ("worldgorger dragon", "necromancy", "early"),
        ("worldgorger dragon", "dance of the dead", "early"),
        ("protean hulk", "flash", "early"),
        ("necrotic ooze", "phyrexian devourer", "early"),
        ("necrotic ooze", "walking ballista", "early"),
        # LATE (B3/B4/B5)
        ("helm of obedience", "rest in peace", "late"),
        ("helm of obedience", "leyline of the void", "late"),
        ("mikaeus, the unhallowed", "triskelion", "late"),
        ("mikaeus, the unhallowed", "walking ballista", "late"),
        ("exquisite blood", "sanguine bond", "late"),
        ("exquisite blood", "vito, thorn of the dusk rose", "late"),
        ("felidar guardian", "saheeli rai", "late"),
    ]
    early_hits = []
    late_hits = []
    for a, b, kind in TWO_CARD_COMBO_PAIRS:
        if a in deck_lower and b in deck_lower:
            if kind == "early":
                early_hits.append((a, b))
            else:
                late_hits.append((a, b))
    sigs["early_combo_pairs"] = early_hits
    sigs["late_combo_pairs"] = late_hits

    combo_floor = None
    if early_hits:
        combo_floor = "B4"
        reasons.append(str(len(early_hits)) + " early-game 2-card combo pair(s) → floor B4: " + str(early_hits[:2]))
    elif late_hits:
        combo_floor = "B3"
        reasons.append(str(len(late_hits)) + " late-game 2-card combo pair(s) -> floor B3: " + str(late_hits[:2]))

    # Fast-mana and tutor-density soft signals removed 2026-05-17 per user
    # direction: those cards are mostly on the Game Changers list anyway, so
    # the GC floor catches them. Removing the soft signals avoids
    # double-counting and keeps the rule clean: only GCs + 2-card combo pairs
    # drive bracket floors. If a B1/B2 deck contains Demonic Tutor that ISN'T
    # on the GC list (e.g. some weaker tutor variant), the user accepts it
    # at face value.

    # Final: take the max of GC + combo floors (default B1 if no signatures)
    min_legal = _bracket_max(gc_floor, combo_floor) or "B1"

    return {
        "min_legal_bracket": min_legal,
        "reason_text": "; ".join(reasons) if reasons else "no high-power signatures detected -> B1 eligible",
        "signatures": sigs,
    }
