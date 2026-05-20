"""Phase 5b.5 — Opposition Deck Registry tests.

Pins the contract for the curated registry of standardized opponents per
bracket. The calibration suite (5b.3) consumes this registry to pick
calibration matchups by role_tag. Without role_tag stability, calibration
runs are not reproducible across module changes.

Asserts:
  1. The JSON registry file loads and has the expected schema.
  2. Every entry's corpus_id resolves to a real deck in corpus_v1.json.
  3. role_tags are unique within each bracket (no overlap → calibration can
     unambiguously pick "the B2-precon-elven-empire opponent").
  4. The 7 Wizards B2 precons confirmed present in corpus are all anchored
     (these are the canonical B2 floor — irreplaceable for calibration).
  5. The Python module exposes load_registry() + get_by_role_tag().
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = REPO_ROOT / "api" / "engine" / "data" / "playtest" / "opposition_decks_v1.json"
CORPUS_PATH = REPO_ROOT / "api" / "engine" / "data" / "corpus" / "corpus_v1.json"


def _load_corpus_index() -> Dict[str, Dict[str, Any]]:
    with open(CORPUS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {e["corpus_id"]: e for e in data.get("decks", [])}


def _load_registry() -> Dict[str, Any]:
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def test_registry_file_exists_and_loads():
    assert REGISTRY_PATH.exists(), f"Registry file missing at {REGISTRY_PATH}"
    reg = _load_registry()
    assert isinstance(reg, dict)
    assert "version" in reg
    assert "entries" in reg
    assert isinstance(reg["entries"], list)
    assert len(reg["entries"]) >= 15, "Expect at least 15 entries (3+7+3+3+3 minimum)"


def test_registry_schema_per_entry():
    reg = _load_registry()
    required_keys = {"corpus_id", "commander", "bracket", "archetype_hint", "role_tag"}
    for entry in reg["entries"]:
        missing = required_keys - set(entry.keys())
        assert not missing, f"Entry {entry.get('corpus_id', '<no id>')} missing keys: {missing}"
        assert entry["bracket"] in {"B1", "B2", "B3", "B4", "B5"}
        assert isinstance(entry["role_tag"], str) and entry["role_tag"].strip() != ""
        assert isinstance(entry["commander"], str) and entry["commander"].strip() != ""


def test_every_entry_resolves_to_corpus():
    reg = _load_registry()
    corpus = _load_corpus_index()
    missing: List[str] = []
    bracket_mismatch: List[str] = []
    for entry in reg["entries"]:
        cid = entry["corpus_id"]
        if cid not in corpus:
            missing.append(cid)
            continue
        corpus_entry = corpus[cid]
        # Bracket in registry must match corpus (sanity — prevents stale entries)
        if corpus_entry.get("bracket") != entry["bracket"]:
            bracket_mismatch.append(
                f"{cid}: registry={entry['bracket']} corpus={corpus_entry.get('bracket')}"
            )
        # Commander too
        if corpus_entry.get("commander") != entry["commander"]:
            bracket_mismatch.append(
                f"{cid}: commander registry={entry['commander']!r} corpus={corpus_entry.get('commander')!r}"
            )
    assert not missing, f"Entries missing from corpus: {missing}"
    assert not bracket_mismatch, f"Registry/corpus mismatches: {bracket_mismatch}"


def test_role_tags_unique_within_bracket():
    reg = _load_registry()
    by_bracket: Dict[str, Set[str]] = {}
    duplicates: List[str] = []
    for entry in reg["entries"]:
        b = entry["bracket"]
        tag = entry["role_tag"]
        seen = by_bracket.setdefault(b, set())
        if tag in seen:
            duplicates.append(f"{b}/{tag}")
        seen.add(tag)
    assert not duplicates, f"Duplicate role_tags within bracket: {duplicates}"


def test_b2_wizards_precons_all_anchored():
    """The 7 Wizards precons confirmed in corpus must all be present in the
    B2 anchor set. These are the canonical "known-weak" opposition for
    calibration — removing one undermines the calibration matrix."""
    reg = _load_registry()
    b2_archetypes_lower = {
        (e["archetype_hint"] or "").lower()
        for e in reg["entries"]
        if e["bracket"] == "B2"
    }
    expected_precons = [
        "draconic domination",
        "breed lethality",
        "heavenly inferno",
        "elven empire",
        "mutant menace",
        "peace offering",
        "scions",  # "Scions & Spellcraft" — substring match
    ]
    missing = [
        p for p in expected_precons
        if not any(p in arch for arch in b2_archetypes_lower)
    ]
    assert not missing, f"Missing B2 Wizards precon anchors: {missing}"


def test_python_module_loads_registry():
    from api.engine.playtest.opposition_decks_v1 import load_registry
    reg = load_registry()
    assert isinstance(reg, dict)
    assert "entries" in reg
    assert len(reg["entries"]) >= 15


def test_python_module_get_by_role_tag():
    from api.engine.playtest.opposition_decks_v1 import get_by_role_tag
    # Pick an anchor we know must exist
    entry = get_by_role_tag("B2-precon-draconic-domination")
    assert entry is not None
    assert entry["bracket"] == "B2"
    assert entry["commander"] == "The Ur-Dragon"


def test_python_module_get_by_role_tag_returns_none_for_unknown():
    from api.engine.playtest.opposition_decks_v1 import get_by_role_tag
    assert get_by_role_tag("not-a-real-tag") is None


def test_python_module_filter_by_bracket():
    from api.engine.playtest.opposition_decks_v1 import filter_by_bracket
    b2 = filter_by_bracket("B2")
    assert len(b2) >= 7, "B2 must have at least 7 Wizards precons"
    assert all(e["bracket"] == "B2" for e in b2)


def test_endpoint_get_full_registry():
    from fastapi.testclient import TestClient
    from api.main import app
    client = TestClient(app)
    resp = client.get("/playtest/opposition_decks_v1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == "opposition_decks_v1"
    assert "summary" in body
    assert body["summary"]["total_entries"] >= 15
    assert "B2" in body["summary"]["per_bracket_count"]
    assert body["summary"]["per_bracket_count"]["B2"] >= 7
    assert len(body["entries"]) == body["summary"]["total_entries"]
    assert body["warnings"] == []


def test_endpoint_filter_by_bracket():
    from fastapi.testclient import TestClient
    from api.main import app
    client = TestClient(app)
    resp = client.get("/playtest/opposition_decks_v1?bracket=B2")
    assert resp.status_code == 200
    body = resp.json()
    assert all(e["bracket"] == "B2" for e in body["entries"])
    assert len(body["entries"]) >= 7


def test_endpoint_filter_by_role_tag():
    from fastapi.testclient import TestClient
    from api.main import app
    client = TestClient(app)
    resp = client.get("/playtest/opposition_decks_v1?role_tag=B2-precon-draconic-domination")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["entries"]) == 1
    assert body["entries"][0]["commander"] == "The Ur-Dragon"
    assert body["warnings"] == []


def test_endpoint_unknown_role_tag_returns_warning():
    from fastapi.testclient import TestClient
    from api.main import app
    client = TestClient(app)
    resp = client.get("/playtest/opposition_decks_v1?role_tag=nope")
    assert resp.status_code == 200
    body = resp.json()
    assert body["entries"] == []
    assert any(w["code"] == "ROLE_TAG_NOT_FOUND" for w in body["warnings"])


def test_endpoint_invalid_bracket_returns_warning():
    from fastapi.testclient import TestClient
    from api.main import app
    client = TestClient(app)
    resp = client.get("/playtest/opposition_decks_v1?bracket=B9")
    assert resp.status_code == 200
    body = resp.json()
    assert body["entries"] == []
    assert any(w["code"] == "INVALID_BRACKET" for w in body["warnings"])
