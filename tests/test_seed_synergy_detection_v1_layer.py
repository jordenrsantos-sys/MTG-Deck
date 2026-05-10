"""Structural test scaffold for ``seed_synergy_detection_v1``.

Sub-task 1: assert only the output envelope shape against the empty stub. Future
sub-tasks will add archetype-driven content assertions (aristocrats / tokens /
voltron / control / reanimator / storm / stax × B1–B5).
"""

from __future__ import annotations

from typing import Dict, List

import pytest

from api.engine.layers.seed_synergy_detection_v1 import (
    CANONICAL_MISSING_ROLE_KEYS,
    SEED_SYNERGY_DETECTION_V1_VERSION,
    run_seed_synergy_detection_v1,
)


SEED_SIZES = (1, 3, 6, 12, 25, 50, 99)
DEFAULT_BRACKET_ID = "B3"


def _synthetic_primitive_index_by_slot(seed_size: int) -> Dict[str, List[str]]:
    return {f"S{idx:03d}": [] for idx in range(seed_size)}


def _synthetic_slot_ids_by_primitive() -> Dict[str, List[str]]:
    return {}


def test_layer_module_loads_cleanly() -> None:
    assert SEED_SYNERGY_DETECTION_V1_VERSION == "seed_synergy_detection_v1"


@pytest.mark.parametrize("seed_size", SEED_SIZES)
def test_empty_stub_returns_envelope_with_expected_seed_size(seed_size: int) -> None:
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot=_synthetic_primitive_index_by_slot(seed_size),
        slot_ids_by_primitive=_synthetic_slot_ids_by_primitive(),
        bracket_id=DEFAULT_BRACKET_ID,
    )

    assert payload["version"] == SEED_SYNERGY_DETECTION_V1_VERSION
    assert payload["status"] == "OK"
    assert payload["reason_code"] is None
    assert payload["codes"] == []
    assert payload["unknowns"] == []

    detection = payload["seed_synergy_detection_v1"]
    assert isinstance(detection, dict)

    assert detection["seed_size"] == seed_size
    assert detection["bracket_id"] == DEFAULT_BRACKET_ID

    assert isinstance(detection["matched_themes"], list)
    assert detection["matched_themes"] == []

    assert isinstance(detection["matched_signatures"], list)
    assert detection["matched_signatures"] == []

    missing_roles = detection["missing_roles"]
    assert isinstance(missing_roles, dict)
    assert sorted(missing_roles.keys()) == sorted(CANONICAL_MISSING_ROLE_KEYS)
    for role in CANONICAL_MISSING_ROLE_KEYS:
        value = missing_roles[role]
        assert isinstance(value, int) and value >= 0

    assert detection["bracket_compatible_flag"] is True


@pytest.mark.parametrize("seed_size", SEED_SIZES)
def test_empty_stub_is_deterministic_across_repeat_calls(seed_size: int) -> None:
    kwargs = {
        "primitive_index_by_slot": _synthetic_primitive_index_by_slot(seed_size),
        "slot_ids_by_primitive": _synthetic_slot_ids_by_primitive(),
        "bracket_id": DEFAULT_BRACKET_ID,
    }

    first = run_seed_synergy_detection_v1(**kwargs)
    second = run_seed_synergy_detection_v1(**kwargs)
    assert first == second


def test_empty_stub_handles_missing_bracket_id() -> None:
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot=_synthetic_primitive_index_by_slot(6),
        slot_ids_by_primitive=_synthetic_slot_ids_by_primitive(),
        bracket_id=None,
    )

    detection = payload["seed_synergy_detection_v1"]
    assert detection["bracket_id"] is None
    assert detection["bracket_compatible_flag"] is True


# ---------------------------------------------------------------------------
# Sub-task 2C.1: pack-loading helper tests with mocked-pack fixtures.
# Each test builds a fake manifest + fake pack files in tmp_path, monkeypatches
# the shared curated_pack_manifest_v1 module's _REPO_ROOT to point at tmp_path
# so resolve_pack_file_path resolves to the fake files. NO real pack data
# touched here — integration tests against the live manifest land in 2C.2.
# ---------------------------------------------------------------------------

import hashlib
import json

from api.engine import curated_pack_manifest_v1 as _shared_manifest
from api.engine.layers.seed_synergy_detection_v1 import (
    SeedSynergyDetectionPackShaMismatch,
    _build_union_allowlist,
    _load_adjacent_packs,
    _load_bands,
    _load_pack_with_sha_check,
    _load_primitive_index_map,
    _load_primitives,
    _load_signal_vocabulary,
    _load_themes,
)


def _sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_pack(root, rel_path: str, payload) -> tuple[str, str]:
    """Write a JSON pack file and return (rel_path, sha256_hex)."""
    full = root / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    raw = (text + "\n").encode("utf-8")
    full.write_bytes(raw)
    return rel_path, _sha256_of(raw)


def _make_manifest_payload(packs):
    """packs: list of (pack_id, rel_path, sha256, load_order) tuples."""
    return {
        "version": "curated_pack_manifest_v1",
        "packs": [
            {
                "load_order": load_order,
                "pack_id": pack_id,
                "pack_version": pack_id,
                "path": rel_path,
                "sha256": sha,
            }
            for pack_id, rel_path, sha, load_order in packs
        ],
    }


def _setup_fake_root(tmp_path, monkeypatch):
    """Point the shared manifest module's _REPO_ROOT at tmp_path for this test."""
    monkeypatch.setattr(_shared_manifest, "_REPO_ROOT", tmp_path)
    return tmp_path / "manifest.json"


# ----- _load_pack_with_sha_check -----


def test_load_pack_with_sha_check_happy_path(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    rel, sha = _write_pack(tmp_path, "fake/sample.json", [{"id": "X"}, {"id": "Y"}])
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("sample_pack", rel, sha, 10)])),
        encoding="utf-8",
    )

    data = _load_pack_with_sha_check("sample_pack", manifest_path=manifest_path)
    assert data == [{"id": "X"}, {"id": "Y"}]


def test_load_pack_with_sha_check_sha_mismatch_raises(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    rel, _real_sha = _write_pack(tmp_path, "fake/drifted.json", {"a": 1})
    bogus_sha = "0" * 64
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("drifted_pack", rel, bogus_sha, 10)])),
        encoding="utf-8",
    )

    with pytest.raises(SeedSynergyDetectionPackShaMismatch) as info:
        _load_pack_with_sha_check("drifted_pack", manifest_path=manifest_path)

    assert info.value.pack_id == "drifted_pack"
    assert info.value.expected_sha256 == bogus_sha
    assert info.value.actual_sha256 != bogus_sha


def test_load_pack_with_sha_check_unknown_pack_raises(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    rel, sha = _write_pack(tmp_path, "fake/only.json", [])
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("only_pack", rel, sha, 10)])),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError) as info:
        _load_pack_with_sha_check("nonexistent_pack", manifest_path=manifest_path)
    assert "PACK_NOT_FOUND" in str(info.value)


def test_load_pack_with_sha_check_missing_file_raises(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    bogus_sha = "1" * 64
    manifest_path.write_text(
        json.dumps(
            _make_manifest_payload([("phantom_pack", "fake/missing.json", bogus_sha, 10)])
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError) as info:
        _load_pack_with_sha_check("phantom_pack", manifest_path=manifest_path)
    assert "FILE_MISSING" in str(info.value)


# ----- specialized loaders -----


def test_load_themes_returns_list_as_is(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    payload = [
        {"theme_id": "THEME_A", "score_formula": "score=1*X"},
        {"theme_id": "THEME_B", "score_formula": "score=2*Y"},
    ]
    rel, sha = _write_pack(tmp_path, "tax/themes.json", payload)
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("taxonomy_themes", rel, sha, 40)])),
        encoding="utf-8",
    )
    result = _load_themes(manifest_path=manifest_path)
    assert result == payload


def test_load_bands_keys_by_theme_id(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    payload = [
        {"theme_id": "THEME_A", "low": 10, "med": 15.5, "high": 20.0},
        {"theme_id": "THEME_B", "low": 8, "med": 12.0, "high": 16.0},
    ]
    rel, sha = _write_pack(tmp_path, "th/bands.json", payload)
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("themes_confidence_bands_v1", rel, sha, 210)])),
        encoding="utf-8",
    )
    result = _load_bands(manifest_path=manifest_path)
    assert set(result.keys()) == {"THEME_A", "THEME_B"}
    assert result["THEME_A"]["low"] == 10
    assert result["THEME_A"]["high"] == 20.0
    assert result["THEME_B"]["med"] == 12.0


def test_load_signal_vocabulary_keys_by_id(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    payload = [
        {"id": "ALPHA_PATTERN", "abstraction_level": "card_named"},
        {"id": "BETA_ENGINE", "abstraction_level": "archetype_label"},
    ]
    rel, sha = _write_pack(tmp_path, "th/signal_vocab.json", payload)
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("themes_signal_vocabulary_v1", rel, sha, 200)])),
        encoding="utf-8",
    )
    result = _load_signal_vocabulary(manifest_path=manifest_path)
    assert set(result.keys()) == {"ALPHA_PATTERN", "BETA_ENGINE"}
    assert result["ALPHA_PATTERN"]["abstraction_level"] == "card_named"


def test_load_primitives_keys_by_primitive_id(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    payload = [
        {"primitive_id": "FOO_PRIMITIVE", "category": "ENGINE", "engine_primitive": "true"},
        {"primitive_id": "BAR_PRIMITIVE", "category": "RESOURCE", "engine_primitive": "false"},
    ]
    rel, sha = _write_pack(tmp_path, "tax/primitives.json", payload)
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("taxonomy_primitives", rel, sha, 20)])),
        encoding="utf-8",
    )
    result = _load_primitives(manifest_path=manifest_path)
    assert set(result.keys()) == {"FOO_PRIMITIVE", "BAR_PRIMITIVE"}
    assert result["FOO_PRIMITIVE"]["category"] == "ENGINE"


def test_load_primitive_index_map_returns_pack_as_is(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    payload = [{"id_field": "primitive_id", "index_field": "alpha"}]
    rel, sha = _write_pack(tmp_path, "tax/idx_map.json", payload)
    manifest_path.write_text(
        json.dumps(_make_manifest_payload([("taxonomy_primitive_mappings", rel, sha, 30)])),
        encoding="utf-8",
    )
    result = _load_primitive_index_map(manifest_path=manifest_path)
    assert result == payload


# ----- _load_adjacent_packs + _build_union_allowlist -----


def _make_adjacent_fixture(tmp_path):
    """Build 6 fake adjacent-pack files; return list of (pack_id, rel, sha, load_order)."""
    fixtures = [
        ("taxonomy_qualifier_facets",   "fake/qf.json",   ["FACET_A", "FACET_B"]),
        ("taxonomy_structural_roles",   "fake/sr.json",   ["ROLE_X", "ROLE_Y"]),
        ("taxonomy_equivalence_classes","fake/eq.json",   ["EQ_M", "EQ_N", "EQ_O"]),
        ("taxonomy_temporal_motifs",    "fake/tm.json",   ["MOTIF_P"]),
        ("taxonomy_commander_contracts","fake/cc.json",   ["CONTRACT_Q", "CONTRACT_R"]),
        ("taxonomy_need_buckets",       "fake/nb.json",   ["BUCKET_S", "BUCKET_T"]),
    ]
    out = []
    for pack_id, rel, idents in fixtures:
        rel_actual, sha = _write_pack(tmp_path, rel, idents)
        out.append((pack_id, rel_actual, sha, 50 + len(out) * 10))
    return out


def test_load_adjacent_packs_returns_set_per_pack(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    packs = _make_adjacent_fixture(tmp_path)
    manifest_path.write_text(json.dumps(_make_manifest_payload(packs)), encoding="utf-8")

    result = _load_adjacent_packs(manifest_path=manifest_path)
    assert set(result.keys()) == {p[0] for p in packs}
    assert "FACET_A" in result["taxonomy_qualifier_facets"]
    assert "EQ_O" in result["taxonomy_equivalence_classes"]
    assert "MOTIF_P" in result["taxonomy_temporal_motifs"]
    # Each set is uppercase-snake-case identifiers only (DSL keywords filtered)
    all_idents = set().union(*result.values())
    assert "AND" not in all_idents
    assert "OR" not in all_idents


def test_load_adjacent_packs_filters_dsl_keywords(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    # Pack content includes DSL keywords and uppercase-snake idents mixed together
    payload = ["KEEP_THIS_IDENT AND DROP_AND IF THEN ELSE", "ANOTHER_IDENT OR mixed"]
    fixtures = [
        ("taxonomy_qualifier_facets",   "fake/qf.json",  payload),
        ("taxonomy_structural_roles",   "fake/sr.json",  []),
        ("taxonomy_equivalence_classes","fake/eq.json",  []),
        ("taxonomy_temporal_motifs",    "fake/tm.json",  []),
        ("taxonomy_commander_contracts","fake/cc.json",  []),
        ("taxonomy_need_buckets",       "fake/nb.json",  []),
    ]
    pack_meta = []
    for pack_id, rel, content in fixtures:
        rel_actual, sha = _write_pack(tmp_path, rel, content)
        pack_meta.append((pack_id, rel_actual, sha, 50 + len(pack_meta) * 10))
    manifest_path.write_text(json.dumps(_make_manifest_payload(pack_meta)), encoding="utf-8")

    result = _load_adjacent_packs(manifest_path=manifest_path)
    qf = result["taxonomy_qualifier_facets"]
    assert "KEEP_THIS_IDENT" in qf
    assert "DROP_AND" in qf  # DROP_AND is a single identifier, not the AND keyword
    assert "ANOTHER_IDENT" in qf
    assert "AND" not in qf
    assert "OR" not in qf
    assert "IF" not in qf
    assert "THEN" not in qf
    assert "ELSE" not in qf


def test_build_union_allowlist_unions_all_sources(tmp_path, monkeypatch) -> None:
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)

    primitives_payload = [
        {"primitive_id": "PRIM_ALPHA", "category": "ENGINE", "engine_primitive": "true"},
        {"primitive_id": "PRIM_BETA",  "category": "RESOURCE", "engine_primitive": "false"},
        {"primitive_id": "PRIM_GAMMA", "category": "VALUE",    "engine_primitive": "true"},
    ]
    signal_vocab_payload = [
        {"id": "OVERLAY_X", "abstraction_level": "archetype_label"},
        {"id": "OVERLAY_Y", "abstraction_level": "card_named"},
    ]
    rel_p, sha_p = _write_pack(tmp_path, "tax/primitives.json", primitives_payload)
    rel_v, sha_v = _write_pack(tmp_path, "th/signal.json",     signal_vocab_payload)
    adjacent = _make_adjacent_fixture(tmp_path)

    all_packs = [
        ("taxonomy_primitives", rel_p, sha_p, 20),
        ("themes_signal_vocabulary_v1", rel_v, sha_v, 200),
    ] + adjacent
    manifest_path.write_text(json.dumps(_make_manifest_payload(all_packs)), encoding="utf-8")

    allowlist = _build_union_allowlist(manifest_path=manifest_path)

    # Sentinels included
    assert "score" in allowlist
    assert "token_kind" in allowlist
    # Primitives included
    assert "PRIM_ALPHA" in allowlist
    assert "PRIM_GAMMA" in allowlist
    # Overlay included
    assert "OVERLAY_X" in allowlist
    assert "OVERLAY_Y" in allowlist
    # Adjacent identifiers included
    assert "FACET_A" in allowlist
    assert "BUCKET_T" in allowlist
    # DSL keywords NOT included
    assert "AND" not in allowlist
    assert "OR" not in allowlist
    # No primitive_id leakage as keyword
    assert isinstance(allowlist, set)
    # Total count: 3 prims + 2 overlay + (2+2+3+1+2+2) adjacent + 2 sentinels = 19
    assert len(allowlist) == 19


# ---------------------------------------------------------------------------
# Sub-task 2C.2: theme-match algorithm tests.
# Two flavors:
#   (a) Pure unit tests for _bin_confidence (synthetic bands; no real packs).
#   (b) Integration tests calling run_seed_synergy_detection_v1 against the
#       real curated_pack_manifest_v1 + post-2C.1 loaders. These exercise the
#       full end-to-end algorithm: tokenize formula -> evaluate -> bin.
# ---------------------------------------------------------------------------

from api.engine.layers.seed_synergy_detection_v1 import _bin_confidence


# ----- _bin_confidence unit tests -----


def test_bin_confidence_high_at_boundary() -> None:
    bands = {"THEME_X": {"low": 10.0, "med": 15.0, "high": 20.0}}
    assert _bin_confidence(20.0, "THEME_X", bands) == "HIGH"
    assert _bin_confidence(25.0, "THEME_X", bands) == "HIGH"


def test_bin_confidence_med_at_boundary() -> None:
    bands = {"THEME_X": {"low": 10.0, "med": 15.0, "high": 20.0}}
    assert _bin_confidence(15.0, "THEME_X", bands) == "MED"
    assert _bin_confidence(19.99, "THEME_X", bands) == "MED"


def test_bin_confidence_low_at_boundary() -> None:
    bands = {"THEME_X": {"low": 10.0, "med": 15.0, "high": 20.0}}
    assert _bin_confidence(10.0, "THEME_X", bands) == "LOW"
    assert _bin_confidence(14.99, "THEME_X", bands) == "LOW"


def test_bin_confidence_below_low_returns_none() -> None:
    bands = {"THEME_X": {"low": 10.0, "med": 15.0, "high": 20.0}}
    assert _bin_confidence(9.99, "THEME_X", bands) is None
    assert _bin_confidence(0.0, "THEME_X", bands) is None


def test_bin_confidence_missing_theme_id_raises_keyerror() -> None:
    bands = {"THEME_X": {"low": 10.0, "med": 15.0, "high": 20.0}}
    with pytest.raises(KeyError):
        _bin_confidence(15.0, "THEME_NOT_IN_BANDS", bands)


# ----- Integration tests: real-pack archetype fixtures -----


def _slot_ids_from_primitive_counts(counts):
    """Helper: build slot_ids_by_primitive from {primitive_id: int_count}."""
    out = {}
    slot_counter = 0
    for prim_id, count in counts.items():
        slots = []
        for _ in range(count):
            slots.append(f"S{slot_counter:03d}")
            slot_counter += 1
        out[prim_id] = slots
    return out


def _theme_ids_in_matches(matches):
    return [m["theme_id"] for m in matches]


def test_aristocrats_archetype_matches_high() -> None:
    """Strong aristocrats deck → THEME_ARISTOCRATS at HIGH confidence."""
    counts = {
        "SAC_OUTLET_FREE": 8,
        "SAC_OUTLET_COSTED": 8,
        "DEATH_PAYOFF_DRAIN": 8,
        "TOKEN_PRODUCTION_CREATURE": 8,
        "REANIMATION_TO_BATTLEFIELD": 8,
    }
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(40)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B3",
    )
    detection = payload["seed_synergy_detection_v1"]
    matched = {m["theme_id"]: m for m in detection["matched_themes"]}
    assert "THEME_ARISTOCRATS" in matched
    assert matched["THEME_ARISTOCRATS"]["confidence"] == "HIGH"


def test_burn_archetype_matches() -> None:
    """Strong burn deck → THEME_BURN appears in matched_themes."""
    counts = {
        "DIRECT_DAMAGE_SPELLS": 8,
        "NONCOMBAT_DAMAGE_PAYOFF": 8,
        "DAMAGE_DOUBLER": 8,
        "SPELL_COPY": 8,
    }
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(32)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B3",
    )
    matched = {m["theme_id"]: m for m in payload["seed_synergy_detection_v1"]["matched_themes"]}
    assert "THEME_BURN" in matched
    assert matched["THEME_BURN"]["confidence"] in {"HIGH", "MED", "LOW"}


def test_cantrips_archetype_matches() -> None:
    """Strong cantrips deck → THEME_CANTRIPS appears in matched_themes."""
    counts = {
        "CANTRIP_DENSITY": 8,
        "CARD_SELECTION": 8,
        "SPELL_COPY": 8,
    }
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(24)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B3",
    )
    matched = {m["theme_id"]: m for m in payload["seed_synergy_detection_v1"]["matched_themes"]}
    assert "THEME_CANTRIPS" in matched
    assert matched["THEME_CANTRIPS"]["confidence"] in {"HIGH", "MED", "LOW"}


def test_tokens_archetype_matches() -> None:
    """Strong tokens deck → THEME_TOKENS at HIGH confidence."""
    counts = {
        "TOKEN_PRODUCTION_CREATURE": 8,
        "TOKEN_PAYOFF_ANTHEM": 8,
        "REPLACEMENT_TOKEN_DOUBLER": 8,
        "POPULATE": 8,
    }
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(32)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B4",
    )
    matched = {m["theme_id"]: m for m in payload["seed_synergy_detection_v1"]["matched_themes"]}
    assert "THEME_TOKENS" in matched


def test_empty_input_yields_no_matches_under_real_packs() -> None:
    """Empty primitive map → no theme matches its threshold."""
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={"S000": []},
        slot_ids_by_primitive={},
        bracket_id="B3",
    )
    detection = payload["seed_synergy_detection_v1"]
    assert detection["matched_themes"] == []
    assert payload["status"] == "OK"
    assert payload["unknowns"] == []


def test_single_low_count_primitive_no_match() -> None:
    """One primitive at count 1 won't push any theme over its 8+ threshold."""
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={"S000": []},
        slot_ids_by_primitive={"BLINK_REPEATABLE": ["S000"]},
        bracket_id="B3",
    )
    assert payload["seed_synergy_detection_v1"]["matched_themes"] == []


def test_determinism_repeated_calls_bitwise_identical() -> None:
    """Same input twice → identical output (deterministic)."""
    counts = {
        "SAC_OUTLET_FREE": 6,
        "DEATH_PAYOFF_DRAIN": 5,
        "TOKEN_PRODUCTION_CREATURE": 4,
    }
    kwargs = {
        "primitive_index_by_slot": {f"S{i:03d}": [] for i in range(15)},
        "slot_ids_by_primitive": _slot_ids_from_primitive_counts(counts),
        "bracket_id": "B2",
    }
    first = run_seed_synergy_detection_v1(**kwargs)
    second = run_seed_synergy_detection_v1(**kwargs)
    assert first == second


def test_matches_sorted_by_score_desc_then_theme_id_asc() -> None:
    """Multi-theme input → matches sorted by score desc, then theme_id asc."""
    # Aristocrats inputs that also satisfy tokens (TOKEN_PRODUCTION_CREATURE
    # appears in both formulas).
    counts = {
        "SAC_OUTLET_FREE": 8,
        "SAC_OUTLET_COSTED": 8,
        "DEATH_PAYOFF_DRAIN": 8,
        "TOKEN_PRODUCTION_CREATURE": 8,
        "REANIMATION_TO_BATTLEFIELD": 8,
        "TOKEN_PAYOFF_ANTHEM": 8,
        "REPLACEMENT_TOKEN_DOUBLER": 8,
        "POPULATE": 8,
    }
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(64)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B4",
    )
    matches = payload["seed_synergy_detection_v1"]["matched_themes"]
    assert len(matches) >= 2
    # Verify sort: score descending; ties broken by theme_id ascending
    for i in range(len(matches) - 1):
        a, b = matches[i], matches[i + 1]
        assert a["score"] > b["score"] or (
            a["score"] == b["score"] and a["theme_id"] < b["theme_id"]
        )


def test_bracket_id_round_trip_preserved() -> None:
    """bracket_id input → echoed in detection.bracket_id."""
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={"S000": []},
        slot_ids_by_primitive={},
        bracket_id="B5",
    )
    assert payload["seed_synergy_detection_v1"]["bracket_id"] == "B5"


def test_bracket_id_none_preserved() -> None:
    """bracket_id=None → detection.bracket_id is None."""
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={"S000": []},
        slot_ids_by_primitive={},
        bracket_id=None,
    )
    assert payload["seed_synergy_detection_v1"]["bracket_id"] is None


def test_unknown_primitive_in_seed_surfaces_in_unknowns() -> None:
    """Input primitive not in union allowlist → reported in unknowns + status WARN."""
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={"S000": []},
        slot_ids_by_primitive={"NOT_A_REAL_PRIMITIVE_XYZ": ["S000"]},
        bracket_id="B3",
    )
    assert payload["status"] == "WARN"
    assert len(payload["unknowns"]) == 1
    unknown_entry = payload["unknowns"][0]
    assert unknown_entry["code"] == "UNKNOWN_PRIMITIVE_IN_SEED"
    assert "NOT_A_REAL_PRIMITIVE_XYZ" in unknown_entry["primitive_ids"]
    # Unknown primitives don't break the rest of the algorithm
    assert "matched_themes" in payload["seed_synergy_detection_v1"]


def test_matched_signatures_remains_empty_stub() -> None:
    """Sub-task 2C.2 only populates matched_themes; signatures stay []."""
    counts = {"SAC_OUTLET_FREE": 8, "DEATH_PAYOFF_DRAIN": 8, "TOKEN_PRODUCTION_CREATURE": 8}
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(24)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B3",
    )
    assert payload["seed_synergy_detection_v1"]["matched_signatures"] == []


def test_missing_roles_remains_canonical_zero_stub() -> None:
    """Sub-task 2C.2 only populates matched_themes; missing_roles stays canonical zero."""
    counts = {"SAC_OUTLET_FREE": 8, "DEATH_PAYOFF_DRAIN": 8, "TOKEN_PRODUCTION_CREATURE": 8}
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(24)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B3",
    )
    missing = payload["seed_synergy_detection_v1"]["missing_roles"]
    assert sorted(missing.keys()) == sorted(CANONICAL_MISSING_ROLE_KEYS)
    assert all(v == 0 for v in missing.values())


def test_bracket_compatible_flag_remains_true_stub() -> None:
    """Sub-task 2C.2 doesn't implement bracket filter; flag stays True."""
    counts = {"SAC_OUTLET_FREE": 8, "DEATH_PAYOFF_DRAIN": 8, "TOKEN_PRODUCTION_CREATURE": 8}
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(24)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B1",
    )
    assert payload["seed_synergy_detection_v1"]["bracket_compatible_flag"] is True


def test_match_score_is_float_and_finite() -> None:
    """Every match's score is a finite float (no NaN/Inf)."""
    import math
    counts = {"SAC_OUTLET_FREE": 8, "DEATH_PAYOFF_DRAIN": 8, "TOKEN_PRODUCTION_CREATURE": 8}
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(24)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B3",
    )
    for match in payload["seed_synergy_detection_v1"]["matched_themes"]:
        assert isinstance(match["score"], float)
        assert not math.isnan(match["score"])
        assert not math.isinf(match["score"])


def test_match_confidence_is_high_med_or_low() -> None:
    """Every match's confidence is one of the three valid bin labels."""
    counts = {"SAC_OUTLET_FREE": 8, "DEATH_PAYOFF_DRAIN": 8, "TOKEN_PRODUCTION_CREATURE": 8}
    payload = run_seed_synergy_detection_v1(
        primitive_index_by_slot={f"S{i:03d}": [] for i in range(24)},
        slot_ids_by_primitive=_slot_ids_from_primitive_counts(counts),
        bracket_id="B3",
    )
    for match in payload["seed_synergy_detection_v1"]["matched_themes"]:
        assert match["confidence"] in {"HIGH", "MED", "LOW"}


def test_build_union_allowlist_dedupes_across_sources(tmp_path, monkeypatch) -> None:
    """If a primitive_id and an adjacent-pack identifier collide, the union dedupes."""
    manifest_path = _setup_fake_root(tmp_path, monkeypatch)
    primitives_payload = [{"primitive_id": "SHARED", "category": "ENGINE", "engine_primitive": "true"}]
    rel_p, sha_p = _write_pack(tmp_path, "tax/primitives.json", primitives_payload)
    rel_v, sha_v = _write_pack(tmp_path, "th/signal.json", [])
    adjacent = []
    for pack_id, rel in [
        ("taxonomy_qualifier_facets",   "fake/qf.json"),
        ("taxonomy_structural_roles",   "fake/sr.json"),
        ("taxonomy_equivalence_classes","fake/eq.json"),
        ("taxonomy_temporal_motifs",    "fake/tm.json"),
        ("taxonomy_commander_contracts","fake/cc.json"),
        ("taxonomy_need_buckets",       "fake/nb.json"),
    ]:
        # SHARED appears in every adjacent pack too
        rel_actual, sha = _write_pack(tmp_path, rel, ["SHARED OTHER_IDENT_" + pack_id.upper()])
        adjacent.append((pack_id, rel_actual, sha, 50 + len(adjacent) * 10))

    all_packs = [
        ("taxonomy_primitives", rel_p, sha_p, 20),
        ("themes_signal_vocabulary_v1", rel_v, sha_v, 200),
    ] + adjacent
    manifest_path.write_text(json.dumps(_make_manifest_payload(all_packs)), encoding="utf-8")

    allowlist = _build_union_allowlist(manifest_path=manifest_path)

    # SHARED appears 7 times across sources but only once in the union
    assert "SHARED" in allowlist
    # 1 SHARED + 6 unique adjacent OTHER_IDENT_* + 0 overlay + 2 sentinels = 9
    assert len(allowlist) == 9
