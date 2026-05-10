"""Pytest for tools/perf/timing_instrumentation_v1 — Phase 6 Stage 6 Stage 1.

Verifies the TimingProbe context manager:
- Default (no probe) → time_layer is a pass-through (no entries collected).
- With probe → entries captured in deterministic insertion order.
- Concurrent / nested probes don't pollute each other (ContextVar isolation).
- elapsed_ms is positive + monotonic + correlates with sleep duration.
- total_elapsed_ms == sum of entries.
- layer_ids preserves insertion order.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from api.engine.perf.timing_instrumentation_v1 import (  # noqa: E402
    TIMING_INSTRUMENTATION_V1_VERSION,
    TimingEntry,
    TimingProbe,
    is_probe_active,
    time_layer,
)


def test_module_exports_version_constant():
    assert TIMING_INSTRUMENTATION_V1_VERSION == "timing_instrumentation_v1"


def test_no_probe_active_by_default():
    assert is_probe_active() is False


def test_time_layer_is_passthrough_when_no_probe_active():
    # Should not raise + should not collect anywhere.
    with time_layer("nowhere_layer"):
        pass
    assert is_probe_active() is False


def test_probe_collects_entries_in_insertion_order():
    probe = TimingProbe()
    with probe:
        with time_layer("first_layer"):
            pass
        with time_layer("second_layer"):
            pass
        with time_layer("third_layer"):
            pass
    assert probe.layer_ids == ["first_layer", "second_layer", "third_layer"]
    assert len(probe.entries) == 3
    assert all(isinstance(e, TimingEntry) for e in probe.entries)


def test_probe_deactivates_on_exit():
    probe = TimingProbe()
    assert is_probe_active() is False
    with probe:
        assert is_probe_active() is True
    assert is_probe_active() is False


def test_elapsed_ms_is_positive_and_correlates_with_sleep():
    probe = TimingProbe()
    with probe:
        with time_layer("sleeper"):
            time.sleep(0.01)  # 10ms
    assert len(probe.entries) == 1
    elapsed = probe.entries[0].elapsed_ms
    assert elapsed > 0
    # Sleep is at least 10ms; allow generous slack for OS jitter on Windows.
    assert elapsed >= 5.0
    assert elapsed < 500.0  # sanity upper bound


def test_total_elapsed_ms_equals_sum_of_parts():
    probe = TimingProbe()
    with probe:
        for layer_id in ("a", "b", "c"):
            with time_layer(layer_id):
                pass
    expected_total = sum(e.elapsed_ms for e in probe.entries)
    assert abs(probe.total_elapsed_ms - expected_total) < 1e-9


def test_started_at_ns_is_monotonic_across_entries():
    probe = TimingProbe()
    with probe:
        for layer_id in ("first", "second", "third"):
            with time_layer(layer_id):
                time.sleep(0.001)
    starts = [e.started_at_ns for e in probe.entries]
    assert starts == sorted(starts), "started_at_ns must be monotonic"


def test_nested_probe_isolates_inner_from_outer():
    """ContextVar isolates probe state — inner probe captures only its own entries."""
    outer = TimingProbe()
    inner = TimingProbe()
    with outer:
        with time_layer("outer_only"):
            pass
        with inner:
            with time_layer("inner_only"):
                pass
        with time_layer("outer_after_inner"):
            pass
    # Outer captures the two outer layers; inner captures just its own.
    assert outer.layer_ids == ["outer_only", "outer_after_inner"]
    assert inner.layer_ids == ["inner_only"]


def test_probe_entries_default_empty_list():
    probe = TimingProbe()
    assert probe.entries == []
    assert probe.layer_ids == []
    assert probe.total_elapsed_ms == 0.0


def test_layer_id_with_unusual_characters_preserved_verbatim():
    """Layer ids are opaque strings — no normalization."""
    probe = TimingProbe()
    weird = "layer.with-dashes_and.dots/v1"
    with probe:
        with time_layer(weird):
            pass
    assert probe.entries[0].layer_id == weird


def test_exception_inside_time_layer_still_records_entry():
    """Even if the wrapped block raises, the timing entry is captured."""
    probe = TimingProbe()
    with probe:
        with pytest.raises(RuntimeError):
            with time_layer("raises_inside"):
                raise RuntimeError("boom")
    assert probe.layer_ids == ["raises_inside"]


def test_repeated_layer_id_records_each_invocation_separately():
    """If the same layer_id is timed twice, both entries are captured."""
    probe = TimingProbe()
    with probe:
        with time_layer("repeated"):
            pass
        with time_layer("other"):
            pass
        with time_layer("repeated"):
            pass
    assert probe.layer_ids == ["repeated", "other", "repeated"]
    assert len(probe.entries) == 3
