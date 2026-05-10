"""TimingProbe — opt-in per-layer wall-clock instrumentation (Phase 6 Stage 6).

Pure helper: a context-managed probe that captures `(layer_id, elapsed_ms)`
tuples in deterministic insertion order. Activation flows through a
`contextvars.ContextVar` so concurrent /build calls in different async
tasks (and in different threads, since ContextVar respects PEP 567 task
locality + thread locality) don't see each other's timing data.

Per HARD safety #5 (`/build` reproducibility): the probe is OPT-IN —
when no probe is active, `time_layer()` is a near-zero overhead pass-
through (one ContextVar.get + one None check). Production /build calls
see no overhead. The perf-audit CLI explicitly enables a probe; tests
can also use it.

Per HARD safety #10 (no non-determinism): timing values are side-channel
ONLY — they go to the probe's `entries` list, never to the BuildResponse.
The same `(BuildRequest, snapshot_id)` returns byte-identical JSON
whether the probe is active or not.
"""
from __future__ import annotations

import contextvars
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, List

# Public version constant — matches the file name + manifest convention.
TIMING_INSTRUMENTATION_V1_VERSION = "timing_instrumentation_v1"

# ContextVar isolates probe state per-task / per-thread per PEP 567.
# Default None → no probe active → time_layer is a pass-through.
_active_probe: contextvars.ContextVar["TimingProbe | None"] = contextvars.ContextVar(
    "timing_probe_active", default=None
)


@dataclass
class TimingEntry:
    """One measurement: layer_id + elapsed wall-clock in milliseconds."""

    layer_id: str
    elapsed_ms: float
    started_at_ns: int


@dataclass
class TimingProbe:
    """Context-managed probe that captures per-layer timing.

    Usage::

        probe = TimingProbe()
        with probe:
            response = run_build_pipeline(...)
        for entry in probe.entries:
            print(entry.layer_id, entry.elapsed_ms)
    """

    entries: List[TimingEntry] = field(default_factory=list)
    _token: object = field(default=None, init=False, repr=False)

    def __enter__(self) -> "TimingProbe":
        self._token = _active_probe.set(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._token is not None:
            _active_probe.reset(self._token)
            self._token = None

    @property
    def total_elapsed_ms(self) -> float:
        """Sum of every captured entry. Useful for sum-of-parts tests."""
        return sum(e.elapsed_ms for e in self.entries)

    @property
    def layer_ids(self) -> List[str]:
        """Captured layer ids in insertion order — deterministic."""
        return [e.layer_id for e in self.entries]


@contextmanager
def time_layer(layer_id: str) -> Iterator[None]:
    """Time the wrapped block under the active TimingProbe.

    When no probe is active (the default — production path), this is a
    near-zero-overhead pass-through: one ContextVar.get + one None check.
    When a probe is active, the wrapped block's wall-clock duration is
    appended to the probe's entries list.

    `layer_id` must be the canonical layer id string (e.g.,
    `"seed_synergy_detection_v1"`); deterministic key ordering depends on
    the caller using the same string for the same layer across runs.
    """
    probe = _active_probe.get()
    if probe is None:
        # Production path — zero instrumentation overhead.
        yield
        return
    started_at_ns = time.perf_counter_ns()
    try:
        yield
    finally:
        elapsed_ns = time.perf_counter_ns() - started_at_ns
        probe.entries.append(
            TimingEntry(
                layer_id=layer_id,
                elapsed_ms=elapsed_ns / 1_000_000.0,
                started_at_ns=started_at_ns,
            )
        )


def is_probe_active() -> bool:
    """True iff a TimingProbe is currently active in this context."""
    return _active_probe.get() is not None
