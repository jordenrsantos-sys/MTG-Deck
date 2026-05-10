"""Engine performance instrumentation (Phase 6 Stage 6).

Module marker. The TimingProbe + time_layer helpers are runtime-side
because pipeline_build.py imports them; the test_runtime_import_isolation
boundary forbids `tools/` imports from runtime modules. The CLI runner
that consumes this module lives at `tools/perf/run_perf_timing_audit.py`.
"""
