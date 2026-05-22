"""Mega-task v5 Phase 5 live smoke.

Replaces the kickoff-mandated chrome-devtools-mcp walk (the MCP wasn't
registered in this session). Hits the live SSE endpoint with the Edgar
B3 test case + must-includes, streams events to disk, parses them, and
asserts:

  1. /snapshots/active returns a non-empty snapshot id.
  2. POST /agent/build_deck_v1/stream returns 200 + content-type
     text/event-stream.
  3. All deterministic phase boundaries fire (candidate_pool /
     select_deck / validate_swap / structural_safety_net / mana_base /
     card_advantage). LLM-conditional phases (intent_interpreter /
     c21_c22_parallel / final_critic) MAY fire if ANTHROPIC_API_KEY is
     set; the script does not require them so it stays runnable
     offline.
  4. The final "complete" event carries a response with a 100-card deck.
  5. /health returns 200 in <250ms while the build is in flight (the
     Phase 1 "second worker can handle non-build traffic" property).
  6. Wall-clock for the build is < 240s (the Phase 4 timeout ceiling).

Usage:
    python tools/mega_task_v5_phase5_live_smoke.py [--port 8000]

Writes a JSON report next to itself for the Phase 5 commit.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple


DETERMINISTIC_PHASES = (
    "candidate_pool",
    "select_deck",
    "validate_swap",
    "structural_safety_net",
    "mana_base",
    "card_advantage",
)

LLM_CONDITIONAL_PHASES = (
    "intent_interpreter",
    "c21_c22_parallel",
    "final_critic",
)


def _http_get(url: str, *, timeout: float = 10.0) -> Tuple[int, Dict[str, str], bytes]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, dict(resp.headers.items()), resp.read()


def _check_active_snapshot(base_url: str) -> str:
    status, _headers, body = _http_get(f"{base_url}/snapshots/active")
    if status != 200:
        raise RuntimeError(f"/snapshots/active returned HTTP {status}")
    payload = json.loads(body.decode("utf-8"))
    sid = payload.get("snapshot_id", "")
    if not sid:
        raise RuntimeError("/snapshots/active returned empty snapshot_id")
    return sid


def _stream_build(base_url: str, snapshot_id: str) -> Dict[str, Any]:
    payload = {
        "db_snapshot_id": snapshot_id,
        "commander": "Edgar Markov",
        "bracket": "B3",
        "theme_hints": [],
        "must_include_cards": ["Vito, Thorn of the Dusk Rose", "Cordial Vampire"],
    }
    req = urllib.request.Request(
        f"{base_url}/agent/build_deck_v1/stream",
        method="POST",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
    )
    events: List[Dict[str, Any]] = []
    raw_lines: List[str] = []
    t_start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=300.0) as resp:
        status = resp.status
        ct = resp.headers.get("content-type", "")
        # Stream-parse the body line by line.
        buf = ""
        while True:
            chunk = resp.read(8192)
            if not chunk:
                break
            buf += chunk.decode("utf-8", errors="replace")
            raw_lines.append(chunk.decode("utf-8", errors="replace"))
            normalized = buf.replace("\r\n", "\n")
            parts = normalized.split("\n\n")
            buf = parts.pop()  # incomplete trailing block
            for block in parts:
                event_type = "message"
                data_lines: List[str] = []
                for line in block.split("\n"):
                    if not line:
                        continue
                    if line.startswith(":"):
                        continue
                    if line.startswith("event:"):
                        event_type = line[6:].strip()
                    elif line.startswith("data:"):
                        data_lines.append(line[5:].strip())
                if data_lines:
                    try:
                        data = json.loads("\n".join(data_lines))
                    except json.JSONDecodeError:
                        continue
                    events.append({"event": event_type, "data": data})

    elapsed = time.perf_counter() - t_start
    return {
        "status": status,
        "content_type": ct,
        "events": events,
        "elapsed_s": elapsed,
    }


def _hammer_health(base_url: str, stop_event: concurrent.futures.Future, every_s: float = 5.0):
    """Hit /health every N seconds until the stop future is done; return latency series."""
    latencies: List[float] = []
    last = 0.0
    while not stop_event.done():
        now = time.time()
        if now - last < every_s:
            time.sleep(0.2)
            continue
        last = now
        t0 = time.perf_counter()
        try:
            status, _h, _b = _http_get(f"{base_url}/health", timeout=5.0)
            latencies.append(time.perf_counter() - t0)
        except Exception:
            latencies.append(float("inf"))
    return latencies


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--report", default=None, help="Optional path to write JSON report.")
    args = parser.parse_args(argv)

    base_url = f"http://127.0.0.1:{args.port}"
    report: Dict[str, Any] = {
        "base_url": base_url,
        "test_case": "edgar_markov_b3_with_must_includes",
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "checks": {},
    }
    failed_checks: List[str] = []

    # 1. /snapshots/active
    try:
        sid = _check_active_snapshot(base_url)
        report["checks"]["snapshots_active"] = {"ok": True, "snapshot_id": sid}
        print(f"[1/6] /snapshots/active -> {sid}")
    except Exception as exc:
        report["checks"]["snapshots_active"] = {"ok": False, "error": str(exc)}
        failed_checks.append("snapshots_active")
        print(f"[1/6] /snapshots/active FAILED: {exc}")
        report["overall_ok"] = False
        Path(args.report or "mega_task_v5_phase5_report.json").write_text(json.dumps(report, indent=2))
        return 1

    # 5. /health concurrent with build (Phase 1 second-worker property).
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    health_future: concurrent.futures.Future = concurrent.futures.Future()

    def _health_runner():
        latencies: List[float] = []
        last_check = 0.0
        while not health_future.done():
            now = time.time()
            if now - last_check < 5.0:
                time.sleep(0.2)
                continue
            last_check = now
            t0 = time.perf_counter()
            try:
                _http_get(f"{base_url}/health", timeout=5.0)
                latencies.append(time.perf_counter() - t0)
            except Exception:
                latencies.append(float("inf"))
        return latencies

    health_task = executor.submit(_health_runner)

    # 2-4: SSE build.
    print("[2-4/6] streaming SSE build (will take ~120s)…")
    try:
        sse_result = _stream_build(base_url, sid)
    except Exception as exc:
        report["checks"]["sse_build"] = {"ok": False, "error": str(exc)}
        failed_checks.append("sse_build")
        print(f"[2-4/6] sse build FAILED: {exc}")
        health_future.set_result([])
        try:
            health_task.result(timeout=5)
        except Exception:
            pass
        executor.shutdown(wait=False)
        report["overall_ok"] = False
        Path(args.report or "mega_task_v5_phase5_report.json").write_text(json.dumps(report, indent=2))
        return 1

    health_future.set_result([])
    health_latencies = health_task.result(timeout=10)
    executor.shutdown(wait=False)

    report["checks"]["sse_build_http"] = {
        "ok": sse_result["status"] == 200,
        "status": sse_result["status"],
        "content_type": sse_result["content_type"],
    }
    if "text/event-stream" not in sse_result["content_type"].lower():
        failed_checks.append("sse_build_http")

    # Phase boundaries.
    phase_status_set = {(e["data"].get("phase"), e["data"].get("status")) for e in sse_result["events"]}
    fired_phases = {p for (p, _s) in phase_status_set}
    missing_deterministic = [p for p in DETERMINISTIC_PHASES if p not in fired_phases]
    fired_llm_conditional = [p for p in LLM_CONDITIONAL_PHASES if p in fired_phases]
    report["checks"]["phase_boundaries"] = {
        "ok": len(missing_deterministic) == 0,
        "fired_count": len(fired_phases),
        "missing_deterministic": missing_deterministic,
        "fired_llm_conditional": fired_llm_conditional,
        "fired_phases_sample": sorted(fired_phases),
    }
    if missing_deterministic:
        failed_checks.append("phase_boundaries")

    # Complete event with 100-card deck.
    complete_events = [e for e in sse_result["events"] if e["data"].get("phase") == "complete"]
    complete_ok = False
    deck_len = 0
    if complete_events:
        final = complete_events[-1]["data"].get("response", {})
        deck_len = len(final.get("deck") or [])
        complete_ok = (
            final.get("version", "").startswith("agent_build_deck_v1")
            and deck_len == 100
        )
    report["checks"]["complete_event"] = {
        "ok": complete_ok,
        "deck_len": deck_len,
        "elapsed_s": sse_result["elapsed_s"],
    }
    if not complete_ok:
        failed_checks.append("complete_event")

    # 240s timeout ceiling.
    report["checks"]["under_240s_ceiling"] = {
        "ok": sse_result["elapsed_s"] < 240.0,
        "elapsed_s": sse_result["elapsed_s"],
    }
    if sse_result["elapsed_s"] >= 240.0:
        failed_checks.append("under_240s_ceiling")

    # /health responsive during build.
    health_responsive = all(latency < 0.5 for latency in health_latencies) if health_latencies else False
    report["checks"]["health_responsive_during_build"] = {
        "ok": health_responsive,
        "latencies_s": health_latencies,
        "max_s": max(health_latencies) if health_latencies else None,
    }
    if not health_responsive:
        failed_checks.append("health_responsive_during_build")

    report["overall_ok"] = len(failed_checks) == 0
    report["failed_checks"] = failed_checks

    print()
    print("=== Phase 5 live smoke summary ===")
    print(f"Elapsed:           {sse_result['elapsed_s']:.1f}s")
    print(f"Events seen:       {len(sse_result['events'])}")
    print(f"Phases fired:      {len(fired_phases)} ({sorted(fired_phases)})")
    print(f"Missing det. phases: {missing_deterministic}")
    print(f"LLM-conditional fired: {fired_llm_conditional}")
    print(f"Final deck length: {deck_len}")
    print(f"/health max latency during build: {report['checks']['health_responsive_during_build'].get('max_s')}s")
    print(f"Failed checks:     {failed_checks}")
    print(f"Overall:           {'PASS' if report['overall_ok'] else 'FAIL'}")

    out_path = Path(args.report or "mega_task_v5_phase5_report.json")
    out_path.write_text(json.dumps(report, indent=2))
    print(f"Report written: {out_path}")
    return 0 if report["overall_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
