"""Mega-task v6 Phase 1: Browser-equivalent SSE consumer.

Simulates exactly what the browser's fetch + ReadableStream + UI SSE parser
would do, including:

  - cross-origin POST with Origin: http://localhost:5173
  - Content-Type: application/json + Accept: text/event-stream
  - manual stream chunk reading (mirrors `response.body.getReader().read()`)
  - SSE parser equivalent to useBuildStreaming.ts::_parseSseBuffer
    (strict \n\n splitting after \r\n normalization; skip `:` comments;
     join multiple `data:` lines)
  - tracks first-event latency + complete-event presence + final deck size

If this client consumes the stream cleanly with non-empty deck in the
complete event, the server side is fully working over the network for any
cross-origin client. Any failure here is a real server bug. Any failure in
the browser AFTER this passes is a UI-layer bug.

Usage:
    python tools/mega_task_v6_phase1_browser_simulation.py --port 8000
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from typing import Any, Dict, List, Tuple

try:
    import httpx
except ImportError:
    sys.stderr.write("httpx is required: pip install httpx\n")
    sys.exit(1)


def _parse_sse_buffer(buffer: str) -> Tuple[List[Dict[str, Any]], str]:
    """Bit-for-bit equivalent of useBuildStreaming.ts::_parseSseBuffer."""
    events: List[Dict[str, Any]] = []
    normalized = buffer.replace("\r\n", "\n")
    parts = normalized.split("\n\n")
    remaining = parts.pop() if parts else ""
    for block in parts:
        event_type = "message"
        data_lines: List[str] = []
        for line in block.split("\n"):
            if not line:
                continue
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event_type = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data_lines.append(line[len("data:"):].strip())
        if data_lines:
            try:
                data = json.loads("\n".join(data_lines))
            except json.JSONDecodeError:
                continue
            events.append({"event": event_type, "data": data})
    return events, remaining


async def consume_stream(base_url: str, snapshot_id: str) -> Dict[str, Any]:
    payload = {
        "db_snapshot_id": snapshot_id,
        "commander": "Edgar Markov",
        "bracket": "B3",
        "theme_hints": [],
        "must_include_cards": [],
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "Origin": "http://localhost:5173",
    }
    events: List[Dict[str, Any]] = []
    first_event_latency: float | None = None
    t_start = time.perf_counter()

    async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0)) as client:
        async with client.stream(
            "POST",
            f"{base_url}/agent/build_deck_v1/stream",
            json=payload,
            headers=headers,
        ) as response:
            status = response.status_code
            content_type = response.headers.get("content-type", "")
            acao = response.headers.get("access-control-allow-origin", "")
            buffer = ""
            async for chunk in response.aiter_text():
                if first_event_latency is None and chunk.strip():
                    first_event_latency = time.perf_counter() - t_start
                buffer += chunk
                new_events, buffer = _parse_sse_buffer(buffer)
                events.extend(new_events)

    elapsed = time.perf_counter() - t_start
    return {
        "status": status,
        "content_type": content_type,
        "acao": acao,
        "first_event_latency_s": first_event_latency,
        "elapsed_s": elapsed,
        "event_count": len(events),
        "phases_seen": sorted({e["data"].get("phase") for e in events if e["data"].get("phase")}),
        "complete_event": next(
            (e for e in events if e["data"].get("phase") == "complete"),
            None,
        ),
    }


async def _amain() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    base_url = f"http://127.0.0.1:{args.port}"

    # 1. snapshot
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{base_url}/snapshots/active")
        snapshot_id = r.json().get("snapshot_id", "")
    if not snapshot_id:
        print("FAILED to get snapshot")
        return 1
    print(f"snapshot: {snapshot_id}")

    # 2. consume stream
    result = await consume_stream(base_url, snapshot_id)
    print(json.dumps({k: (v if k != "complete_event" else
                          {"present": v is not None,
                           "deck_len": (len((v or {}).get("data", {}).get("response", {}).get("deck", []))
                                        if v else 0)})
                      for k, v in result.items()},
                     indent=2))
    complete = result.get("complete_event")
    deck_len = (
        len(complete["data"].get("response", {}).get("deck", []))
        if complete else 0
    )
    ok = (
        result["status"] == 200
        and "text/event-stream" in result["content_type"]
        and result["acao"] == "http://localhost:5173"
        and result["event_count"] >= 6
        and complete is not None
        and deck_len == 100
    )
    print("OK" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_amain()))
