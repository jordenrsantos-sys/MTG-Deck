"""
run_mpa_calibration — Phase 5b.3. CLI for the MPA calibration suite.

Loads the calibration matrix (api/engine/data/playtest/calibration_matrix_v1.json),
resolves each matchup's deck pair via the opposition_decks_v1 registry, runs
N anti-bias-mirrored games per matchup via /playtest/benchmark_v1, and emits
a markdown report flagging any matchup whose actual win-rate falls outside
the expected band ("calibration drift").

Why a CLI vs running in-process: the endpoint is the canonical entry point
that already handles corpus loading, hydration, and calibration log writes.
The CLI calls it via HTTP so the runner and the prod path stay in lockstep
— no risk of the CLI diverging from what production decisions consult.

Usage:
    python tools/run_mpa_calibration.py \\
        --snapshot 20260217_190902 \\
        --api-base http://localhost:8000 \\
        --out-dir api/engine/data/playtest

Halt behavior: writes calibration_report_<utc_ts>.md, prints the path to
stdout, exits 0 even on drift (drift is signal, not a runtime failure).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX_PATH = REPO_ROOT / "api" / "engine" / "data" / "playtest" / "calibration_matrix_v1.json"
DEFAULT_OUT_DIR = REPO_ROOT / "api" / "engine" / "data" / "playtest"


def _http_post_json(url: str, body: Dict[str, Any], timeout: int = 1800) -> Dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _load_matrix(matrix_path: Path) -> Dict[str, Any]:
    with open(matrix_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_role_tag_to_corpus_id(role_tag: str) -> Optional[str]:
    sys.path.insert(0, str(REPO_ROOT))
    from api.engine.playtest.opposition_decks_v1 import get_by_role_tag
    entry = get_by_role_tag(role_tag)
    return entry["corpus_id"] if entry else None


def _compute_seat_stats(per_game_log: List[Dict[str, Any]]) -> Dict[str, int]:
    """Derive seat-win counts from the per-game log.

    Encoding:
      swap=False → deck_a at seat 0, deck_b at seat 1
      swap=True  → deck_b at seat 0, deck_a at seat 1
    """
    seat0_wins = 0
    seat1_wins = 0
    draws = 0
    for g in per_game_log:
        outcome = g.get("outcome")
        swap = g.get("swap", False)
        if outcome == "draw":
            draws += 1
            continue
        if outcome == "A_wins":
            if not swap:
                seat0_wins += 1
            else:
                seat1_wins += 1
        elif outcome == "B_wins":
            if not swap:
                seat1_wins += 1
            else:
                seat0_wins += 1
    return {"seat0_wins": seat0_wins, "seat1_wins": seat1_wins, "draws": draws}


def _run_matchup(
    *,
    api_base: str,
    snapshot: str,
    matchup: Dict[str, Any],
    n_game_pairs: int,
    max_turns_per_game: int,
) -> Dict[str, Any]:
    """Execute a single matchup; returns a dict with raw + computed fields."""
    deck_a_id = _resolve_role_tag_to_corpus_id(matchup["deck_a_role_tag"])
    deck_b_id = _resolve_role_tag_to_corpus_id(matchup["deck_b_role_tag"])
    matchup_name = matchup["matchup_name"]
    if not deck_a_id or not deck_b_id:
        return {
            "matchup_name": matchup_name,
            "error": "ROLE_TAG_UNRESOLVED",
            "deck_a_role_tag": matchup["deck_a_role_tag"],
            "deck_b_role_tag": matchup["deck_b_role_tag"],
            "deck_a_corpus_id": deck_a_id,
            "deck_b_corpus_id": deck_b_id,
        }

    body = {
        "db_snapshot_id": snapshot,
        "deck_a_corpus_id": deck_a_id,
        "deck_b_corpus_id": deck_b_id,
        "benchmark_name": matchup_name,
        "n_game_pairs": n_game_pairs,
        "max_turns_per_game": max_turns_per_game,
        "expected_winrate_min": matchup.get("expected_winrate_min"),
        "expected_winrate_max": matchup.get("expected_winrate_max"),
    }
    start = time.time()
    try:
        result = _http_post_json(f"{api_base}/playtest/benchmark_v1", body)
    except urllib.error.HTTPError as exc:
        return {
            "matchup_name": matchup_name,
            "error": f"HTTP {exc.code}: {exc.reason}",
            "deck_a_corpus_id": deck_a_id,
            "deck_b_corpus_id": deck_b_id,
        }
    except Exception as exc:
        return {
            "matchup_name": matchup_name,
            "error": f"{exc.__class__.__name__}: {exc}",
            "deck_a_corpus_id": deck_a_id,
            "deck_b_corpus_id": deck_b_id,
        }
    elapsed = time.time() - start

    seat_stats = _compute_seat_stats(result.get("per_game_log", []))
    n_games = result.get("n_games", 0)
    actual_winrate = result.get("deck_a_winrate", 0.0)
    exp_min = matchup.get("expected_winrate_min")
    exp_max = matchup.get("expected_winrate_max")
    in_band = (
        actual_winrate >= exp_min and actual_winrate <= exp_max
        if (exp_min is not None and exp_max is not None) else None
    )
    drift_amount = 0.0
    if exp_min is not None and exp_max is not None and not in_band:
        if actual_winrate < exp_min:
            drift_amount = round(exp_min - actual_winrate, 4)
        else:
            drift_amount = round(actual_winrate - exp_max, 4)
    draw_rate = round(seat_stats["draws"] / n_games, 4) if n_games > 0 else None

    return {
        "matchup_name": matchup_name,
        "deck_a_role_tag": matchup["deck_a_role_tag"],
        "deck_b_role_tag": matchup["deck_b_role_tag"],
        "deck_a_corpus_id": deck_a_id,
        "deck_b_corpus_id": deck_b_id,
        "expected_winrate_min": exp_min,
        "expected_winrate_max": exp_max,
        "confidence_band": matchup.get("confidence_band"),
        "rationale": matchup.get("rationale", ""),
        "n_games": n_games,
        "actual_winrate": actual_winrate,
        "in_band": in_band,
        "drift_amount": drift_amount,
        "draw_count": seat_stats["draws"],
        "draw_rate": draw_rate,
        "seat0_wins": seat_stats["seat0_wins"],
        "seat1_wins": seat_stats["seat1_wins"],
        "elapsed_seconds": round(elapsed, 1),
        "per_game_log": result.get("per_game_log", []),
        "calibration_status_at_run": result.get("overall_calibration_status"),
    }


def _format_report(
    matrix: Dict[str, Any],
    matchup_results: List[Dict[str, Any]],
    *,
    snapshot: str,
    n_game_pairs: int,
    max_turns_per_game: int,
    total_elapsed: float,
) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines: List[str] = []
    lines.append(f"# MPA Calibration Report — {ts}")
    lines.append("")
    lines.append(f"- Matrix: `{matrix.get('version', 'calibration_matrix_v1')}`")
    lines.append(f"- Snapshot: `{snapshot}`")
    lines.append(f"- Games per matchup: `{n_game_pairs * 2}` (n_game_pairs={n_game_pairs}, mirrored)")
    lines.append(f"- Max turns/game: `{max_turns_per_game}`")
    lines.append(f"- Total elapsed: `{total_elapsed:.1f}s`")
    lines.append("")

    # Summary table
    lines.append("## Summary")
    lines.append("")
    lines.append("| Matchup | N | Actual WR | Expected | In Band | Drift | Draw Rate | Seat 0/1 wins |")
    lines.append("|---|---|---|---|---|---|---|---|")
    drift_count = 0
    total_draws = 0
    total_games_for_draws = 0
    for r in matchup_results:
        if r.get("error"):
            lines.append(
                f"| {r['matchup_name']} | — | ERROR | — | — | — | — | {r.get('error')} |"
            )
            continue
        in_band = r.get("in_band")
        band_str = "✓" if in_band else ("✗" if in_band is False else "—")
        if in_band is False:
            drift_count += 1
        exp = (
            f"{r['expected_winrate_min']:.2f}-{r['expected_winrate_max']:.2f}"
            if r.get("expected_winrate_min") is not None else "—"
        )
        total_draws += r.get("draw_count", 0)
        total_games_for_draws += r.get("n_games", 0)
        lines.append(
            f"| {r['matchup_name']} | {r['n_games']} | {r['actual_winrate']:.3f} "
            f"| {exp} | {band_str} | {r['drift_amount']:.3f} "
            f"| {r['draw_rate']:.3f} | {r['seat0_wins']}/{r['seat1_wins']} |"
        )
    lines.append("")

    # Headline aggregates
    overall_draw_rate = (
        round(total_draws / total_games_for_draws, 4)
        if total_games_for_draws > 0 else None
    )
    lines.append("## Headline aggregates")
    lines.append("")
    lines.append(f"- **Matchups outside expected band (drift count):** `{drift_count}` of `{sum(1 for r in matchup_results if not r.get('error'))}`")
    lines.append(f"- **Overall draw rate (all matchups):** `{overall_draw_rate}` ({total_draws} draws / {total_games_for_draws} games)")
    # Anti-bias check: find mirror match if present
    mirror = next((r for r in matchup_results if "mirror" in r.get("matchup_name", "").lower() and not r.get("error")), None)
    if mirror is not None:
        s0 = mirror["seat0_wins"]
        s1 = mirror["seat1_wins"]
        decisive = s0 + s1
        if decisive > 0:
            seat0_share = s0 / decisive
            lines.append(
                f"- **Anti-bias mirror check:** seat 0 won `{s0}`, seat 1 won `{s1}` "
                f"(seat 0 share = {seat0_share:.3f} of decisive games; draws={mirror['draw_count']})"
            )
        else:
            lines.append(
                f"- **Anti-bias mirror check:** no decisive games (all {mirror['draw_count']} draws); "
                "anti-bias inconclusive from this run"
            )
    else:
        lines.append("- **Anti-bias mirror check:** mirror matchup not found in results")
    lines.append("")

    # Per-matchup details
    lines.append("## Per-matchup details")
    lines.append("")
    for r in matchup_results:
        lines.append(f"### {r['matchup_name']}")
        lines.append("")
        if r.get("error"):
            lines.append(f"**Error:** `{r['error']}`")
            lines.append("")
            continue
        lines.append(f"- **deck_a:** `{r['deck_a_role_tag']}` → `{r['deck_a_corpus_id']}`")
        lines.append(f"- **deck_b:** `{r['deck_b_role_tag']}` → `{r['deck_b_corpus_id']}`")
        lines.append(f"- **rationale:** {r['rationale']}")
        lines.append(f"- **expected:** {r['expected_winrate_min']:.2f}-{r['expected_winrate_max']:.2f} ({r['confidence_band']})")
        lines.append(f"- **actual:** deck_a winrate = {r['actual_winrate']:.3f} over {r['n_games']} games")
        lines.append(f"- **in band:** {r['in_band']}; drift amount: {r['drift_amount']:.3f}")
        lines.append(f"- **draws:** {r['draw_count']} ({r['draw_rate']:.3f} draw rate)")
        lines.append(f"- **seat wins:** seat0={r['seat0_wins']}, seat1={r['seat1_wins']}")
        lines.append(f"- **elapsed:** {r['elapsed_seconds']:.1f}s")
        # Per-game outcomes condensed
        outcomes = [(g.get("swap"), g.get("outcome"), g.get("turns"), g.get("loss_reason")) for g in r.get("per_game_log", [])]
        if outcomes:
            lines.append("")
            lines.append("| # | swap | outcome | turns | loss_reason |")
            lines.append("|---|---|---|---|---|")
            for i, (swap, outcome, turns, lr) in enumerate(outcomes, 1):
                lines.append(f"| {i} | {swap} | {outcome} | {turns} | {lr} |")
        lines.append("")

    # Recommendations
    lines.append("## Recommendations")
    lines.append("")
    if overall_draw_rate is not None and overall_draw_rate > 0.5:
        lines.append(
            f"- **Policy finish-rate is a Phase 5b blocker.** Draw rate "
            f"{overall_draw_rate:.1%} exceeds 50%. Either raise max_turns_per_game "
            "or upgrade the policy to close games faster (instants, removal, "
            "win-condition recognition)."
        )
    elif overall_draw_rate is not None and overall_draw_rate < 0.2:
        lines.append(f"- Finish-rate healthy (draw rate {overall_draw_rate:.1%} < 20%); calibration signal is usable.")
    if drift_count > 0:
        lines.append(f"- {drift_count} matchup(s) outside expected bands. Inspect per-matchup details — drift may indicate policy gaps or expected-band miscalibration.")
    else:
        lines.append("- All matchups within expected bands. Calibration matrix v1 is currently consistent with policy behavior.")
    if mirror and mirror.get("seat0_wins", 0) + mirror.get("seat1_wins", 0) == 0:
        lines.append("- Mirror match produced zero decisive games — cannot validate anti-bias at this N. Either raise N or max_turns.")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="MPA calibration suite runner (Phase 5b.3).")
    ap.add_argument("--snapshot", required=True, help="db_snapshot_id (e.g. 20260217_190902)")
    ap.add_argument("--api-base", default="http://localhost:8000")
    ap.add_argument("--matrix", default=str(DEFAULT_MATRIX_PATH))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--n-game-pairs", type=int, default=None, help="Override matrix default")
    ap.add_argument("--max-turns-per-game", type=int, default=None, help="Override matrix default")
    args = ap.parse_args()

    matrix_path = Path(args.matrix)
    matrix = _load_matrix(matrix_path)
    n_game_pairs = args.n_game_pairs or matrix.get("default_n_game_pairs", 5)
    max_turns = args.max_turns_per_game or matrix.get("default_max_turns_per_game", 25)

    print(f"[calibration] matrix: {matrix_path}")
    print(f"[calibration] {len(matrix['matchups'])} matchups × {n_game_pairs * 2} games = {len(matrix['matchups']) * n_game_pairs * 2} total games")
    print(f"[calibration] max_turns_per_game: {max_turns}")
    print(f"[calibration] api_base: {args.api_base}")
    print()

    start_total = time.time()
    matchup_results: List[Dict[str, Any]] = []
    for i, matchup in enumerate(matrix["matchups"], 1):
        name = matchup["matchup_name"]
        print(f"[calibration] [{i}/{len(matrix['matchups'])}] running {name}...", flush=True)
        r = _run_matchup(
            api_base=args.api_base,
            snapshot=args.snapshot,
            matchup=matchup,
            n_game_pairs=n_game_pairs,
            max_turns_per_game=max_turns,
        )
        matchup_results.append(r)
        if r.get("error"):
            print(f"    ERROR: {r['error']}", flush=True)
        else:
            print(
                f"    actual={r['actual_winrate']:.3f} expected={r['expected_winrate_min']:.2f}-{r['expected_winrate_max']:.2f} "
                f"in_band={r['in_band']} draws={r['draw_count']}/{r['n_games']} elapsed={r['elapsed_seconds']:.1f}s",
                flush=True,
            )
    total_elapsed = time.time() - start_total

    report = _format_report(
        matrix, matchup_results,
        snapshot=args.snapshot,
        n_game_pairs=n_game_pairs,
        max_turns_per_game=max_turns,
        total_elapsed=total_elapsed,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"calibration_report_{ts}.md"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
    print()
    print(f"[calibration] report written: {out_path}")
    print(f"[calibration] total elapsed: {total_elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
