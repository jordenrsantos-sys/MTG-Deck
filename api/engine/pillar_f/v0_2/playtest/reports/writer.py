"""Markdown + JSON cycle report writer.

Sub-C Phase 4. Per scoping doc section 8.

Writes two artifacts per cycle:
- cycle_report.md (human-readable summary)
- cycle.json (machine-readable full snapshot)

Plus per-game game_<NNN>.json files (one per game in the cycle, dumped
by the cycle runner not this module).
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict


REPORT_WRITER_VERSION = "pillar_f_v0_2_playtest_report_writer_v1"


def _safe_dict(obj: Any) -> Any:
    """Convert dataclasses + non-JSON-friendly types for json.dumps."""
    if hasattr(obj, "__dataclass_fields__"):
        return _safe_dict(asdict(obj))
    if isinstance(obj, dict):
        return {str(k): _safe_dict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_dict(x) for x in obj]
    return obj


def write_cycle_report_json(
    report: Any, output_path: Path,
) -> None:
    """Dump the StageTwoReport to JSON."""
    payload = _safe_dict(report)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def write_cycle_report_markdown(
    report: Any, output_path: Path,
) -> None:
    """Render a markdown summary of the cycle report.

    Layout per scoping doc section 8 + iter-12 extensions:
      # Stage 2 Validation Report -- <deck_id>
      Recommendation: <tier>
      Win-rate breakdown
      Damage analysis
      Politics summary
      Cost summary
      Combat summary
      Notable game logs
      Recommendation prose
    """
    lines: list[str] = []
    lines.append(f"# Stage 2 Validation Report -- {report.deck_under_test_id}")
    lines.append("")
    lines.append(f"**Archetype:** {report.deck_under_test_archetype}")
    lines.append(f"**Games run:** {report.games_completed} "
                 f"completed + {report.games_halted_for_cost} halted "
                 f"= {report.games_completed + report.games_halted_for_cost}")
    lines.append(f"**Total spend:** ${report.cost_summary['total_spend']:.2f}")
    lines.append(f"**Recommendation:** **{report.pass_recommendation}**")
    lines.append("")

    # Win-rate breakdown.
    lines.append("## Win-rate breakdown")
    lines.append(f"- Win rate: {report.win_rate:.2%}")
    lines.append(
        f"- Avg turn eliminated when lost: "
        f"{report.avg_turn_eliminated_when_lost:.1f}"
    )
    lines.append("")

    # Damage analysis (iter-11 stub).
    lines.append("## Damage analysis (iter-11 proxy via politics log)")
    lines.append(f"- Avg damage events dealt: {report.avg_damage_dealt:.1f}")
    lines.append(f"- Avg damage events taken: {report.avg_damage_taken:.1f}")
    lines.append("")

    # Politics summary.
    lines.append("## Politics summary")
    pol = report.politics_summary
    lines.append(
        f"- Alliance distribution across all seats: "
        f"ally={pol['total_ally']}, neutral={pol['total_neutral']}, "
        f"rival={pol['total_rival']}"
    )
    lines.append(
        f"- Total deals made: {pol['total_deals_made']} "
        f"(honored: {pol['total_deals_honored']})"
    )
    lines.append(
        f"- Games with alliance transitions: "
        f"{pol['games_with_alliance_transition']}/"
        f"{report.games_completed + report.games_halted_for_cost}"
    )
    lines.append("")

    # Cost summary.
    lines.append("## Cost summary")
    cs = report.cost_summary
    lines.append(f"- Total: ${cs['total_spend']:.2f}")
    lines.append(f"- Per-game avg: ${cs['per_game_avg']:.2f}")
    lines.append(f"- Per-game max: ${cs['per_game_max']:.2f}")
    lines.append(f"- Games halted for cost: {int(cs['games_halted_for_cost'])}")
    lines.append(f"- Fallback events fired: {int(cs['total_fallback_events'])}")
    lines.append("")

    # Combat summary.
    lines.append("## Combat summary")
    cb = report.combat_summary
    lines.append(f"- Games with at least one attack: {cb['games_with_combat']}")
    lines.append(f"- Games with multi-block: {cb['games_with_multi_block']}")
    lines.append(
        f"- Attacker decisions: {cb['total_attacker_decisions']} "
        f"(fallbacks: {cb['total_combat_fallbacks']})"
    )
    lines.append(f"- Blocker decisions: {cb['total_blocker_decisions']}")
    lines.append("")

    # Notable games.
    lines.append("## Notable game logs")
    if report.per_game_brief:
        # Most decisive win = win with deck_under_test life highest.
        wins = [
            g for g in report.per_game_brief
            if g["deck_under_test_outcome"] == "WIN"
        ]
        losses = [
            g for g in report.per_game_brief
            if g["deck_under_test_outcome"] == "LOSS_ELIMINATED"
        ]
        if wins:
            most_decisive = max(
                wins,
                key=lambda g: g["final_life_totals"].get(
                    str(g.get("winner_pid") or 0), 0,
                ),
            )
            lines.append(
                f"- Most decisive win: game_{most_decisive['game_idx']:03d} "
                f"(seed {most_decisive['seed']}, "
                f"turn {most_decisive['turns_run']}, "
                f"final life: {most_decisive['final_life_totals']})"
            )
        if losses:
            earliest_loss = min(losses, key=lambda g: g["turns_run"])
            lines.append(
                f"- Most decisive loss: game_"
                f"{earliest_loss['game_idx']:03d} "
                f"(seed {earliest_loss['seed']}, "
                f"eliminated by turn {earliest_loss['turns_run']})"
            )
    lines.append("")

    # Recommendation prose.
    lines.append("## Recommendation")
    lines.append(f"**{report.pass_recommendation}** -- {report.recommendation_reason}")
    lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_per_game_json(
    game_result: Any, output_path: Path,
) -> None:
    """Dump a single StageTwoGameResult to JSON for per-game replay."""
    payload = _safe_dict(game_result)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
