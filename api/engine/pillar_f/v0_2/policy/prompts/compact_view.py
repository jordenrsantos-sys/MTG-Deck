"""compact_view — turn a perspective_view dict into a token-efficient
text representation for LLM prompts.

Iter-10's `state.perspective_view(viewer_player_id)` returns the full
state dict with hidden zones redacted (~30-50KB on mid-game state).
That's too expensive to feed to the LLM directly. compact_view
summarizes the same information in ~1000-4000 tokens depending on
game depth.

Output is plain text, organized for LLM readability:
  PHASE / TURN line
  PLAYERS block: per-player one-line summary (life + hand size + mana
                 pool + commander damage)
  STACK block: top-down resolution order
  BATTLEFIELD block: per-player battlefield contents
  YOUR HAND block: full card detail (only the viewer's hand)
  LAST 3 TURNS LOG: compressed action log

Determinism: dict iteration via `sorted()` everywhere so the same
input always produces the same output (important for prompt cache
hits + test stability).

Token counting: simple word-count proxy (1 token ≈ 4 chars for English)
since tiktoken isn't a project dependency. The Anthropic SDK reports
actual token usage in the response; the helper here estimates.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


COMPACT_VIEW_VERSION = "pillar_f_v0_2_policy_compact_view_v1"


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 characters per token for English.
    Anthropic + OpenAI tokenizers average around this for prose. The
    SDK reports actual usage; this is just for compact_view's internal
    size-budget logic."""
    if not text:
        return 0
    return max(1, len(text) // 4)


def _summarize_card(card_d: Dict[str, Any], *, full: bool = False) -> str:
    """Render one card. `full=True` shows oracle_text + cost (for hand);
    `full=False` shows name + P/T + keywords + tapped flag (for
    battlefield)."""
    if card_d.get("opaque"):
        # Opaque card (in opponent hand / opponent library / face-down).
        zone = card_d.get("zone", "hidden")
        return f"<opaque:{zone}>"
    name = card_d.get("name") or "?"
    type_line = card_d.get("type_line") or ""
    if full:
        cost = card_d.get("mana_cost") or ""
        oracle = (card_d.get("oracle_text") or "").replace("\n", " ").strip()
        if len(oracle) > 200:
            oracle = oracle[:200] + "..."
        parts = [name]
        if cost:
            parts.append(cost)
        if type_line:
            parts.append(type_line)
        if oracle:
            parts.append(f"[{oracle}]")
        return " | ".join(parts)
    # Battlefield-style: short summary.
    parts = [name]
    if "creature" in type_line.lower():
        p = card_d.get("power") or "?"
        t = card_d.get("toughness") or "?"
        parts.append(f"{p}/{t}")
    keywords = card_d.get("keywords") or []
    if keywords:
        parts.append(",".join(keywords[:3]))  # cap at 3 to save tokens
    if card_d.get("tapped"):
        parts.append("TAP")
    counters = card_d.get("counters") or {}
    if counters:
        parts.append(",".join(f"{c}={v}" for c, v in sorted(counters.items())
                              if v > 0))
    return " ".join(parts)


def _player_summary_line(view: Dict[str, Any], pid: int,
                        cards_by_id: Dict[str, Any]) -> str:
    """One-line player summary: life + hand size + mana pool +
    commander damage taken."""
    player = view["players"][pid]
    life = player.get("life_total", 40)
    hand_n = len(player.get("zones", {}).get("hand") or [])
    lib_n = len(player.get("zones", {}).get("library") or [])
    gy_n = len(player.get("zones", {}).get("graveyard") or [])
    ex_n = len(player.get("zones", {}).get("exile") or [])
    mp = player.get("mana_pool") or {}
    mp_total = sum(int(v) for v in mp.values())
    cmdr_dmg = player.get("commander_damage_taken_from") or {}
    cmdr_dmg_str = ""
    if cmdr_dmg:
        cmdr_dmg_str = " cmdr_dmg:" + ",".join(
            f"{k}={v}" for k, v in sorted(cmdr_dmg.items()) if v > 0
        )
    return (
        f"P{pid} life={life} hand={hand_n} lib={lib_n} gy={gy_n} "
        f"exile={ex_n} mana_pool={mp_total}{cmdr_dmg_str}"
    )


def _battlefield_summary(view: Dict[str, Any], pid: int,
                         cards_by_id: Dict[str, Any]) -> List[str]:
    """Per-player battlefield: one line per permanent."""
    player = view["players"][pid]
    bf = player.get("zones", {}).get("battlefield") or []
    lines: List[str] = []
    for cid in bf:
        card_d = cards_by_id.get(cid) or {}
        lines.append(f"  - {_summarize_card(card_d, full=False)}")
    return lines


def _stack_summary(view: Dict[str, Any], cards_by_id: Dict[str, Any]) -> List[str]:
    """Stack contents, top-down (last entry = top = resolves first)."""
    stack = view.get("stack") or []
    if not stack:
        return ["  (empty)"]
    lines: List[str] = []
    # Top of stack = last item per LIFO. Show top first.
    for i, entry in enumerate(reversed(stack)):
        controller = entry.get("controller", 0)
        etype = entry.get("entry_type", "spell")
        desc = entry.get("description", "")
        cid = entry.get("card_id")
        card_d = cards_by_id.get(cid) if cid else None
        card_str = ""
        if card_d and not card_d.get("opaque"):
            card_str = f" ({card_d.get('name', '?')})"
        targets = entry.get("targets") or []
        tgt_str = f" targets={targets}" if targets else ""
        lines.append(f"  [{i}] P{controller} {etype}{card_str}: {desc}{tgt_str}")
    return lines


def _own_hand_full(view: Dict[str, Any], viewer_id: int,
                   cards_by_id: Dict[str, Any]) -> List[str]:
    """Viewer's own hand — full card detail."""
    player = view["players"][viewer_id]
    hand = player.get("zones", {}).get("hand") or []
    lines: List[str] = []
    for cid in hand:
        card_d = cards_by_id.get(cid) or {}
        lines.append(f"  - {_summarize_card(card_d, full=True)}")
    return lines


def compact_view(
    perspective_view: Dict[str, Any],
    *,
    viewer_player_id: Optional[int] = None,
    last_n_turns: int = 3,
    action_log: Optional[List[str]] = None,
) -> str:
    """Build the compact text view from a perspective_view dict.

    Args:
        perspective_view: result of `state.perspective_view(viewer_id)`.
        viewer_player_id: which seat the LLM controls. If None, falls
            back to perspective_view['viewer_player_id'].
        last_n_turns: how many recent turns of action log to include.
        action_log: list of pre-formatted action lines, e.g.
            ["T5 P2 cast Cyclonic_Rift", "T5 P0 pass", ...].

    Returns:
        Plain text suitable for embedding in an LLM prompt.
    """
    viewer_id = viewer_player_id
    if viewer_id is None:
        viewer_id = perspective_view.get("viewer_player_id", 0)

    cards_by_id = perspective_view.get("cards_by_id") or {}
    num_players = len(perspective_view.get("players") or [])

    lines: List[str] = []
    # Header.
    turn = perspective_view.get("turn_number", 1)
    phase = perspective_view.get("phase", "?")
    step = perspective_view.get("step", "?")
    active = perspective_view.get("active_player", 0)
    priority = perspective_view.get("priority_holder")
    monarch = perspective_view.get("the_monarch")
    day_night = perspective_view.get("day_or_night", "neither")
    lines.append(
        f"== TURN {turn} | phase={phase} step={step} | "
        f"active=P{active} priority=P{priority} | "
        f"monarch=P{monarch} day_night={day_night} =="
    )
    lines.append(f"VIEWER: P{viewer_id}")
    lines.append("")

    # Per-player one-line summaries.
    lines.append("PLAYERS:")
    for pid in range(num_players):
        lines.append("  " + _player_summary_line(
            perspective_view, pid, cards_by_id,
        ))
    lines.append("")

    # Stack.
    lines.append("STACK (top-down resolution):")
    lines.extend(_stack_summary(perspective_view, cards_by_id))
    lines.append("")

    # Battlefield per player.
    lines.append("BATTLEFIELD:")
    for pid in range(num_players):
        lines.append(f" P{pid}:")
        bf_lines = _battlefield_summary(perspective_view, pid, cards_by_id)
        if not bf_lines:
            lines.append("  (no permanents)")
        else:
            lines.extend(bf_lines)
    lines.append("")

    # Own hand.
    lines.append(f"YOUR HAND (P{viewer_id}):")
    hand_lines = _own_hand_full(perspective_view, viewer_id, cards_by_id)
    if not hand_lines:
        lines.append("  (empty)")
    else:
        lines.extend(hand_lines)
    lines.append("")

    # Action log.
    if action_log:
        # Filter to most-recent N turns. The format "T<n> P<x> ..." is
        # used by the cost/politics tracker.
        recent_turns = set()
        for line in reversed(action_log):
            # Parse turn number from "T<n> ..." prefix.
            if line.startswith("T") and len(line) > 1:
                try:
                    space_idx = line.index(" ")
                    turn_str = line[1:space_idx]
                    recent_turns.add(int(turn_str))
                except (ValueError, IndexError):
                    pass
            if len(recent_turns) >= last_n_turns:
                break
        last_recent = sorted(recent_turns)[-last_n_turns:]
        filtered = [
            l for l in action_log
            if any(l.startswith(f"T{n} ") for n in last_recent)
        ]
        lines.append(f"RECENT ACTIONS (last {last_n_turns} turns):")
        for line in filtered[-30:]:  # cap at 30 lines to bound tokens
            lines.append(f"  {line}")
        lines.append("")

    return "\n".join(lines)
