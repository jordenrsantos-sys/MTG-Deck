"""Politics state tracker — per-player record of opponent dynamics.

Stored on PlayerState.politics_state (substrate reserved this slot in
iter-10). Schema per scoping doc section 4:

```python
{
  "threats": {opponent_id: {score, board_strength, tempo,
                             life_pressure, recent_aggression}},
  "deals": [{opponent_player_id, deal_type, agreed_turn, kept}],
  "alliances": {opponent_id: "ally" | "neutral" | "rival"},
  "damage_log": {opponent_id: int},  # rolling recent-turns damage
  "damage_log_turn_window": [(turn_number, opponent_id, amount), ...],
}
```

`update_politics_state(state, viewer_id, event)` is called by the
substrate (or the policy layer) after every significant game event.
"""
from __future__ import annotations

from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.state import GameState


POLITICS_STATE_VERSION = "pillar_f_v0_2_policy_politics_state_v1"

# Cap on deals[] length per scoping doc section 7.
DEALS_CAP = 50

# How many recent turns to count for recent_aggression. Anything older
# than (current_turn - RECENT_AGGRESSION_WINDOW) falls off the rolling
# damage_log.
RECENT_AGGRESSION_WINDOW = 3

# Valid alliance values.
ALLIANCE_VALUES = ("ally", "neutral", "rival")


def _ensure_initialized(politics_state: Dict[str, Any]) -> None:
    """Backfill any missing schema keys on a fresh politics_state dict."""
    politics_state.setdefault("threats", {})
    politics_state.setdefault("deals", [])
    politics_state.setdefault("alliances", {})
    politics_state.setdefault("damage_log", {})
    politics_state.setdefault("damage_log_turn_window", [])


def update_politics_state(
    state: GameState, viewer_id: int, event: Dict[str, Any],
) -> None:
    """Apply an event to viewer's PlayerState.politics_state.

    Event shapes:
      - {"type": "combat_damage", "from": opponent_id, "amount": int}
      - {"type": "spell_cast_against", "from": opponent_id,
         "spell_card_id": str}
      - {"type": "deal_made", "with": opponent_id,
         "deal_type": str, "agreed_turn": int}
      - {"type": "deal_honored", "with": opponent_id,
         "deal_type": str}
      - {"type": "deal_broken", "with": opponent_id,
         "deal_type": str}
      - {"type": "threat_recompute", "opponent_id": int,
         "threat_dict": {...}}  ← upserts threats[opponent_id]
    """
    if not (0 <= viewer_id < len(state.players)):
        return
    viewer = state.players[viewer_id]
    ps = viewer.politics_state
    _ensure_initialized(ps)
    evt_type = event.get("type")
    turn = state.turn_number

    if evt_type == "combat_damage":
        opp = event.get("from")
        amount = int(event.get("amount", 0))
        if opp is None or amount <= 0:
            return
        ps["damage_log_turn_window"].append((turn, opp, amount))
        # Auto-update rolling damage_log per opponent.
        _roll_damage_log(ps, current_turn=turn)
        # Aggression damage → escalate alliance one step (ally→neutral
        # →rival on each significant hit).
        _bump_alliance(ps, opp, "rival")

    elif evt_type == "spell_cast_against":
        opp = event.get("from")
        if opp is None:
            return
        # Spell targeting viewer counts as 1 unit of "tactical aggression"
        # so the bookkeeping looks the same — append to damage_log_turn_window
        # with amount=1 and treat as a mild rival-bump.
        ps["damage_log_turn_window"].append((turn, opp, 1))
        _roll_damage_log(ps, current_turn=turn)
        # Don't auto-bump alliance for a single targeting; gentler
        # signal than combat damage.

    elif evt_type == "deal_made":
        opp = event.get("with")
        deal_type = event.get("deal_type", "unknown")
        agreed = int(event.get("agreed_turn", turn))
        if opp is None:
            return
        ps["deals"].append({
            "opponent_player_id": opp,
            "deal_type": deal_type,
            "agreed_turn": agreed,
            "kept": False,  # status TBD; set on honored/broken
        })
        _cap_deals(ps)
        _bump_alliance(ps, opp, "ally")

    elif evt_type == "deal_honored":
        opp = event.get("with")
        deal_type = event.get("deal_type")
        if opp is None:
            return
        # Mark most-recent deal of matching type as kept.
        for d in reversed(ps["deals"]):
            if d.get("opponent_player_id") == opp:
                if deal_type is None or d.get("deal_type") == deal_type:
                    d["kept"] = True
                    break
        _bump_alliance(ps, opp, "ally")

    elif evt_type == "deal_broken":
        opp = event.get("with")
        deal_type = event.get("deal_type")
        if opp is None:
            return
        for d in reversed(ps["deals"]):
            if d.get("opponent_player_id") == opp:
                if deal_type is None or d.get("deal_type") == deal_type:
                    d["kept"] = False
                    break
        # Deal break → bump straight to rival.
        ps["alliances"][opp] = "rival"

    elif evt_type == "threat_recompute":
        opp = event.get("opponent_id")
        threat = event.get("threat_dict") or {}
        if opp is None:
            return
        ps["threats"][opp] = dict(threat)


def _cap_deals(ps: Dict[str, Any]) -> None:
    """Drop oldest entries to stay under DEALS_CAP."""
    deals = ps["deals"]
    if len(deals) > DEALS_CAP:
        excess = len(deals) - DEALS_CAP
        del deals[:excess]


def _roll_damage_log(ps: Dict[str, Any], *, current_turn: int) -> None:
    """Recompute rolling damage_log: sum damage in last
    RECENT_AGGRESSION_WINDOW turns per opponent, drop older entries
    from damage_log_turn_window.

    Note: caller is responsible for invoking this on turn rollover too
    (else stale damage from older turns lingers). The substrate's
    cleanup_step or the policy layer's turn-start hook can call
    `roll_damage_log_for_turn(state, viewer_id, current_turn)` to
    keep this fresh between events.
    """
    window = ps["damage_log_turn_window"]
    cutoff = current_turn - RECENT_AGGRESSION_WINDOW
    # Drop turns before cutoff. cutoff itself is INCLUDED (3-turn
    # window = [turn-2, turn-1, turn]).
    kept = [(t, opp, amt) for (t, opp, amt) in window if t > cutoff]
    ps["damage_log_turn_window"] = kept
    # Recompute summary.
    summary: Dict[int, int] = {}
    for (_t, opp, amt) in kept:
        summary[opp] = summary.get(opp, 0) + amt
    ps["damage_log"] = summary


def _bump_alliance(
    ps: Dict[str, Any], opponent_id: int, toward: str,
) -> None:
    """Move alliance one step toward the given direction.

    Transitions:
      - toward "ally":  rival → neutral → ally
      - toward "rival": ally → neutral → rival
    Other transitions snap to the value directly.
    """
    current = ps["alliances"].get(opponent_id, "neutral")
    if toward not in ALLIANCE_VALUES:
        return
    if toward == "ally":
        if current == "rival":
            ps["alliances"][opponent_id] = "neutral"
        elif current == "neutral":
            ps["alliances"][opponent_id] = "ally"
        # already ally → no change
    elif toward == "rival":
        if current == "ally":
            ps["alliances"][opponent_id] = "neutral"
        elif current == "neutral":
            ps["alliances"][opponent_id] = "rival"
        # already rival → no change
    else:
        # toward neutral → snap.
        ps["alliances"][opponent_id] = "neutral"


def roll_damage_log_for_turn(
    state: GameState, viewer_id: int, current_turn: int,
) -> None:
    """Public helper: drop stale damage_log_turn_window entries on
    turn rollover. Call this once per turn-start so recent_aggression
    decays properly even without new combat events."""
    if not (0 <= viewer_id < len(state.players)):
        return
    ps = state.players[viewer_id].politics_state
    _ensure_initialized(ps)
    _roll_damage_log(ps, current_turn=current_turn)


def export_politics_context(
    state: GameState, viewer_id: int,
) -> Dict[str, Any]:
    """Build the politics_context dict the LLM prompts consume.

    Shape matches main_phase.py + response_window.py expectations:
      {"threats": {pid: {score, board_strength, ...}}, "alliances":
       {pid: str}, "deals": [...]}
    """
    if not (0 <= viewer_id < len(state.players)):
        return {}
    ps = state.players[viewer_id].politics_state
    _ensure_initialized(ps)
    return {
        "threats": dict(ps.get("threats") or {}),
        "alliances": dict(ps.get("alliances") or {}),
        "deals": list(ps.get("deals") or []),
    }
