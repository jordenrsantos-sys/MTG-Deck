"""Combat-phase prompts: attackers + blockers.

Scoping doc section 2b. Two prompt types:

1. Attackers prompt — single call per combat phase. Active player's
   LLM picks which creatures to attack + each attacker's target
   (player_id or planeswalker card_id).

2. Blockers prompt — one call per defending player. Each defender's
   LLM picks which blockers (if any) to assign per attacker + the
   damage-assignment order for multi-block.

Response contracts (output JSON):

Attackers:
  {
    "attackers": [
      {"attacker_index": <int>, "target_index": <int>}
    ],
    "rationale": "..."
  }

Blockers:
  {
    "blocks": [
      {"attacker_index": <int>, "blocker_indices": [<int>, ...]}
    ],
    "rationale": "..."
  }

`*_index` refers into the pre-computed eligible lists shown in the
prompt. Engine maps indices back to card_ids.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


COMBAT_PROMPT_VERSION = "pillar_f_v0_2_policy_combat_prompt_v1"


ATTACKERS_SYSTEM_PROMPT = """You are piloting one seat in a multiplayer Commander game. You are the active player declaring attackers. Pick which of your creatures attack and what each one targets (a player or an opposing planeswalker).

Output VALID JSON ONLY with this shape:
{
  "attackers": [
    {"attacker_index": <int>, "target_index": <int>}
  ],
  "rationale": "<short reason, ≤ 200 chars>"
}

Indices refer to the lists shown in the user prompt. The `attackers` array may be empty if you choose not to attack. Each `attacker_index` should appear at most once.

Honor table politics: prefer attacking the highest-threat opponent (from POLITICS CONTEXT). Don't attack an ally you have an active deal with. Don't attack a creature you'd lose in combat unless trading is worth it. Untargeted attack chip damage is fine when no creatures threaten lethal back.

Output ONLY the JSON. No prose, no markdown fences."""


BLOCKERS_SYSTEM_PROMPT = """You are piloting one seat in a multiplayer Commander game. You are a defending player. Pick which of your creatures block which attackers (if any).

Output VALID JSON ONLY with this shape:
{
  "blocks": [
    {"attacker_index": <int>, "blocker_indices": [<int>, ...]}
  ],
  "rationale": "<short reason, ≤ 200 chars>"
}

Indices refer to the lists shown in the user prompt. The `blocks` array may be empty (take all damage). For multi-block, list blockers in damage-assignment ORDER (active player declares; iter-10 honors block declaration order).

Goal: keep yourself alive. Block lethal attacks. Trade creatures with bigger opponents when you can. Chump-block (block with a 1/1 to absorb damage from a 5/5) when life is low.

Output ONLY the JSON. No prose, no markdown fences."""


def build_attackers_prompt(
    compact_view_text: str,
    eligible_attackers: List[Dict[str, Any]],
    attack_targets: List[Dict[str, Any]],
    *,
    politics_context: Optional[Dict[str, Any]] = None,
    deck_archetype_hint: Optional[str] = None,
    last_error_message: Optional[str] = None,
) -> str:
    """Build the attacker-declaration user prompt.

    Args:
        compact_view_text: result of compact_view().
        eligible_attackers: list of {card_id, name, power, toughness,
            keywords, description}.
        attack_targets: list of {target_id, kind, description},
            kind in {"player", "planeswalker"}.
        politics_context: per-opponent threat scores (Phase 7 populates).
        deck_archetype_hint: Pillar D theme_profile primary archetype.
        last_error_message: re-prompt context after validator failure.
    """
    parts: List[str] = []
    parts.append("=== CURRENT GAME STATE ===")
    parts.append(compact_view_text)
    parts.append("")

    if deck_archetype_hint:
        parts.append(f"=== YOUR DECK ARCHETYPE: {deck_archetype_hint} ===")
        parts.append("")

    if politics_context:
        parts.append("=== POLITICS CONTEXT ===")
        threats = politics_context.get("threats") or {}
        for opp_id, info in sorted(threats.items(), key=lambda kv: str(kv[0])):
            score = info.get("score", 0.0)
            parts.append(f"  P{opp_id}: threat_score={score:.2f}")
        alliances = politics_context.get("alliances") or {}
        if alliances:
            parts.append("  Alliances: " + ", ".join(
                f"P{k}={v}" for k, v in sorted(alliances.items(), key=lambda kv: str(kv[0]))
            ))
        parts.append("")

    parts.append("=== ELIGIBLE ATTACKERS ===")
    for i, a in enumerate(eligible_attackers):
        desc = a.get("description") or a.get("name", "?")
        parts.append(f"  [{i}] {desc}")
    if not eligible_attackers:
        parts.append("  (none eligible)")
    parts.append("")

    parts.append("=== ATTACK TARGETS ===")
    for i, t in enumerate(attack_targets):
        desc = t.get("description") or t.get("kind", "?")
        parts.append(f"  [{i}] {desc}")
    parts.append("")

    if last_error_message:
        parts.append("=== ATTENTION: PRIOR RESPONSE FAILED VALIDATION ===")
        parts.append(f"  {last_error_message}")
        parts.append("")

    parts.append(
        "Return JSON with the attackers array (each entry: "
        "attacker_index + target_index) and a short rationale. "
        "Empty attackers array = no attack this turn."
    )
    return "\n".join(parts)


def build_blockers_prompt(
    compact_view_text: str,
    eligible_blockers: List[Dict[str, Any]],
    attackers_to_block: List[Dict[str, Any]],
    *,
    politics_context: Optional[Dict[str, Any]] = None,
    last_error_message: Optional[str] = None,
) -> str:
    """Build the blocker-assignment user prompt for one defender.

    Args:
        compact_view_text: result of compact_view() from defender's view.
        eligible_blockers: list of {card_id, name, power, toughness,
            keywords, description} — defender's untapped creatures.
        attackers_to_block: list of {attacker_index, name, power,
            toughness, keywords, target_pid, description} — incoming
            attackers from defender's perspective.
        politics_context: defender's politics.
        last_error_message: re-prompt context.
    """
    parts: List[str] = []
    parts.append("=== CURRENT GAME STATE (defending player view) ===")
    parts.append(compact_view_text)
    parts.append("")

    if politics_context:
        parts.append("=== POLITICS CONTEXT ===")
        threats = politics_context.get("threats") or {}
        for opp_id, info in sorted(threats.items(), key=lambda kv: str(kv[0])):
            parts.append(f"  P{opp_id}: threat_score={info.get('score', 0.0):.2f}")
        parts.append("")

    parts.append("=== INCOMING ATTACKERS (you are the defender) ===")
    for i, atk in enumerate(attackers_to_block):
        desc = atk.get("description") or atk.get("name", "?")
        parts.append(f"  [{i}] {desc}")
    parts.append("")

    parts.append("=== YOUR ELIGIBLE BLOCKERS ===")
    for i, b in enumerate(eligible_blockers):
        desc = b.get("description") or b.get("name", "?")
        parts.append(f"  [{i}] {desc}")
    if not eligible_blockers:
        parts.append("  (none — you'll take all damage)")
    parts.append("")

    if last_error_message:
        parts.append("=== ATTENTION: PRIOR RESPONSE FAILED VALIDATION ===")
        parts.append(f"  {last_error_message}")
        parts.append("")

    parts.append(
        "Return JSON with the blocks array (each entry: attacker_index "
        "+ blocker_indices list in damage-assignment order). Empty blocks "
        "array = no blocks (take all damage)."
    )
    return "\n".join(parts)
