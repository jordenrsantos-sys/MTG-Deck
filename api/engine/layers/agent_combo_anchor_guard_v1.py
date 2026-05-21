"""
agent_combo_anchor_guard_v1 — Iter 3 Phase 2: architectural envelope guard.

When a user names a must-include card, they signal intent ("I want this
card") but NOT necessarily intent to assemble the combo that card
participates in. The iter-2 Ur-Dragon case stress-tested this: user named
Tiamat (a tutor card that fetches 5 specific dragons), the LLM intent
interpreter suggested `Hellkite Charger`, and C2.2 wild-combo discovery
applied `Old Gnawbone` as a swap — both cards Tiamat fetches. The iter-2
"creativity envelope" rule held only because Hellkite Charger wasn't in
the deterministic pool; Old Gnawbone slipped through.

This module is the architectural fix. The rule:

  For each card X the user listed as must-include, scan the combo
  registry for any combo variant where X is one of the card_names. Add
  every OTHER card in those variants to a forbidden_set. The forbidden
  set is threaded into every LLM phase's prompt as a hard "do not
  suggest" list. Outputs are also re-validated against the set after
  the LLM responds: any forbidden card the LLM proposed is dropped and
  logged as a `guard_fire` event.

  Exception: if the user ALSO listed a partner card as a must-include,
  the user has explicitly opted into the combo. In that case the partner
  does NOT enter the forbidden set (and neither do other co-listed
  must-includes).

The registry handles combos of all sizes (2-card pairs through 5+ card
chains). Multi-card combo handling matters: if Tiamat is one anchor in a
5-card combo, the other 4 cards in that combo are all forbidden (not
just one partner).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


COMBO_ANCHOR_GUARD_VERSION = "agent_combo_anchor_guard_v1.0"


def build_forbidden_set(
    must_includes: Iterable[str],
    combo_registry: Optional[Dict[str, Any]] = None,
    registry_path: Optional[Path] = None,
) -> Tuple[Set[str], List[Dict[str, Any]]]:
    """Compute the forbidden-card set from user must-includes.

    Args:
        must_includes: card names the user listed as must-includes.
        combo_registry: parsed `combo_brackets_v1.json` (the `by_variant_id`
            section). If None, the function loads from `registry_path` or
            the default location.
        registry_path: alternate path to the registry JSON. Only consulted
            if `combo_registry` is None.

    Returns:
        (forbidden_names_lower, sources)
          - forbidden_names_lower: set of lowercased card names the LLM
            phases must not suggest.
          - sources: list of dicts, one per combo that contributed to the
            forbidden set, with `combo_id`, `combo_size`,
            `user_anchor` (must-include that triggered the rule),
            `completing_cards` (the new forbidden cards added by this
            combo). For audit logging.

    The function never raises on a missing/corrupt registry — it returns
    (empty set, empty list) and lets the caller decide whether to warn.
    Build_deck() does emit a warning in that path so the user sees the
    guard is in degraded mode.
    """
    must_include_lower = {(n or "").strip().lower() for n in must_includes if isinstance(n, str)}
    must_include_lower.discard("")
    if not must_include_lower:
        return set(), []

    if combo_registry is None:
        try:
            path = registry_path or _default_registry_path()
            raw = json.loads(path.read_text(encoding="utf-8"))
            combo_registry = raw.get("by_variant_id") if isinstance(raw, dict) else None
        except Exception:
            return set(), []

    if not isinstance(combo_registry, dict):
        return set(), []

    forbidden_lower: Set[str] = set()
    sources: List[Dict[str, Any]] = []

    for combo_id, variant in combo_registry.items():
        if not isinstance(variant, dict):
            continue
        card_names = variant.get("card_names")
        if not isinstance(card_names, list) or len(card_names) < 2:
            continue

        names_lower = [(n or "").strip().lower() for n in card_names if isinstance(n, str)]
        names_lower = [n for n in names_lower if n]
        anchors_present = must_include_lower.intersection(names_lower)
        if not anchors_present:
            continue

        # `partners` = card names in this variant that are NOT user must-
        # includes. Those become forbidden.
        partner_names_lower = [n for n in names_lower if n not in must_include_lower]
        if not partner_names_lower:
            # User listed every card in this combo — they explicitly
            # opted in. No additions.
            continue

        for partner in partner_names_lower:
            forbidden_lower.add(partner)

        # Pick one representative user anchor for the audit log (the first
        # alphabetically — stable across runs).
        user_anchor = sorted(anchors_present)[0]
        # Original-case names (preserved from registry) for the audit log.
        original_partners = [
            n for n in card_names
            if isinstance(n, str) and n.strip().lower() in partner_names_lower
        ]
        sources.append({
            "combo_id": str(combo_id),
            "combo_size": variant.get("combo_size") or len(card_names),
            "user_anchor": user_anchor,
            "completing_cards": original_partners,
        })

    return forbidden_lower, sources


def _default_registry_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "combos" / "combo_brackets_v1.json"


def filter_llm_suggestions(
    suggestions: Iterable[str],
    forbidden_set: Set[str],
) -> Tuple[List[str], List[str]]:
    """Split an LLM-proposed name list into (kept, blocked).

    `forbidden_set` is the lowercased name set returned by
    `build_forbidden_set`.

    Names are compared case-insensitively; original casing is preserved
    in the returned lists.
    """
    kept: List[str] = []
    blocked: List[str] = []
    for name in suggestions or []:
        if not isinstance(name, str) or not name.strip():
            continue
        if name.strip().lower() in forbidden_set:
            blocked.append(name)
        else:
            kept.append(name)
    return kept, blocked


def format_forbidden_block_for_prompt(forbidden_set: Set[str]) -> str:
    """Compose the system-prompt block that warns the LLM not to suggest
    forbidden cards. Returns empty string if the set is empty so the
    prompt doesn't include a useless 'forbidden: []' line."""
    if not forbidden_set:
        return ""
    names = sorted(forbidden_set)
    return (
        "\nFORBIDDEN CARDS (do NOT suggest under any circumstances): "
        + ", ".join(names)
        + "\nThese cards would complete a combo with one of the user's "
        "must-include cards. The user did not list them, which signals "
        "they want the must-include card for non-combo reasons. Respect "
        "this — the deck-building algorithm will silently drop any "
        "forbidden card you propose, so suggesting them is wasted work."
    )
