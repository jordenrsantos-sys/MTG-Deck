"""Counterspell-family card annotation helpers.

Sub-C Phase 2. Owns sub-B Phase 9 deferred gate 6.

v11's `cards/spell/counterspells.py` already registers the actual
counter-resolution functions via `register_spell` (which wraps the
substrate's `register_resolver` and calls `counter_target(state,
target_entry_id)` to remove the targeted stack object). What v11
does NOT do is build the per-Card `iter10_annotation` dict that
sub-B's `compute_eligible_actions` consumes.

This module provides `make_counterspell_annotation(card_name)` that
builds the right iter10_annotation:

```python
{
    "description": "counter target spell",
    "payment": {"resolver": "spell_counterspell"},  # from v11 registry
    "target_stack_top": True,   # flag for eligible_actions
}
```

When a Card with this annotation enters a deck, the LLM can `cast` it
during a response window where state.stack is non-empty;
compute_eligible_actions resolves `default_targets` to the top
stack entry's `entry_id` at cast time (see Phase 2's
compute_eligible_actions patch in policy/eligible_actions.py).

Sub-C does NOT modify v11's existing register_spell registrations;
this module is purely additive.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.cards.spell.framework import (
    build_spell_payload, get_spell_resolver_key,
    all_registered_spell_names,
)


COUNTERSPELL_ANNOTATIONS_VERSION = (
    "pillar_f_v0_2_playtest_counter_war_annotations_v1"
)


# Counterspell-family cards per scoping doc + kickoff Phase 2:
# Counterspell, Negate, Mana Drain, Swan Song, Force of Will,
# Force of Negation, Dovin's Veto, Arcane Denial, Mindbreak Trap,
# Mental Misstep, Pact of Negation, Daze, plus v11-added Fierce
# Guardianship + An Offer You Can't Refuse.
#
# Some of these are already registered in v11; others are
# "intended-future" entries the iter-11 cards module did not ship
# (Force of Will / Mindbreak Trap / Mental Misstep / Pact of Negation
# / Daze / Dovin's Veto / Force of Negation). For those, sub-C
# registers a basic counter resolver below.
COUNTERSPELL_FAMILY_NAMES: List[str] = [
    "Counterspell",
    "Negate",
    "Mana Drain",
    "Swan Song",
    "Arcane Denial",
    "An Offer You Can't Refuse",
    "Fierce Guardianship",
    # Not yet in v11; sub-C registers via _register_missing_counterspells:
    "Force of Will",
    "Force of Negation",
    "Dovin's Veto",
    "Mindbreak Trap",
    "Mental Misstep",
    "Pact of Negation",
    "Daze",
]


def _register_missing_counterspells() -> None:
    """For counterspells in COUNTERSPELL_FAMILY_NAMES that v11 did not
    register, add a minimal `counter target spell` resolver so sub-C
    decks can cast them.

    Each resolver just calls substrate's counter_target on entry.targets[0].
    Side effects (e.g., Force of Will's exile-blue-card cost, Mana Drain's
    add-mana) are stubbed to noop — iter-12+ refinement.
    """
    from api.engine.pillar_f.v0_2.stack import counter_target
    from api.engine.pillar_f.v0_2.state import GameState, StackEntry
    from api.engine.pillar_f.v0_2.cards.spell.framework import register_spell

    def _basic_counter(state: GameState, entry: StackEntry) -> None:
        if not entry.targets:
            return None
        target_entry_id = entry.targets[0]
        counter_target(state, target_entry_id)
        return None

    for name in COUNTERSPELL_FAMILY_NAMES:
        if get_spell_resolver_key(name) is None:
            register_spell(name, _basic_counter)


# Trigger registration of missing counterspells on import.
_register_missing_counterspells()


def make_counterspell_annotation(
    card_name: str,
    *,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the iter10_annotation dict that sub-B's
    compute_eligible_actions consumes for a counterspell-family card.

    Args:
        card_name: Counterspell, Negate, Mana Drain, etc. Must be in
            COUNTERSPELL_FAMILY_NAMES.
        description: optional human-readable description for the prompt
            (defaults to "<card_name>: counter target spell").

    Returns: dict with shape:
        {
          "description": ...,
          "payment": {"resolver": "spell_<slug>"},
          "target_stack_top": True,
        }
    """
    if card_name not in COUNTERSPELL_FAMILY_NAMES:
        raise ValueError(
            f"{card_name!r} is not a known counterspell-family card. "
            f"Known: {COUNTERSPELL_FAMILY_NAMES}"
        )
    resolver_key = get_spell_resolver_key(card_name)
    if resolver_key is None:
        raise RuntimeError(
            f"Counterspell {card_name!r} is in COUNTERSPELL_FAMILY_NAMES "
            f"but no resolver was registered. Check "
            f"_register_missing_counterspells."
        )
    return {
        "description": description or f"{card_name}: counter target spell",
        "payment": {"resolver": resolver_key},
        "target_stack_top": True,
    }


def attach_counterspell_annotation(card, *, description: Optional[str] = None):
    """Convenience: set `card.iter10_annotation` on a Card instance
    constructed elsewhere (e.g., during deck construction)."""
    card.iter10_annotation = make_counterspell_annotation(
        card.name, description=description,
    )
    return card
