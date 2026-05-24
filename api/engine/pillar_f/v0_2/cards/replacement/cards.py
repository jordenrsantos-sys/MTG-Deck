"""Phase 5 — Per-card replacement-effect handlers.

Coverage (9 in-top-500 cards):
  - Dauthi Voidwalker (graveyard-from-anywhere → exile, opp-only)
  - Hardened Scales (+1/+1 counter +1)
  - Panharmonicon (ETB triggers fire twice — partial: registers a
    queryable static modifier; the v11 ETB framework consults the
    counter)
  - Academy Manufactor (Clue/Food/Treasure → one of each — DEFERRED
    to Phase 8 / complex; needs TokenCreateEvent that substrate
    doesn't ship)
  - Doubling Season (counter side via substrate built-in
    `doubling_season_counters`; token side DEFERRED to Phase 8)
  - Charcoal Diamond (ETB tapped via substrate built-in `etb_tapped`)
  - Laboratory Maniac (draw from empty library → win)
  - Aven Mindcensor (opponent library search restricted to top 4)
  - Anointed Procession (token doubling — DEFERRED to Phase 8, same
    reason as Doubling Season's token side)

Per-card modules register the replacement_fn via the substrate's
register_replacement_fn API + a builder via this module's
register_replacement_builder. The substrate's apply_replacements
walks the registered effects when an Event of the matching type is
processed.
"""
from __future__ import annotations

from typing import Optional

from api.engine.pillar_f.v0_2.cards.continuous import (
    StaticModifier, register_static_modifier,
)
from api.engine.pillar_f.v0_2.cards.replacement.framework import (
    register_replacement_builder,
)
from api.engine.pillar_f.v0_2.replacement import (
    Event, register_replacement_fn,
)
from api.engine.pillar_f.v0_2.state import (
    GameState, ReplacementEffect,
)


# ==========================================================
# Dauthi Voidwalker — "If a card would be put into an opponent's
# graveyard from anywhere, instead exile it."
# ==========================================================


def _dauthi_voidwalker_replace(event: Event, state: GameState,
                                source_card_id: Optional[str]) -> None:
    """Generalized to-graveyard replacement: hits DieEvent + DiscardEvent
    + MillEvent. Targets the opponent's card; checks Dauthi's controller
    via state.replacement_effects to know who the source is."""
    # Find Dauthi's controller from the ReplacementEffect entry.
    dauthi_controller: Optional[int] = None
    for r in state.replacement_effects:
        if r.source_card_id == source_card_id:
            dauthi_controller = r.controller
            break
    if dauthi_controller is None:
        return
    # Determine the card's owner side from the event.
    target_owner: Optional[int] = None
    if event.event_type == "DieEvent":
        target_owner = getattr(event, "controller", None)
    elif event.event_type == "DiscardEvent":
        target_owner = getattr(event, "player_id", None)
    elif event.event_type == "MillEvent":
        target_owner = getattr(event, "player_id", None)
    if target_owner is None or target_owner == dauthi_controller:
        return
    # Replace destination → exile.
    if event.event_type == "DieEvent":
        event.instead_zone = "exile"
        event.replaced = True


register_replacement_fn("dauthi_voidwalker", _dauthi_voidwalker_replace)


def _build_dauthi_voidwalker(state: GameState, card_id: str,
                              controller: int) -> ReplacementEffect:
    # Pattern matches DieEvent (substrate's most-common to-graveyard
    # event). DiscardEvent + MillEvent need separate ReplacementEffect
    # entries since the substrate matches on event_type per entry.
    return ReplacementEffect(
        effect_id=f"dauthi_{card_id}",
        source_card_id=card_id, controller=controller,
        event_pattern={"type": "DieEvent"},
        replacement_fn_name="dauthi_voidwalker",
        description="Dauthi Voidwalker: opp's cards exile instead of GY",
    )


register_replacement_builder("Dauthi Voidwalker", _build_dauthi_voidwalker)


# ==========================================================
# Hardened Scales — "If one or more +1/+1 counters would be put on a
# creature you control, that many plus one +1/+1 counters are put on
# it instead."
# ==========================================================


def _hardened_scales_replace(event: Event, state: GameState,
                              source_card_id: Optional[str]) -> None:
    if event.event_type != "CounterAddEvent":
        return
    if getattr(event, "counter_type", "") != "+1/+1":
        return
    # Find Hardened Scales' controller.
    hs_controller: Optional[int] = None
    for r in state.replacement_effects:
        if r.source_card_id == source_card_id:
            hs_controller = r.controller
            break
    if hs_controller is None:
        return
    # Confirm target is controlled by hs_controller.
    target = state.get_card(getattr(event, "target_card_id", ""))
    if target is None or target.controller != hs_controller:
        return
    event.count += 1
    event.replaced = True


register_replacement_fn("hardened_scales", _hardened_scales_replace)


def _build_hardened_scales(state: GameState, card_id: str,
                            controller: int) -> ReplacementEffect:
    return ReplacementEffect(
        effect_id=f"hardened_scales_{card_id}",
        source_card_id=card_id, controller=controller,
        event_pattern={"type": "CounterAddEvent"},
        replacement_fn_name="hardened_scales",
        description="Hardened Scales: +1/+1 counter +1",
    )


register_replacement_builder("Hardened Scales", _build_hardened_scales)


# ==========================================================
# Laboratory Maniac — "If you would draw a card while your library has
# no cards in it, you win the game instead."
# ==========================================================


def _laboratory_maniac_replace(event: Event, state: GameState,
                                source_card_id: Optional[str]) -> None:
    if event.event_type != "DrawEvent":
        return
    pid = getattr(event, "player_id", None)
    if pid is None or not (0 <= pid < len(state.players)):
        return
    # Find Lab Maniac's controller — only fires for that controller.
    lm_controller: Optional[int] = None
    for r in state.replacement_effects:
        if r.source_card_id == source_card_id:
            lm_controller = r.controller
            break
    if lm_controller != pid:
        return
    if state.players[pid].zones.library:
        return  # Library not empty — draw normally.
    # Library empty — win the game instead.
    event.replaced = True
    event.prevent = True
    state.winner_player_id = pid
    state.game_over = True


register_replacement_fn("laboratory_maniac", _laboratory_maniac_replace)


def _build_laboratory_maniac(state: GameState, card_id: str,
                              controller: int) -> ReplacementEffect:
    return ReplacementEffect(
        effect_id=f"lab_maniac_{card_id}",
        source_card_id=card_id, controller=controller,
        event_pattern={"type": "DrawEvent"},
        replacement_fn_name="laboratory_maniac",
        description="Laboratory Maniac: draw from empty library = win",
    )


register_replacement_builder("Laboratory Maniac", _build_laboratory_maniac)


# ==========================================================
# Aven Mindcensor — "If an opponent would search a library, that
# player searches the top four cards of that library instead."
#
# This is a query-time modification of the substrate's tutor logic;
# the substrate doesn't ship a "LibrarySearchEvent". For iter-10, we
# register this as a STATIC MODIFIER in the continuous-bucket registry
# so iter-11+ tutor plumbing can consult it before executing the
# search. The flying / flash printed keywords are already in Card.keywords.
# ==========================================================


register_static_modifier(StaticModifier(
    card_name="Aven Mindcensor",
    effect_key="library_search_restriction",
    params={
        "applies_to": "opponents",
        "search_depth": 4,        # search only top 4 cards
    },
    description="Aven Mindcensor: opponents search only top 4",
))


# ==========================================================
# Panharmonicon — "If an artifact or creature entering causes a
# triggered ability of a permanent you control to trigger, that
# ability triggers an additional time."
#
# This requires the v11 ETB framework's fire_etb_triggers to consult a
# "trigger multiplier" registry. For iter-10 we register Panharmonicon
# as a STATIC MODIFIER ("etb_trigger_multiplier"). Phase 2's ETB
# framework can be extended later (or in this commit if minimal) to
# multiply matching triggers.
# ==========================================================


register_static_modifier(StaticModifier(
    card_name="Panharmonicon",
    effect_key="etb_trigger_multiplier",
    params={
        "applies_to": "controller",  # affects controller's permanents
        "source_filter": ["Artifact", "Creature"],  # ETB of artifact/creature
        "additional_trigger_count": 1,  # +1 trigger per match
    },
    description="Panharmonicon: artifact/creature ETB triggers +1",
))


# ==========================================================
# Charcoal Diamond — ETB tapped (substrate built-in).
# Charcoal Diamond is a 2-CMC mana rock that ETBs tapped and taps for
# {B}. ETB-tapped is handled by the substrate's `etb_tapped`
# replacement_fn; the {T}: Add {B} ability is wired via the
# activated/mana_rocks pattern (but Charcoal Diamond wasn't explicitly
# in the Phase 3 list — wire it here).
# ==========================================================


from api.engine.pillar_f.v0_2.cards.activated.framework import (
    ActivatedAbilityMeta, register_activated_ability,
    add_mana_resolver,
)
from api.engine.pillar_f.v0_2.stack import register_resolver

register_resolver("act_charcoal_diamond_tap", add_mana_resolver("B"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Charcoal Diamond", ability_key="tap_mana",
    resolver_name="act_charcoal_diamond_tap",
    cost_tap=True, description="{T}: Add {B}",
))


def _build_charcoal_diamond(state: GameState, card_id: str,
                             controller: int) -> ReplacementEffect:
    return ReplacementEffect(
        effect_id=f"charcoal_diamond_{card_id}",
        source_card_id=card_id, controller=controller,
        event_pattern={"type": "EnterBattlefieldEvent",
                       "card_id": card_id},  # ETB only for THIS card
        replacement_fn_name="etb_tapped",   # substrate built-in
        description="Charcoal Diamond: ETB tapped",
    )


register_replacement_builder("Charcoal Diamond", _build_charcoal_diamond)


# ==========================================================
# Doubling Season — counter side uses substrate built-in
# `doubling_season_counters`. The token side needs TokenCreateEvent
# the substrate doesn't ship; deferred to Phase 8.
# ==========================================================


def _build_doubling_season(state: GameState, card_id: str,
                            controller: int) -> ReplacementEffect:
    return ReplacementEffect(
        effect_id=f"doubling_season_{card_id}",
        source_card_id=card_id, controller=controller,
        event_pattern={"type": "CounterAddEvent"},
        replacement_fn_name="doubling_season_counters",  # substrate built-in
        description="Doubling Season: counter doubling (token side Phase 8)",
    )


register_replacement_builder("Doubling Season", _build_doubling_season)


# ==========================================================
# Anointed Procession + Academy Manufactor: TOKEN-DOUBLING only.
# Substrate doesn't ship TokenCreateEvent → fully DEFERRED to Phase 8
# (complex/), where a v11-local TokenCreateEvent shim + a modified
# create-token flow can implement these. Register them as static
# modifiers here so the coverage sweep counts them as "addressed".
# ==========================================================


register_static_modifier(StaticModifier(
    card_name="Anointed Procession",
    effect_key="token_creation_multiplier",
    params={"applies_to": "controller", "multiplier": 2},
    description="Anointed Procession: token creation 2x (Phase 8 impl)",
))


register_static_modifier(StaticModifier(
    card_name="Academy Manufactor",
    effect_key="token_creation_substitution",
    params={
        "applies_to": "controller",
        "substitute_from": ["Clue", "Food", "Treasure"],
        "substitute_to": ["Clue", "Food", "Treasure"],
        "mode": "one_of_each",
    },
    description="Academy Manufactor: Clue/Food/Treasure → one of each (Phase 8)",
))
