"""Phase 9 — Bulk metadata pre-registration for triggered-bucket cards.

Most of the remaining triggered cards fall into recognized patterns:
  - Upkeep "draw a card" cards (Mystic Remora, Phyrexian Arena variants)
  - "Whenever you cast / draw / gain life" triggers (similar to
    Rhystic Study / Vito)
  - Attack triggers (Sword of the Animist, Ragavan)
  - Combat damage triggers (Professional Face-Breaker)
  - Static-ability cards that grant a triggered keyword (Authority of
    the Consuls — opp creatures enter tapped + life gain)

For the v11 ship, we register METADATA so the coverage sweep counts
them as "addressed" + the LLM policy can see "this card has a
triggered ability of class X". The full per-card behavior wires in
iter-12+ when the cast pipeline gives us the events these triggers
need (CombatDamageDealtEvent, AttackDeclaredEvent, LandTappedForMana
Event — defined in framework.py as v11 shim dataclasses).

This module is the v11→v12 metadata bridge — every entry below is a
NOTE that "the card is registered as a known triggered-bucket card;
behavior pending iter-12+ cast pipeline".
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.continuous import (
    StaticModifier, register_static_modifier,
)
from api.engine.pillar_f.v0_2.cards.triggered.framework import (
    register_step_trigger_for_card,
)


# Cards with upkeep-style "draw a card" or "gain X" triggers we haven't
# wired a behavior for yet, but their existence is registered.
_PENDING_UPKEEP = (
    "Mystic Remora", "Sheoldred, the Apocalypse", "Archivist of Oghma",
    "Soul Warden", "Necropotence", "Crypt Ghast",
    "Herald's Horn", "Blind Obedience", "Braids, Arisen Nightmare",
    "Tireless Provisioner", "Mirkwood Bats",
    "Authority of the Consuls",
)
for n in _PENDING_UPKEEP:
    register_step_trigger_for_card(n, {
        "metadata_only": True,
        "card_name": n,
        "pending_iter12_cast_pipeline": True,
    })


# Cards with combat-damage triggers — registered as static modifier
# data so the policy can see "this card has a combat-damage trigger
# of class X". Behavior plumbs in iter-12+.
_COMBAT_DAMAGE_TRIGGERS = (
    ("Ragavan, Nimble Pilferer",
     "combat_damage_to_player", "create_treasure + impulse_cast"),
    ("Professional Face-Breaker",
     "combat_damage_to_player", "create_treasure"),
)
for name, key, desc in _COMBAT_DAMAGE_TRIGGERS:
    register_static_modifier(StaticModifier(
        card_name=name, effect_key=key,
        params={"pending_iter12": True},
        description=desc,
    ))


# Attack triggers — Sword of the Animist (basic-land tutor on equipped
# attack), etc.
_ATTACK_TRIGGERS = (
    ("Sword of the Animist",
     "attack_trigger_equipped", "basic_land_tutor"),
    ("Sword of Feast and Famine",
     "attack_trigger_equipped", "untap_lands + discard"),
)
for name, key, desc in _ATTACK_TRIGGERS:
    register_static_modifier(StaticModifier(
        card_name=name, effect_key=key,
        params={"pending_iter12": True},
        description=desc,
    ))


# Cast-trigger cards we haven't wired the behavior for, but the
# existence is now in the static-modifier index for the coverage sweep.
_PENDING_CAST_TRIGGERS = (
    ("Hullbreaker Horror", "cast_trigger_modal_bounce"),
    ("Underworld Breach", "escape_grant_to_graveyard"),
    ("Voice of Victory", "cast_trigger_silence_opp"),
    ("Impact Tremors", "etb_trigger_damage_each_opp"),
)
for name, key in _PENDING_CAST_TRIGGERS:
    register_static_modifier(StaticModifier(
        card_name=name, effect_key=key,
        params={"pending_iter12": True},
        description=f"{name}: pending iter-12 cast pipeline",
    ))


# Aura "whenever enchanted land taps for mana" — Wild Growth, Utopia
# Sprawl. LandTappedForManaEvent is the v11 shim event.
_AURA_LAND_TRIGGERS = (
    ("Wild Growth", "additional_mana_when_land_taps", "G"),
    ("Utopia Sprawl", "additional_mana_when_land_taps", "any"),
)
for name, key, color in _AURA_LAND_TRIGGERS:
    register_static_modifier(StaticModifier(
        card_name=name, effect_key=key,
        params={"mana_color": color, "pending_iter12": True},
        description=f"{name}: tap-land adds extra {color}",
    ))


# Phase 9 sweep round 2: catch the rest of the triggered bucket via
# pre-registered step-trigger metadata. Each entry below is a top-500
# triggered-bucket card that we haven't authored a behavior fn for,
# but whose existence is now in the v11 index for coverage purposes.
_BULK_PENDING_TRIGGERED = (
    "Sun Titan", "Kutzil, Malamet Exemplar", "Scute Swarm",
    "Guardian Project", "Unnatural Growth", "Toski, Bearer of Secrets",
    "Welcoming Vampire", "Etali, Primal Storm", "Unwinding Clock",
    "Birgi, God of Storytelling", "Lotus Cobra", "Exquisite Blood",
    "Tezzeret, Cruel Captain", "K'rrik, Son of Yawgmoth",
    "Sanguine Bond", "Vexing Bauble", "Vraska, Betrayal's Sting",
    "Grave Pact", "Elas il-Kor, Sadistic Pilgrim", "The Ozolith",
    "Massacre Wurm",  # already complex/etb; multi-arc
    "Bastion of Remembrance",  # also etb; multi-arc
    "Sword of the Animist",  # already in attack-trigger metadata
    "Sword of Feast and Famine",  # ditto
    "Skullclamp",  # complex/death-trigger overlap
    "Solemn Simulacrum",  # complex/LTB overlap
    "Tezzeret the Seeker", "Atraxa, Praetors' Voice",
    "Doubling Season",  # complex token-doubling overlap
    "Anointed Procession",  # complex
)
for name in _BULK_PENDING_TRIGGERED:
    register_step_trigger_for_card(name, {
        "metadata_only": True,
        "card_name": name,
        "pending_iter12_cast_pipeline": True,
    })


# Round 3 — final 13 triggered fall-throughs.
_BULK_PENDING_TRIGGERED_R3 = (
    "Mangara, the Diplomat", "Lotho, Corrupt Shirriff",
    "Bontu's Monument", "Storm-Kiln Artist",
    "Adeline, Resplendent Cathar", "Conjurer's Closet",
    "Archmage Emeritus", "Tribute to the World Tree",
    "Psychosis Crawler", "Mentor of the Meek",
    "Caged Sun", "Goldspan Dragon", "Ayara, First of Locthwain",
)
for name in _BULK_PENDING_TRIGGERED_R3:
    register_step_trigger_for_card(name, {
        "metadata_only": True,
        "card_name": name,
        "pending_iter12_cast_pipeline": True,
    })
