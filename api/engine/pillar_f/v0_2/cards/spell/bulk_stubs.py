"""Phase 9 — Bulk-register thin stubs for the high-volume spell patterns
that share resolver shapes with Phase 7's already-wired spells.

Each stub reuses an existing resolver from the Phase 7 modules. This
extends coverage to ~70 additional spells without per-card oracle
authoring — the LLM policy + cast pipeline still see the right
"this card does X" classification, and the v11 substrate sees a
resolved spell (even if the stub is generic).

Iter-11+ will replace these stubs with per-card resolvers as the
v11 authoring continues. The bulk-stub layer is the v11→v12 bridge.
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.spell.framework import register_spell

# Pull existing resolvers from Phase 7 modules.
from api.engine.pillar_f.v0_2.cards.spell.counterspells import (
    _counterspell_resolve,
)
from api.engine.pillar_f.v0_2.cards.spell.removal import (
    _doom_blade_resolve,
)
from api.engine.pillar_f.v0_2.cards.spell.ramp_and_draw import (
    _natures_lore_resolve, _rampant_growth_resolve, _cultivate_resolve,
    _demonic_tutor_resolve, _dark_ritual_resolve,
    _brainstorm_resolve,
)


# ==========================================================
# Bulk counterspell stubs — all just call _counterspell_resolve.
# ==========================================================


_BULK_COUNTERS = (
    "Force of Will", "Force of Negation", "Flusterstorm",
    "Pyroblast", "Red Elemental Blast", "Pact of Negation",
    "Tibalt's Trickery", "Mental Misstep", "Spell Pierce",
    "Dispel", "Dovin's Veto", "Disallow", "Hindering Light",
)
for name in _BULK_COUNTERS:
    register_spell(name, _counterspell_resolve)


# ==========================================================
# Bulk destroy-creature stubs.
# ==========================================================


_BULK_DESTROY = (
    "Abrade", "Withering Torment", "Fell the Profane", "Pongify",
    "Untimely Malfunction", "Rapid Hybridization", "Infernal Grasp",
    "Terminate", "Hero's Downfall", "Anguished Unmaking",
    "Putrefy", "Mortify", "Smother", "Pernicious Deed",
    "Doom Blade",  # already registered; idempotent overwrite
    "Murder", "Vraska's Contempt",
)
for name in _BULK_DESTROY:
    register_spell(name, _doom_blade_resolve)


# ==========================================================
# Bulk draw stubs — most "draw N cards" spells route through a generic
# "controller draws 2" resolver.
# ==========================================================


from api.engine.pillar_f.v0_2.state import GameState, StackEntry


def _generic_draw_2_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    for _ in range(2):
        lib = state.players[pid].zones.library
        if not lib:
            state.players[pid].has_drawn_from_empty_library = True
            break
        state.players[pid].zones.hand.append(lib.pop(0))
        state.players[pid].cards_drawn_this_turn += 1
    return None


def _generic_draw_3_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    for _ in range(3):
        lib = state.players[pid].zones.library
        if not lib:
            state.players[pid].has_drawn_from_empty_library = True
            break
        state.players[pid].zones.hand.append(lib.pop(0))
        state.players[pid].cards_drawn_this_turn += 1
    return None


_DRAW_2 = (
    "Deadly Dispute", "Sign in Blood", "Night's Whisper",
    "Village Rites", "Costly Plunder", "Read the Bones",
    "Painful Truths", "Frantic Search", "Mind Stone",  # iter-10 stub
)
for name in _DRAW_2:
    register_spell(name, _generic_draw_2_resolve)


_DRAW_3 = (
    "Windfall", "Wheel of Fortune", "Reforge the Soul",
    "Return of the Wildspeaker", "Shamanic Revelation",
    "Harmonize", "Tezzeret's Gambit", "Concentrate",
)
for name in _DRAW_3:
    register_spell(name, _generic_draw_3_resolve)


# Ponder = look-at-top-3 + shuffle stub. Iter-10 stub: draw 1.
def _ponder_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    lib = state.players[pid].zones.library
    if lib:
        state.players[pid].zones.hand.append(lib.pop(0))
        state.players[pid].cards_drawn_this_turn += 1


register_spell("Ponder", _ponder_resolve)
register_spell("Preordain", _ponder_resolve)
register_spell("Opt", _ponder_resolve)


# ==========================================================
# Bulk tutor stubs — find-something-and-put-in-hand (iter-10 stub:
# call Demonic Tutor's resolver which is fully generic).
# ==========================================================


_BULK_TUTORS = (
    "Worldly Tutor", "Diabolic Intent", "Crop Rotation",
    "Entomb", "Imperial Seal", "Finale of Devastation",
    "Eladamri's Call", "Idyllic Tutor", "Cruel Tutor",
    "Idyllic Tutor", "Beseech the Mirror", "Insidious Roots",
)
for name in _BULK_TUTORS:
    register_spell(name, _demonic_tutor_resolve)


# Land-search ramp spells route to existing single-basic finders.
_LAND_FETCH = (
    "Farseek", "Three Visits", "Skyshroud Claim",
    "Edge of Autumn", "Lay of the Land",
    "Search for Tomorrow", "Sakura-Tribe Elder",  # creature but search-tied
)
for name in _LAND_FETCH:
    register_spell(name, _natures_lore_resolve)


# ==========================================================
# Bulk damage-spell stubs.
# ==========================================================


def _generic_3_damage_resolve(state: GameState, entry: StackEntry) -> None:
    """Deals 3 damage to target — payment["target"] or first target."""
    if not entry.targets:
        return None
    target = entry.targets[0]
    if isinstance(target, int) and 0 <= target < len(state.players):
        state.players[target].life_total -= 3
    else:
        card = state.get_card(target)
        if card is not None and card.is_creature():
            card.damage_marked += 3
            if card.toughness_int() <= card.damage_marked:
                holder = None
                for ps in state.players:
                    if target in ps.zones.battlefield:
                        holder = ps.player_id
                        break
                if holder is not None:
                    state.move_card(target, from_player=holder,
                                   from_zone="battlefield",
                                   to_player=card.owner,
                                   to_zone="graveyard")


register_spell("Lightning Bolt", _generic_3_damage_resolve)
register_spell("Lightning Strike", _generic_3_damage_resolve)
register_spell("Galvanic Blast", _generic_3_damage_resolve)
register_spell("Magma Jet", _generic_3_damage_resolve)


# ==========================================================
# Bulk mana spells.
# ==========================================================


def _cabal_ritual_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    state.players[pid].mana_pool.B += 3


register_spell("Cabal Ritual", _cabal_ritual_resolve)


def _seething_song_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    state.players[pid].mana_pool.R += 5


register_spell("Seething Song", _seething_song_resolve)


# ==========================================================
# Misc / "other" bucket stubs — register as no-op for now (lets the
# spell resolve cleanly without crashing). Iter-11+ replaces with
# per-card behavior.
# ==========================================================


def _noop_resolve(state: GameState, entry: StackEntry) -> None:
    return None


_BULK_NOOP = (
    "Jeska's Will", "Victimize", "Sink into Stupor", "Silence",
    "Culling the Weak", "Austere Command", "Yawgmoth's Will",
    "Tendrils of Agony", "Exsanguinate", "Akroma's Will",
    "Teferi's Protection", "Malakir Rebirth", "Flare of Fortitude",
    "Inspiring Call", "Tamiyo's Safekeeping", "Bridgeworks Battle",
    "Ephemerate", "Reality Shift", "Archdruid's Charm",
    "Storm Herald", "Krark-Clan Ironworks",
    "Gamble", "Show and Tell",
)
for name in _BULK_NOOP:
    register_spell(name, _noop_resolve)


# Phase 9 sweep round 2: catch remaining top-500 spell-bucket cards.
_BULK_NOOP_R2 = (
    "Big Score", "Thrill of Possibility", "Rishkar's Expertise",
    "Green Sun's Zenith", "High Tide", "Mana Geyser",
    "Sevinne's Reclamation", "Flawless Maneuver", "Farewell",
    "Imp's Mischief", "Aetherize", "Buried Alive", "Stroke of Midnight",
    "Veil of Summer", "Chord of Calling", "Snap", "Rakdos Charm",
    "Redirect Lightning", "Damnation", "Demand Answers", "Damn",
    "Mindbreak Trap", "Defile", "Rite of Flame", "Unexpected Windfall",
    "Time Spiral", "Sublime Epiphany", "Tooth and Nail",
    "Boom // Bust", "Diabolic Edict", "Echoes of Eternity",
    "Snap // Crackle", "Wear // Tear", "Damp the Light",
    "Approach of the Second Sun", "Disenchant", "Naturalize",
    "Eldritch Evolution", "Mystic Confluence", "Cyclone Summoner",
    "Burnt Offering", "Cataclysmic Gearhulk", "Final Parting",
    "Toxrill, the Corrosive",
)
for name in _BULK_NOOP_R2:
    register_spell(name, _noop_resolve)


# Final round 3 — catch-all for remaining spell-bucket fall-throughs.
_BULK_NOOP_R3 = (
    "Fabricate", "Saw in Half", "Reprieve", "Consider", "Bolt Bend",
    "Valakut Awakening", "Bala Ged Recovery", "Gitaxian Probe",
    "Snakeskin Veil", "Nature's Rhythm", "Dramatic Reversal",
    "Grim Tutor", "Nature's Claim", "Snuff Out", "Return the Favor",
    "Explore", "Tainted Pact", "Assassin's Trophy", "Sundering Eruption",
)
for name in _BULK_NOOP_R3:
    register_spell(name, _noop_resolve)
