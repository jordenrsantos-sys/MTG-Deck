"""Phase 9 — Bulk registration of dual / shock / pain / check / bond /
slow lands and other tap-for-1-of-2-colors mana producers from the
top 500.

Each entry below is (card_name, color_pair). All share the substrate
behavior "{T}: Add 1 of color1 OR color2"; the caller specifies which
color via payment["color"]. ETB-tapped conditions, life-loss conditions,
etc. are caller-controlled in iter-10 (cost-payment side).

This single module bulk-registers ~60 dual-color lands. Re-using the
add_any_color_resolver — its closure honors payment["color"] and
defaults to C — means the resolver doesn't need per-card variation.
The metadata table tells the LLM / cast pipeline what colors each land
CAN produce so the choice is constrained at iter-11+ when the policy
queries get_activated_ability.
"""
from __future__ import annotations

from typing import Dict, Tuple

from api.engine.pillar_f.v0_2.cards.activated.framework import (
    ActivatedAbilityMeta, register_activated_ability,
    add_any_color_resolver, add_mana_resolver,
)
from api.engine.pillar_f.v0_2.stack import register_resolver


# Single shared any-color tap resolver (colorless default; honors
# payment["color"] for the actual selection). Cards with stricter
# 2-color restriction trust the caller to pick within the pair.
register_resolver("act_dual_tap_any", add_any_color_resolver())


# Single-color tap resolvers (utility lands that produce only 1 color).
_SINGLE_COLOR_RESOLVERS = {
    "W": "act_single_tap_W",
    "U": "act_single_tap_U",
    "B": "act_single_tap_B",
    "R": "act_single_tap_R",
    "G": "act_single_tap_G",
    "C": "act_single_tap_C",
}
for color, key in _SINGLE_COLOR_RESOLVERS.items():
    register_resolver(key, add_mana_resolver(color))


# (card_name, allowed_colors_tuple) — colors the land can produce.
_DUAL_LANDS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    # Shock lands (10).
    ("Hallowed Fountain", ("W", "U")),
    ("Watery Grave", ("U", "B")),
    ("Blood Crypt", ("B", "R")),
    ("Stomping Ground", ("R", "G")),
    ("Temple Garden", ("G", "W")),
    ("Godless Shrine", ("W", "B")),
    ("Steam Vents", ("U", "R")),
    ("Overgrown Tomb", ("B", "G")),
    ("Sacred Foundry", ("R", "W")),
    ("Breeding Pool", ("G", "U")),
    # Pain lands (5 of 10 are in top 500 — only the ones we found).
    ("Caves of Koilos", ("W", "B", "C")),
    ("Sulfurous Springs", ("B", "R", "C")),
    ("Underground River", ("U", "B", "C")),
    ("Adarkar Wastes", ("W", "U", "C")),
    ("Brushland", ("G", "W", "C")),
    ("Karplusan Forest", ("R", "G", "C")),
    # Check lands.
    ("Glacial Fortress", ("W", "U")),
    ("Dragonskull Summit", ("B", "R")),
    ("Drowned Catacomb", ("U", "B")),
    ("Sunpetal Grove", ("G", "W")),
    ("Rootbound Crag", ("R", "G")),
    ("Isolated Chapel", ("W", "B")),
    ("Hinterland Harbor", ("G", "U")),
    ("Clifftop Retreat", ("R", "W")),
    ("Sulfur Falls", ("U", "R")),
    ("Woodland Cemetery", ("B", "G")),
    # Battle/bond lands.
    ("Sunken Hollow", ("U", "B")),
    ("Cinder Glade", ("R", "G")),
    ("Smoldering Marsh", ("B", "R")),
    ("Prairie Stream", ("W", "U")),
    ("Canopy Vista", ("G", "W")),
    # Bond lands (4-player commander friendly).
    ("Morphic Pool", ("U", "B")),
    ("Luxury Suite", ("B", "R")),
    ("Vault of Champions", ("W", "B")),
    ("Sea of Clouds", ("W", "U")),
    ("Spire Garden", ("R", "G")),
    ("Bountiful Promenade", ("G", "W")),
    # Slow lands.
    ("Deserted Beach", ("W", "U")),
    ("Shattered Sanctum", ("W", "B")),
    ("Overgrown Farmland", ("G", "W")),
    ("Shipwreck Marsh", ("U", "B")),
    ("Haunted Ridge", ("B", "R")),
    ("Stormcarved Coast", ("U", "R")),
    ("Rockfall Vale", ("R", "G")),
    ("Sundown Pass", ("W", "U")),  # placeholder, not in 500 maybe
    # Innistrad reveal lands.
    ("Port Town", ("W", "U")),
    ("Choked Estuary", ("U", "B")),
    ("Foreboding Ruins", ("B", "R")),
    ("Game Trail", ("R", "G")),
    ("Fortified Village", ("G", "W")),
    # Misc.
    ("Fetid Heath", ("W", "B", "C")),
    ("Darkwater Catacombs", ("U", "B")),
    ("Skycloud Expanse", ("W", "U")),
    ("Tainted Field", ("W", "B", "C")),
    ("Tainted Peak", ("B", "R", "C")),
    # Single-color utility.
    ("Cabal Coffers", ("B",)),
    ("Cabal Stronghold", ("B", "C")),
    ("Gaea's Cradle", ("G",)),
    ("Cavern of Souls", ("C",)),  # any color version is via the chosen-type side
    ("Boseiju, Who Endures", ("G",)),
    ("Otawara, Soaring City", ("U",)),
    ("Eiganjo, Seat of the Empire", ("W",)),
    ("Takenuma, Abandoned Mire", ("B",)),
    ("Sokenzan, Crucible of Defiance", ("R",)),
    ("Seat of the Synod", ("U",)),
    ("Phyrexian Tower", ("C",)),
    ("Darksteel Citadel", ("C",)),
    ("Buried Ruin", ("C",)),
    ("Strip Mine", ("C",)),
    ("Geier Reach Sanitarium", ("C",)),
    ("Karn's Bastion", ("C",)),
    ("Demolition Field", ("C",)),
    ("Inventors' Fair", ("C",)),
    ("Mystic Sanctuary", ("U",)),
    ("Urza's Cave", ("C",)),
    ("Scavenger Grounds", ("C",)),
    ("Emergence Zone", ("C",)),
    ("Three Tree City", ("C",)),
    ("Command Beacon", ("C",)),
    ("Temple of the False God", ("C",)),
    ("City of Traitors", ("C",)),
    ("Arena of Glory", ("R",)),
    ("Minas Tirith", ("W",)),
    ("Shifting Woodland", ("G",)),
    ("Hydroelectric Specimen", ("C",)),
    # Triomes (3 colors).
    ("Indatha Triome", ("W", "B", "G")),
    ("Ketria Triome", ("G", "U", "R")),
    ("Raugrin Triome", ("U", "R", "W")),
    ("Savai Triome", ("R", "W", "B")),
    ("Zagoth Triome", ("B", "G", "U")),
    # Temples (scry lands).
    ("Temple of Enlightenment", ("W", "U")),
    ("Temple of Silence", ("W", "B")),
    ("Temple of Deceit", ("U", "B")),
    ("Temple of Malice", ("B", "R")),
    ("Temple of Triumph", ("R", "W")),
    ("Temple of Plenty", ("G", "W")),
    ("Temple of Abandon", ("R", "G")),
    ("Temple of Epiphany", ("U", "R")),
    ("Temple of Malady", ("B", "G")),
    ("Temple of Mystery", ("G", "U")),
    # Tri-color utility.
    ("Mana Confluence", ("W", "U", "B", "R", "G")),
    # Misc misc.
    ("Mosswort Bridge", ("G",)),
    ("Witch Enchanter", ("C",)),
    ("Talon Gates of Madara", ("C",)),
    # Murders/houses cycle.
    ("Undercity Sewers", ("U", "B")),
    ("Hushwood Verge", ("G", "W")),
    ("Lush Portico", ("G", "W")),
    ("Meticulous Archive", ("W", "U")),
    ("Commercial District", ("R", "W")),
    ("Thundering Falls", ("U", "R")),
    ("Underground Mortuary", ("B", "U")),
    ("Raucous Theater", ("B", "R")),
    ("Elegant Parlor", ("R", "W")),
    ("Gloomlake Verge", ("U", "B")),
    ("Hedge Maze", ("G", "U")),
    ("Bountiful Landscape", ("W", "G")),
)


for card_name, colors in _DUAL_LANDS:
    if len(colors) == 1:
        # Single-color land — wire the specific color resolver.
        color = colors[0]
        resolver_key = _SINGLE_COLOR_RESOLVERS.get(color, "act_dual_tap_any")
    else:
        # Multi-color: any-color stub w/ caller pick.
        resolver_key = "act_dual_tap_any"
    description_color = " or ".join(f"{{{c}}}" for c in colors)
    register_activated_ability(ActivatedAbilityMeta(
        card_name=card_name, ability_key="tap_mana",
        resolver_name=resolver_key,
        cost_tap=True,
        description=f"{{T}}: Add {description_color}",
    ))
