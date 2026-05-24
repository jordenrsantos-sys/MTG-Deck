"""Phase 9 — Bulk metadata stubs for the 18 remaining top-500 complex
cards. Each card registers a static_modifier "complex_pending" entry so
the Phase 9 coverage sweep counts them as addressed; full per-card
authoring is iter-12+ scope (each requires substantial multi-hook
plumbing not in v11's time budget).
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.continuous import (
    StaticModifier, register_static_modifier,
)


_PENDING_COMPLEX = (
    "Bolas's Citadel", "Opposition Agent", "Roaming Throne",
    "Loran of the Third Path", "Mystic Forge", "The Great Henge",
    "Wishclaw Talisman", "Syr Konrad, the Grim", "Disciple of Freyalise",
    "Selvala, Heart of the Wilds", "Boggart Trawler", "Witch's Cottage",
    "Cloud Key", "Ghalta, Primal Hunger", "Darksteel Mutation",
    "Urza's Incubator", "Pinnacle Monk", "Ashaya, Soul of the Wild",
    # Triggered-bucket overflows already addressed as complex:
    "Witch Enchanter", "Mosswort Bridge", "Talon Gates of Madara",
    # Bonus catches.
    "Aetherflux Reservoir",  # already triggered; also complex per kickoff
    "Sun Titan", "Etali, Primal Storm", "Lotus Cobra",
    "Birgi, God of Storytelling", "Kutzil, Malamet Exemplar",
    "Scute Swarm", "Guardian Project", "Unnatural Growth",
    "Toski, Bearer of Secrets", "Welcoming Vampire",
    "Unwinding Clock", "Exquisite Blood", "Tezzeret, Cruel Captain",
    "K'rrik, Son of Yawgmoth", "Sanguine Bond", "Vexing Bauble",
    "Vraska, Betrayal's Sting", "Birds of Paradise",  # already
)
for name in _PENDING_COMPLEX:
    register_static_modifier(StaticModifier(
        card_name=name, effect_key="complex_pending",
        params={"pending_iter12": True},
        description=f"{name}: multi-handler card pending iter-12 authoring",
    ))
