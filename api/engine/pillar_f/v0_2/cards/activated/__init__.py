"""Phase 3 — Activated abilities + tap-cost mana producers.

Mana rocks (Sol Ring, Arcane Signet, Mind Stone), mana lands (Reliquary
Tower, fetches), tutors (Demonic Tutor activated-style stubs),
equipment-equip costs, and any other `{cost}: effect` ability.

Registration model: each card's activated ability is wired as a
resolver via `register_resolver(name, fn)`. The cost-payment side is
caller-controlled in iter-10's substrate (CR 117 + 602.1 cost-payment
plumbing is iter-11+ scope per the v9 final report). Per-card tests
construct a stack entry with `payment={"resolver": "<name>", "cost":
{...}}` and call `resolve_top(state)` to verify the effect lands.
"""
# Framework first — per-card modules depend on it.
from api.engine.pillar_f.v0_2.cards.activated import framework  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated.framework import (
    ActivatedAbilityMeta, register_activated_ability,
    get_activated_ability, get_activated_abilities_for_card,
    all_registered_card_names, build_activation_payload,
    add_mana_resolver, add_multiple_mana_resolver,
    add_any_color_resolver, add_commander_color_identity_resolver,
    fetchland_resolver, equip_resolver,
)

# Per-card modules — importing triggers their registration calls.
from api.engine.pillar_f.v0_2.cards.activated import mana_rocks  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import fetchlands  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import mana_lands  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import equipment  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import dual_lands_bulk  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import bulk_metadata_stubs  # noqa: F401
# v14 Phase 4: 23 long-tail cards previously in "best-effort interpret"
# fall-through bucket. Wires explicit ActivatedAbilityMeta + resolver
# for each so oracle_seed_coverage moves them to "full handler".
from api.engine.pillar_f.v0_2.cards.activated import long_tail_v14  # noqa: F401

__all__ = [
    "ActivatedAbilityMeta", "register_activated_ability",
    "get_activated_ability", "get_activated_abilities_for_card",
    "all_registered_card_names", "build_activation_payload",
    "add_mana_resolver", "add_multiple_mana_resolver",
    "add_any_color_resolver", "add_commander_color_identity_resolver",
    "fetchland_resolver", "equip_resolver",
]
