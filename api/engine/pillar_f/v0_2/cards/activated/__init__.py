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
# Phase 3 modules wire in as cards are added.
