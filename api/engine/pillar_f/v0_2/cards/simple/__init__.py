"""Phase 1 — Simple permanents.

Vanilla creatures (no oracle_text), basic lands, and basic-keyword
creatures that need no per-card handler logic beyond the default
permanent behavior built into Card + the layer engine.

Each card in this bucket either:
  - Needs no registration at all (oracle text is empty / printed
    keywords are already in Card.keywords), OR
  - Registers a single trivial activated ability — e.g., Plains
    registers `{T}: Add {W}`, Llanowar Elves registers `{T}: Add {G}`.

Phase 1's bar is "lowest complexity per the kickoff" — these registrations
are intentionally thin.
"""
from api.engine.pillar_f.v0_2.cards.simple import basic_lands  # noqa: F401
from api.engine.pillar_f.v0_2.cards.simple import mana_dorks  # noqa: F401
from api.engine.pillar_f.v0_2.cards.simple import vanilla_creatures  # noqa: F401
