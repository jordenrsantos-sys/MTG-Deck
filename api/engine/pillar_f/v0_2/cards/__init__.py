"""Per-card oracle-text handlers compiled against the iter-10 Pillar F
v0.2 substrate. Mega-task v11 deliverable.

Scope of this package:
  - This package ONLY ADDS handlers via the substrate's existing
    registration APIs (register_replacement_fn, register_layer_effect,
    register_resolver, register_step_trigger, etc.). It does NOT modify
    the substrate.
  - Each module under one of the 9 handler-type subpackages
    (simple/, etb/, ltb/, activated/, continuous/, replacement/,
    triggered/, spell/, complex/) registers a focused group of cards.
  - Import this package (or any subpackage) to trigger registration of
    all cards it contains. Registration is keyed by oracle_id (preferred)
    or card name; helpers in each subpackage's __init__.py wire the
    registrations.

Package layout:
    cards/
      _meta/              — corpus pull script + top_500 JSON (data only)
      simple/             — Phase 1 — vanilla creatures + basic lands
      etb/                — Phase 2 — enter-the-battlefield triggers
      ltb/                — Phase 2 — leaves-the-battlefield / die triggers
      activated/          — Phase 3 — activated abilities + mana producers
      continuous/         — Phase 4 — layer-6 / layer-7 static effects
      replacement/        — Phase 5 — replacement effects
      triggered/          — Phase 6 — non-ETB triggered abilities
      spell/              — Phase 7 — instants + sorceries (spell_resolve)
      complex/            — Phase 8 — multi-handler / combo pieces

Coordination note (v10 parallel arc):
    Mega-task v10 (LLM strategic policy) lives in `policy/`, NOT in this
    package. v10 plugs into the substrate via its callback-hook surface
    (the priority responder + decision-point hooks). Disjoint module
    trees → merge conflicts in this package are not expected.
"""

CARDS_PKG_VERSION = "pillar_f_v0_2_cards_v1_skeleton"

# Importing the cards package triggers registration of every card
# handler in every subpackage. Test fixtures that exercise card behavior
# should `import api.engine.pillar_f.v0_2.cards  # noqa: F401`.
from api.engine.pillar_f.v0_2.cards import simple  # noqa: F401
from api.engine.pillar_f.v0_2.cards import etb  # noqa: F401
from api.engine.pillar_f.v0_2.cards import ltb  # noqa: F401
from api.engine.pillar_f.v0_2.cards import activated  # noqa: F401
from api.engine.pillar_f.v0_2.cards import continuous  # noqa: F401
from api.engine.pillar_f.v0_2.cards import replacement  # noqa: F401
from api.engine.pillar_f.v0_2.cards import triggered  # noqa: F401
from api.engine.pillar_f.v0_2.cards import spell  # noqa: F401
from api.engine.pillar_f.v0_2.cards import complex  # noqa: F401
