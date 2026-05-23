"""Per-card metadata: top-500 ranked list + categorization overrides.

This subpackage holds DATA, not code. The `build_top_500.py` script
regenerates `top_500_edh_cards.json` from the strength-oracle corpus +
Scryfall bulk. `_categorization_overrides.json` lets the build script
override the regex-driven handler-type bucketing for cards where the
heuristic gets it wrong (e.g., Path of Ancestry is really an activated
mana land, not a replacement effect).
"""
