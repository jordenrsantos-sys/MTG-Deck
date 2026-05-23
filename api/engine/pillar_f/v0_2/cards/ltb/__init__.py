"""Phase 2 — Leaves-the-battlefield / dies triggers.

Sibling of `cards/etb/`. LTB triggers fire when a permanent moves from
battlefield to any other zone (CR 603.6c — last known information rule
applies, so the trigger condition checks the card's state immediately
before it left). Common patterns: "When ~ dies, do X", "When ~ leaves
the battlefield, do Y" (broader — fires on bounce/exile/sacrifice too).

Per-card modules register the trigger + the resolver via the substrate's
`register_resolver` API.
"""
# Phase 2 modules wire in as cards are added.
