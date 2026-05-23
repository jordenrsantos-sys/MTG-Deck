"""Phase 8 — Complex / multi-handler cards.

Cards with multiple interacting abilities + combo-line pieces:
  - Edgar Markov (eminence + ETB token + attack trigger)
  - Dockside Extortionist (ETB w/ opponent-permanent count)
  - Underworld Breach (escape mechanic from graveyard)
  - Food Chain + Eternal Scourge / Misthollow Griffin (infinite mana
    via exile-and-recast)
  - Aetherflux Reservoir (life-gain triggers + activated win)
  - Ad Nauseam line (reveal + lose-life loop until stop)
  - Demonic Consultation (exile until card type or empty library)
  - Sensei's Divining Top (activated + draw interaction)

Per-card modules register the full handler set for their card —
typically a mix of ETB, activated, replacement, and triggered
abilities, each wired through the appropriate substrate API. Each
complex card also ships its own integration test demonstrating its
headline combo line.

These are the hardest cards in the corpus. Phase 8's bar per the
kickoff is "build the full handler set + add a dedicated combo-line
integration test for each".
"""
# Phase 8 modules wire in as cards are added.
