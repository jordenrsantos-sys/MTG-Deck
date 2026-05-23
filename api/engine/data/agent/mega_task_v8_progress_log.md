# Mega-task v8 / Pillar D iter 9 — progress log

Iter 9 dispatch on top of v7 ship (`a708b0f34`). Kickoff:
`mega_task_v8_kickoff.md`.

Append-only, timestamped sections per phase.

---

## Phase 0 — Pre-flight + baseline UI build (2026-05-23)

**Substrate snapshot:**
- HEAD: `a708b0f34` (v7 Phase 9 — SHIPPED at 10/12).
- pytest baseline (verified at v7 ship): 1608 / 25 skipped / 0 failed.
- vitest baseline (verified at v7 ship): 774 / 2 pre-existing failed.
- Memory dir state: 5 entries (MEMORY.md + 4 project/feedback files).

**Coherence sweep #3 deferred items status:** All 4 shipped in v6:
- `agent_voyage_downgrade_pass_v1` wiring — SHIPPED v6 Phase 5.
- `voyage_rules_embedding_v1` at-scale — SHIPPED v6 Phase 6 (667 sections).
- `ENGINE_API_GUIDE.md` overhaul — SHIPPED v6 Phase 8.
- 8 pre-existing test failures — SHIPPED v6 Phase 7 (retired with @skip).

No carry-over from sweep #3.

**Edgar B3 baseline build captured** (via TestClient — chrome-devtools-mcp
not in tool roster; substituted with TestClient + Python tool):

Warning code counts on the iter-9 entry build:
- `POOL_SLOT_FALLBACK_TRIGGERED`: 1
- `POOL_BACKFILL_USED_OVERFLOW_CANDIDATES`: 1
- `MANA_BASE_DISCREPANCY_UNJUSTIFIED`: 1
- `CARD_ADVANTAGE_DISCREPANCY_UNJUSTIFIED`: 1
- `CURVE_DISCREPANCY`: 1
- `INTERACTION_DISCREPANCY`: 1
- `SWAP_ITERATION`: 3
- `WILD_POOL_SEMANTIC_AUGMENTED`: 1
- `COMBO_ANCHOR_GUARD_ACTIVE`: 1
- `POOL_FORBIDDEN_FILTERED`: 1
- `THEME_THEME_NOT_FOUND`: 4

`STRUCTURAL_SAFETY_NET_SINGLETON_FIXED` did NOT fire on this seed — the
Edgar duplicate issue may be seed-dependent. Phase 2 fixes the upstream
regardless.

**A-prefix wave confirmed:** 32 A-prefix cards in the deck, mostly via
`slot_fallback:*` sources. Examples:
- `A-Karn, Living Legacy` (ramp), `A-Visions of Phyrexia` (ramp),
  `A-Carnelian Orb of Dragonkind` (ramp), `A-Town` (ramp),
  `A-Hall of Tagsin` (ramp), `A-Excavation Explosion` (ramp).
- `Academic Dispute` (card_draw), `Abeyance` (card_draw),
  `Asmodeus the Archfiend` (card_draw), `Aang's Defense` (card_draw),
  `Aang's Journey` (card_draw).
- `Abstruse Appropriation` (removal), `Active Volcano` (removal),
  `Affa Guard Hound` (removal), `Agonizing Demise` (removal),
  `Anguished Unmaking` (removal), `Aim for the Head` (removal).

**Root cause located.** `_inject_slot_fallback_candidates` in
`agent_build_deck_v1.py` (v7 Phase 1 code) sorts candidates by
`name` ASC after color-identity filtering:
```python
color_legal.sort(key=lambda c: c["name"])
```
That's the alphabetical-fill bug — there is no archetype-relevance
signal in the ranking, so the first N matches by name win.

**Phase 1 will fix this** with a tiered archetype-relevance score:
tier 1 archetype-tagged → tier 2 primitive-overlap → tier 3 generic-
staple. Alphabetical is NEVER a tier.

**Phase 0 deliverables:**
- Progress log scaffolded (this file).
- Task list created (9 phases, this session).
- Baseline warning + A-prefix evidence captured.

**Commit message:** "Phase 0 (mega-task v8): pre-flight + iter-9 baseline capture".
