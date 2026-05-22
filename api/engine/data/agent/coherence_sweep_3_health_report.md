# Coherence Sweep #3 health report

Project-wide health audit after mega-tasks v1-v5. Sweep started 2026-05-22 on top of `4cee4a287` (mega-task v5 ship). Tested baselines: pytest 1489 / vitest 758.

This report is populated phase-by-phase. The final executive summary + categorized punch list at the top are written during Phase 10 synthesis.

---

## Executive summary (Phase 10)

`<pending — populated during Phase 10>`

---

## Categorized punch list (Phase 10)

### Fixed inline
`<pending>`

### Queued for iter 7 mega-task v6
`<pending>`

### Out-of-scope / wontfix
`<pending>`

---

## Section 1 — Substrate cache audit (Phase 1)

**Verdict: clean.** No caches exceed the 30s cold-start trigger for inline-fix. The only cache that previously did (`deck_strength_check_v1._CORPUS_VECTORS` at ~111 min cold-start) was already fixed in mega-task v5 Phase 5 via the persistent JSON disk cache. All other module-level caches load in under 1.6 seconds.

### Module-level lazy caches (populated on first call)

| Cache | Module | Cold-start | Size | Persistence | Verdict |
|---|---|---|---|---|---|
| `_ONTOLOGY_CACHE` | `extractors/new_combo_discovery_v1.py:88` | 26.7ms | 64 ontology tags + their combos_with edges | none (re-derived from loaded ontology object) | clean |
| `_cache` | `playtest/opposition_decks_v1.py:36` | 1.2ms | 54-entry tiered opposition registry | none (16 KB JSON file is on disk) | clean |
| `_COMBO_BRACKETS_CACHE` | `layers/corpus_batch_ingest_v1.py:32` | 477.6ms | 3679 combo-pair index | none (source `combo_brackets_v1.json` is on disk) | clean |
| `_CURVES_CACHE` | `layers/curve_smoother_v1.py:50` | 1.6ms | 14-archetype curve targets | none (source JSON is on disk) | clean |
| `_CACHE` (Voyage matrix) | `layers/agent_semantic_retrieval_v1.py:54` | 1557.8ms | 30,395-row × 1024-dim float32 matrix (~120 MB in-memory) | none (source `card_embeddings_v1.sqlite` is on disk) | clean — under threshold |
| `_CORPUS_RAW` + `_CORPUS_VECTORS` | `layers/deck_strength_check_v1.py:33-34` | 418.7ms | 13,408 corpus vectors | **disk-persisted** via `corpus_vectors_cache_v1.json` (v5 Phase 5) | clean — already fixed |

### Module-level eager-loaded data (populated at import time)

| Loader | Module | Import cost | Data |
|---|---|---|---|
| `_THEMES + _TYPAL_THEMES + _SIGNAL_VOCAB_BASE + _SIGNAL_VOCAB + _CONFIDENCE_BANDS` | `layers/deck_theme_classifier_v1.py:85-107` | 26.7ms | 41 themes, 137 signals |
| `_PAIR_INDEX + _OUTCOMES` | `layers/combo_enabler_reasons_v1.py:121-122` | 24.5ms | 4,423 pairs, 4,527 outcomes |
| `_OUTCOMES + _BRACKET_PAIR_INDEX` | `layers/deck_combo_insights_v1.py:209-210` | 449.8ms | 4,527 outcomes, 3,679 bracket-pairs |

Full `api.main` import (which transitively imports all of the above plus the entire engine): **897ms** on this box. uvicorn worker boot is dominated by this module-init cost; there is no significant "first-request" cold-start beyond the Voyage matrix (1.5s) and the strength_check vector cache load (~420ms from disk).

### Function-local caches (per-call, intentional)

`proof_scaffold_v1.ruleset_sha_cache` and `proof_attempt_v1.evidence_lookup_cache` are intentional per-call lookup caches scoped to a single endpoint invocation. Not module-level; not in scope for this audit.

### Inline fixes landed

None. No cache exceeded the 30s trigger. The audit verifies the substrate is already in good shape post-mega-task-v5.

### Queued for iter 7 / wontfix

None from Phase 1.


## Section 2 — Cross-pillar integration verification (Phase 2)

`<pending — populated during Phase 2>`

## Section 3 — Memory ↔ code alignment (Phase 3)

`<pending — populated during Phase 3>`

## Section 4 — Test coverage gaps (Phase 4)

`<pending — populated during Phase 4>`

## Section 5 — Database + schema integrity (Phase 5)

`<pending — populated during Phase 5>`

## Section 6 — UI ↔ endpoint contract drift (Phase 6)

`<pending — populated during Phase 6>`

## Section 7 — Documentation drift (Phase 7)

`<pending — populated during Phase 7>`

## Section 8 — Orphan code detection (Phase 8)

`<pending — populated during Phase 8>`

## Section 9 — External-dep audit (Phase 9)

`<pending — populated during Phase 9>`

## Section 10 — Per-pillar smoke tests (Phase 10)

`<pending — populated during Phase 10>`
