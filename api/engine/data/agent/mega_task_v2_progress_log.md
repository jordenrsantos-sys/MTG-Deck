# Mega-task v2 — Progress Log

Append-only log for the mega-task that ships Pillar D iter 4 + Pillar C
primitive extractor + Pillar E v0.2 card advantage optimizer + Pillar F
v0.1 upgrade.

Started: 2026-05-21.
Authority: autonomous per `mega_task_v2_kickoff.md` until hard halt condition.
Substrate: iter 3 + Pillar E v0.1 + Pillar C ontology v0 + Pillar F v0.1
+ Track 5 v0.1 (commit `2f177ee7a`, mega-task v1 Phase 14).

---

## Phase 0 — Pre-flight + memory sync — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.00 (no LLM build runs in Phase 0)
- environment:
  - Python 3.10.11 (kickoff requested 3.10+ ✓)
  - VOYAGE_API_KEY: SET (length=46)
  - ANTHROPIC_API_KEY: SET (length=108)
  - E: drive: ~1.07TB free (well clear of 95% halt threshold)
  - git status clean before commit (only `mega_task_v2_kickoff.md` was untracked at the start)
- tests baseline:
  - pytest: **1145 passed / 8 pre-existing fails** (test_bracket_gc_limits_v1, test_complete_bracket_violations_v1 × 5, test_no_random_imports, test_pipeline_profile_bracket_enforcement_v1) — matches the v1 final report's 1144 + 1 (a previously-omitted test now counted). Halt floor for this mega-task: must stay ≥ 1144 + new tests added per phase.
  - vitest: **711 passed / 2 pre-existing fails** (metricPillHeader v1.6 stage 3 markers) — matches v1 final report.
- self-correction events: none
- files read in Phase 0 (per kickoff):
  - `repo/api/engine/data/agent/mega_task_v1_final_report.md` — confirms iter 3 final 5-case sweep metrics, per-phase status, 144 new tests, $5.40 spend, all 6/6 success criteria pass under user-revised targets.
  - `repo/api/engine/data/agent/pillar_d_iteration_3_validation_report.md` — per-case detail (Edgar 143.3s, Krenko 139.3s, Atraxa 137.7s control fallback, Yuriko 136.6s, Ur-Dragon 129.8s; Hellkite absent on Ur-Dragon, Old Gnawbone accepted as corpus baseline).
  - `repo/api/engine/data/agent/mega_task_v1_progress_log.md` — per-phase findings; key takeaway: iter 3 outer chain (B2 → C2.1 → C2.2 → D2) is serial with ~150s floor before parallelization, D2 batched and at floor.
  - `repo/api/engine/data/primitives/ontology_v0.md` — 64 tags / 6 dimensions / 20-edge interaction graph / 10-pair Spellbook coverage. Source of truth for Phase 5 primitive extractor.
  - `spaces/.../memory/MEMORY.md` (index) + `project_mega_task_v1_shipped_2026-05-21.md` + `project_5_pillar_forward_plan.md` — confirms forward plan iter-4 priorities match the kickoff's phase ordering.
- key findings:
  - **Architecture entry points confirmed** for the LLM phases in `agent_build_deck_v1.py`:
    - `_run_intent_interpreter` (B2) at line 1765 — called at line 176.
    - `_run_candidate_critic` (C2.1) at line 2257 — called at line 272.
    - `_run_wild_combo_discovery` (C2.2) at line 2673 — called at line 296.
    - `_run_final_critic` (D2 batched ×3) at line 3430 — called at line 339.
    - `_run_mana_base_critique` (Pillar E v0.1) at line 3682 — called at line 418.
  - **Phase 3 outer-chain parallel plan**: C2.1 and C2.2 share identical input dependencies (iter-1 baseline deck + B2 intent_analysis + wide candidate pool). The merge step happens between C2.1's pick application and C2.2's swap evaluation — C2.1 precedence per kickoff.
  - **Phase 1 Voyage scaffolding**: `agent_semantic_retrieval_v1.py` already exposes `is_available()`, `query_neighbors()`, `build_index()` with no-op fallbacks. Iter 4 swaps the no-ops for real Voyage calls + sqlite-vec storage.
  - **Phase 5 extractor scope**: ontology_v0.md has 64 tags. Most extraction_rule lists are 2-3 regexes. Some tags (`combo-assembly`, `combat-extra-step`) have empty extraction_rule lists — they are tagged by membership in derivative datasets (Spellbook for combo-assembly; aliased to extra-combat for combat-extra-step). The extractor must handle these as named-from-other-sources tags rather than skipping them.
- next phase: Phase 1 — Voyage AI semantic retrieval activation.

---

## Phase 1 — Voyage AI semantic retrieval activation — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: ~$1.92 cumulative ($1.62 one-time Voyage index build + $0.30 Phase 1 smoke). Well clear of the $80 alarm.
- tests: pytest **1149 passed / 8 pre-existing fails** (Phase 0 baseline 1145 + 4 new). 11 tests in updated test_agent_iter3_phase_7_semantic_retrieval.py (was 7 in iter 3 scaffolding).
- self-correction events:
  - **Tier-1**: After the initial Voyage index build succeeded (30,395 cards, 169s, $1.62), a sanity-check query exposed a `color_identity` parser bug — the cards table stores `color_identity` as JSON (`["B", "R", "W"]`) and the first `_commander_legal_cards` implementation naively `split(",")` on the literal JSON string. Fixed the parser to `json.loads`, then ran a one-shot SQL update on `card_embeddings.color_identity` to repair the 30,395 already-indexed rows. No new Voyage API call required. Verified Edgar Markov / Sol Ring / Thassa's Oracle neighbor queries return semantically appropriate matches.
- key findings:
  - **Activated `agent_semantic_retrieval_v1.py`**: `build_index()` reads Commander-legal cards from the active snapshot (30,395 cards), batches them into 128-card requests to Voyage `voyage-3` ($0.18/MT), stores float32 vectors as BLOBs in `repo/api/engine/data/embeddings/card_embeddings_v1.sqlite`. Idempotent: re-running with the same snapshot + model + row count short-circuits via the `embeddings_meta` table.
  - **No sqlite-vec extension dependency**: brute-force cosine over numpy (lazy-loaded into a module-level cache) is ~50ms per top-k query on the 30k×1024-dim matrix (~120MB RAM). Avoids the extension-install complication.
  - **Index storage**: `card_embeddings(name PK, color_identity, type_line, oracle_text, cmc, released_at, vec BLOB)` + `embeddings_meta(key PK, value)`. The 141MB sqlite file lives at `repo/api/engine/data/embeddings/card_embeddings_v1.sqlite` and is properly gitignored via the existing `*.sqlite` rule.
  - **Color-identity filter** is honored: passing `color_identity_filter=["W","B","R"]` to `query_neighbors` drops candidates whose CI isn't a subset (verified in unit test + Edgar live sweep).
  - **Edgar smoke test (1-case, 144.3s wallclock, $0.294 cost)**:
    - C2.2 wide pool gained **72 semantic-neighbor cards** (target: ≥5).
    - **2 of those 72 made it into the final deck** (Elenda, the Dusk Rose + Mavren Fein, Dusk Apostle, both surfaced via C2.2 wild-combo discovery picks from the Voyage-augmented pool). Target: ≥2.
    - novel_combo_count: 10 (target ≥4).
    - Cost: $0.294 (target ≤$0.35).
    - Wallclock 144.3s — comparable to iter 3 baseline (143.3s on Edgar). Voyage queries add negligible latency (<100ms total per build).
  - **Source tagging**: when a C2.2 wild-combo pick comes from a wide-pool entry whose source was `semantic_neighbor`, the final source string gets a `|from_semantic_neighbor` suffix. This makes the Phase 7 sweep tooling able to count Voyage's contribution per case without instrumentation hacks.
  - **Module change set**:
    - `api/engine/layers/agent_semantic_retrieval_v1.py` — full activation (was no-op scaffolding).
    - `api/engine/layers/agent_build_deck_v1.py` — `from_semantic_neighbor` source-tag preservation in `_run_wild_combo_discovery`.
    - `tests/test_agent_iter3_phase_7_semantic_retrieval.py` — 11 tests (was 7) covering `is_available`/`query_neighbors`/`build_index` idempotency/`EMBEDDING_DB_PATH`.
    - `requirements.txt` — voyageai>=0.3.0 + numpy>=1.24.0.
- next phase: Phase 2 — counters-matter archetype + Phase 6 detector refinement.

---

## Phase 2 — Counters-matter archetype detector — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: ~$1.92 (Phase 2 is pure-Python — no new LLM build runs; deferred the Atraxa LLM smoke to Phase 7's 5-case sweep)
- tests: pytest **1153 passed / 8 pre-existing fails** (Phase 1 baseline 1149 + 4 new counters_matter tests).
- self-correction events: none
- key findings:
  - `agent_c22_prompt_templates_v1.ARCHETYPES` extended with `"counters_matter"`, inserted **between `aristocrats` and `control`** in the tuple so that the tribal-first priority is preserved (tribal > voltron > storm > aristocrats > counters_matter > control > combo > …).
  - Detection patterns added to `_KEYWORD_PATTERNS["counters_matter"]`: `\bproliferate\b`, `\+1/\+1 counters?`, `\bcharge counters?\b`, `\bloyalty counters?\b`, `\benergy counters?\b`, `\bcounters (matter|theme)`, `\bplus.one.plus.one\b`, `\bcounter[- ]doubling`, `\bcounters? on permanents`.
  - Per-archetype prompt fragment added to `_FRAGMENTS["counters_matter"]`: 5-bullet guidance covering counter distribution (Hardened Scales / Doubling Season), proliferate/doubling effects, counter-removal synergies, counter-scaling wincons (Walking Ballista, infect), and counter-sacrifice outlets.
  - **4 new unit tests** in `test_agent_iter3_phase_6_c22_archetypes.py`:
    - `test_atraxa_proliferate_detects_counters_matter` — passes.
    - `test_roalesk_apex_hybrid_detects_counters_matter` — passes.
    - `test_pir_toothy_detects_counters_matter` — passes.
    - `test_edgar_markov_does_not_false_positive_counters_matter` — passes (Edgar with vampire+1/+1 counter prose still detects as `tribal` because vampire keywords outscore counter keywords).
  - **Existing 18 tests unchanged** — including the iter-3 `test_atraxa_proliferate_detects_default_or_control` which only asserted `result in ARCHETYPES and result != "tribal"` — `counters_matter` satisfies both.
  - **Live Atraxa LLM smoke deferred** to Phase 7's 5-case sweep (saves ~$0.30 in development LLM spend; the detection is pure-Python and unit-tested, so the integration risk is minimal). Phase 7 success criterion 10 (`atraxa_archetype_is_counters_matter`) gates the live verification.
- next phase: Phase 3 — outer-chain parallelization [BLOCKING].

---
