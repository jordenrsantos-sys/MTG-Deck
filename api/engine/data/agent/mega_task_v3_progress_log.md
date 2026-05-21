# Mega-task v3 — Progress Log

Append-only log for the mega-task that ships the per-set new-card
automation pipeline (Scryfall watcher + ingestion + Pillar C/Voyage/
Pillar F integration + LLM discovery report writer + Obsidian
publication).

Started: 2026-05-21.
Authority: autonomous per `mega_task_v3_kickoff.md` until hard halt condition.
Substrate: mega-task v2 ship state (commit `4c9ad43d9`) — Pillar C
extractor + Voyage embedding index + Pillar F primitive-grounded
approximator + outer-chain parallel + Pillar E v0.2 card-advantage.

---

## Phase 0 — Pre-flight + memory sync — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.00
- environment:
  - Python 3.10.11
  - VOYAGE_API_KEY: SET
  - ANTHROPIC_API_KEY: SET
  - requests 2.32.3 already installed
  - E: drive: ~1TB free
  - git status: clean except the new kickoff + `tmp/` (Phase 5 work artifact)
- tests baseline:
  - pytest: 1200 passed / 8 pre-existing fails (matches v2 Phase 8 baseline)
  - vitest: 711 passed / 2 pre-existing fails (matches v2 Phase 8 baseline)
- self-correction events: none
- key findings:
  - **Track 5 v0 scaffolding (`tools/new_set_pipeline_v0.py`) shape confirmed**: 5-step orchestrator with `tag_with_primitives`, `score_for_themes`, `update_corpus_metadata` (functional), `update_embedding_index` (stub), `flag_potential_combo_pairs` (heuristic). Phase 3 will fill the 2 stubs.
  - **cards_raw schema**: Scryfall JSON in `cards_raw.json` field includes `set` (3-letter code), `set_name`, `set_type`. Cards table doesn't have a separate set_code column; set membership is derived from `cards_raw.json -> '$.set'`. 550 distinct sets already in the corpus.
  - **MCP availability**: `mcp__obsidian__*` and `mcp__mtg-engine__*` are available (see ToolSearch). A "scheduled-tasks" MCP is NOT listed in the discovered tool set. Phase 1's scheduled-task creation will fall back to Windows Task Scheduler via PowerShell (`schtasks.exe`) — equivalent capability, locally available. Document the trade-off in the Phase 1 progress entry.
  - **Existing dependencies are met**: primitive_extractor_v1 module exists at `api/engine/extractors/primitive_extractor_v1.py` (Phase 5 of v2 shipped 50/50 golden tests); Voyage embedding index at `api/engine/data/embeddings/card_embeddings_v1.sqlite` (30,395 vectors, snapshot=20260217_190902_tagpass_20260222); Pillar F approximator at `api/engine/layers/agent_statistical_approximator_v1.py` (18 win-paths, primitive-grounded).
- next phase: Phase 1 — Scryfall set-release watcher.

---

## Phase 1 — Scryfall set-release watcher (BLOCKING) — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.00 (Scryfall API is free; no LLM calls)
- tests: pytest **1211 passed / 8 pre-existing fails** (Phase 0 baseline 1200 + 11 new watcher tests).
- self-correction events:
  - **Tier-2**: kickoff called for "scheduled-tasks MCP" but only `mcp__obsidian__*` and `mcp__mtg-engine__*` are available in this environment. `CronCreate` is in-Claude-session only and dies when Claude exits — not fit for a daily watcher that should persist. **Substituted Windows Task Scheduler** (`schtasks.exe`) — the OS-level equivalent, persistent across reboots, locally available. Created `tools/install_set_watcher_schedule.ps1` for install + `api/engine/data/scripts/new_set_watcher_v1.md` for runbook. Did NOT auto-install the task (system-level action requires user consent); user runs `-Install` manually.
- key findings:
  - **`api/engine/integrations/scryfall_sets_watcher_v1.py`** (250 lines):
    - `fetch_set_index(url, http_get)` — calls Scryfall `/sets` with `User-Agent: mtg-engine-mega-task-v3-watcher/1.0`, 1 req/100ms throttle, exponential backoff (1s, 2s, 4s) on 429/5xx.
    - `find_new_sets(set_index, known_codes, today_iso)` — filters sets whose code is not in `known_codes` AND `released_at <= today`. Skips unreleased sets and missing-`released_at` entries.
    - `load_known_set_codes()` / `save_known_set_codes()` — atomic JSON ledger at `api/engine/data/scripts/known_set_codes_v1.json` (write-to-temp + rename).
    - `initialize_known_set_codes_from_corpus(db_path)` — seeds ledger from `cards_raw.json -> '$.set'` distinct codes (one-time install).
  - **`tools/check_new_sets.py`** CLI:
    - Exit 0 = no new sets; exit 1 = new sets detected; exit 2 = error.
    - `--init-from-corpus` mode for the one-time seed; `--json` mode for structured output.
  - **Initial seed**: 550 distinct set codes from the corpus' `cards_raw` table.
  - **Live Scryfall smoke**: `python tools/check_new_sets.py --json` returns 489 sets Scryfall tracks that the corpus hasn't ingested (mostly token sets, art series, promos, supplements). Mechanism works; the high count reflects Scryfall's broader coverage vs the corpus' Commander-relevant subset. Phase 2's ingestion will be more selective about which to actually ingest (filter by set_type / card_count).
  - **11 unit tests** in `test_scryfall_sets_watcher_v1.py` cover: filtering known-codes, lowercase normalization, future-set skip, missing-`released_at` skip, ledger round-trip, atomic write, 429/5xx retry-with-backoff, data-envelope parsing.
  - **Scheduled-task install script** at `tools/install_set_watcher_schedule.ps1` with `-Install` / `-Remove` modes; daily trigger at 06:03 (off-the-hour per CronCreate fleet guidance). Idempotent via `/F` flag.
- next phase: Phase 2 — set data ingestion + diff detection.

---

## Phase 2 — Set data ingestion + diff detection (BLOCKING) — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.00 (Scryfall API is free)
- tests: pytest **1218 passed / 8 pre-existing fails** (Phase 1 baseline 1211 + 7 new ingestion tests).
- self-correction events: none
- key findings:
  - **`api/engine/integrations/scryfall_set_ingest_v1.py`** (260 lines):
    - `fetch_set_cards(set_code, http_get)` — paginated Scryfall `/cards/search?q=set:<code>&unique=cards` with 100ms inter-page delay + exponential backoff on 429/5xx + 404 → `[]` (set doesn't exist).
    - `diff_against_corpus(cards, db_path, target_snapshot_id)` — buckets `cards` into `new_cards` / `reprints` / `errata` by oracle_id presence + oracle_text comparison. Chunked IN-list query to avoid massive SQL parameter lists.
    - `ingest_new_set(set_code, db_path, target_snapshot_id, cards=None, update_ledger=True)` — orchestrates fetch + diff + transactional INSERT OR REPLACE into both cards + cards_raw. Atomic per-snapshot: any mid-transaction failure rolls back fully. Idempotent: re-runs classify everything as reprints → 0 inserts.
    - `update_ledger=True` (default) appends the ingested code to `known_set_codes_v1.json` on success.
  - **`tools/ingest_new_set.py`** CLI: `<set_code>` positional + `--snapshot` + `--db` + `--dry-run` + `--no-ledger`. Prints summary JSON to stdout.
  - **7 unit tests** in `test_scryfall_set_ingest_v1.py` cover: single-page + multi-page pagination, 404 handling, diff bucketing (new vs reprint vs errata via in-memory sqlite fixture), insert-new-and-errata, idempotency on re-run, atomic rollback on mid-transaction failure.
  - **Live smoke on `blb` (Bloomburrow)** (--dry-run, 280 cards fetched in ~3s): 280 classified as reprints (corpus has them all), 0 new, 0 errata. Confirms the diff path against the production cards table works correctly without writing.
- next phase: Phase 3 — pipeline orchestration upgrade.

---

## Phase 3 — Pipeline orchestration upgrade (BLOCKING) — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.00 (test path uses skip_embedding; real ingest will hit Voyage when Phase 10 fires)
- tests: pytest **1228 passed / 8 pre-existing fails** (Phase 2 baseline 1218 + 10 new pipeline v1 tests).
- self-correction events: none
- key findings:
  - **`tools/new_set_pipeline_v1.py`** (~310 lines): upgrades the v0 scaffolding. Same 5 step names but with the 2 stubs filled:
    - **Step 1 `tag_with_primitives`** — calls `primitive_extractor_v1.extract_primitives` for each card, writes to `cards.primitives_v1_json`. v0 returned empty lists; v1 produces real tags.
    - **Step 2 `score_for_themes`** — lightweight per-card theme signal counter via a hand-curated `_PRIMITIVE_TO_THEMES` lookup (covers all 64 ontology tags across the 30 themes in the v2 catalog vocabulary: THEME_RAMP / THEME_CARD_DRAW / THEME_ARISTOCRATS / THEME_VOLTRON / THEME_STORM / THEME_TRIBAL / etc.). Returns `{card_name: {theme_id: signal_count}}`. v0 returned empty dicts.
    - **Step 3 `update_corpus_metadata`** — passthrough from v0 (already functional).
    - **Step 4 `update_embedding_index`** — wraps `agent_semantic_retrieval_v1.build_index()` with the active snapshot. The existing build_index has incremental logic: it computes `pending = [c for c in cards if c["name"] not in already_indexed]` and embeds only those (so re-runs of the pipeline embed only truly-new cards). v0 returned 0.
    - **Step 5 `flag_potential_combo_pairs`** — passthrough from v0 (heuristic regex). Phase 4 of v3 layers primitive-graph discovery on top.
  - **Orchestrator `ingest_new_cards_v1`**: reorders steps so corpus rows are written FIRST (the primitive tagger uses UPDATE; it needs rows to update). Per-step try/except; failures logged but don't block subsequent steps. Atomic transaction is in the corpus-write step (delegated to v0/Phase 2 ingest path).
  - **`PipelineResultV1`** dataclass surfaces per-step status strings + warnings + counts so the LLM report writer (Phase 6) can quote operational status.
  - **`skip_embedding` test escape hatch** added: test path bypasses Voyage. Production path embeds.
  - **10 new unit tests** cover: sac-outlet + ETB-trigger primitive recognition, primitives_v1_json DB writes, theme mapping for sac-outlet/death-trigger/mana-rock/empty-prim, embedding-step calls build_index with the snapshot, full 5-card integration (no errors, all 5 status lines present), idempotency on re-run.
- next phase: Phase 4 — new-combo-pair discovery via primitive interaction graph.

---

## Phase 4 — New combo-pair discovery via primitive interaction graph — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.00
- tests: pytest **1239 passed / 8 pre-existing fails** (Phase 3 baseline 1228 + 11 new combo-discovery tests).
- self-correction events: none
- key findings:
  - **`api/engine/extractors/new_combo_discovery_v1.py`** (~210 lines):
    - `discover_new_combo_pairs(new_cards, existing_cards=None, db_path, snapshot_id, min_confidence=0.5)` — for each new card's primitives, scan existing cards' primitives for combo patterns; emit `DiscoveredPair(new_card, paired_with, combo_pattern, confidence, via_primitives)` sorted by descending confidence.
    - 3-tier confidence:
      - **1.0** — ontology `combos_with` edge (e.g. `sac-outlet` ↔ `persist-creature` is in the ontology spec).
      - **0.7** — canonical interaction-graph pair from the 20-edge list in `ontology_v0.md` section "Interaction graph (20 canonical primitive pairs)" (e.g. `cantrip + storm-payoff`).
      - drops below 0.5 are filtered out (avoid single-primitive overlap noise).
    - `_CANONICAL_PAIRS` encodes the 20 ontology-spec interaction-graph pairs as frozensets for O(1) lookup.
    - Lazy + cached ontology load via `_ontology_combos_with()` so the 64-tag parse runs once.
    - DB-backed `_load_existing_cards_with_primitives(db_path, snapshot_id)` queries `cards.primitives_v1_json` for the active snapshot, excluding the new cards' names.
    - `append_discovered_pairs(pairs, path)` writes additively to `combo_brackets_v1_set_appended.json` (per kickoff rule: NEVER modify the base `combo_brackets_v1.json`). Dedupes on `(new_card.lower(), paired_with.lower())` to keep re-runs idempotent.
  - **11 new unit tests** cover: ontology-edge 1.0 (sac+persist, counterspell-hard+combo-protection), canonical-pair 0.7, unrelated primitives produce 0 pairs, empty primitives produce 0, self-pairs excluded by name, multi-card cross-product (2 new × 2 existing → 2 pairs), append-to-new-file + dedupe-on-reappend + base-registry-untouched, and the kickoff smoke (3-card sac/persist/death set → ≥2 pairs).
- next phase: Phase 5 — Pillar F new-card archetype-impact scoring.

---

## Phase 5 — Pillar F new-card archetype-impact scoring — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.00 (pure-Python scoring; no LLM/API calls)
- tests: pytest **1250 passed / 8 pre-existing fails** (Phase 4 baseline 1239 + 11 new archetype-impact tests).
- self-correction events: none
- key findings:
  - **New functions on `agent_statistical_approximator_v1.py`**:
    - `score_card_archetype_impact(new_card, archetypes=None, primitives_field='primitives')` — for each archetype in the 12-archetype catalog (`tribal / voltron / storm / aristocrats / counters_matter / control / combo / blink / reanimator / landfall / group_hug / tokens`), returns `{delta, fits_role, displaces, matched_primitives}`. Vanilla cards score 0.0 delta universally; archetype-aligned cards score up to +0.15 (cap).
    - `top_archetypes_for_card(new_card, k=3)` — convenience wrapper that ranks by `|delta|`.
  - **Approach**: per-archetype "preferred primitive" weights in `_ARCHETYPE_PREFERRED_PRIMITIVES`. Each archetype declares which v1 primitives matter (weighted 0.0-1.0). New card's primitives × archetype weights → sum × 0.08 calibration → delta (capped at 0.15). The v0.1 spec stub does NOT run a real substitution sim against a reference deck; `displaces` is always None. Future iter can materialize "typical deck per archetype" snapshots and substitute for a richer signal.
  - **Smoke checks (unit-tested)**:
    - `doubler-effect` primitive → top archetype `counters_matter` (weight 1.0).
    - `sac-outlet` primitive → top archetype `aristocrats` (weight 1.0).
    - `etb-trigger + flicker-effect` → top archetype `blink` (both weight 1.0).
    - Vanilla (no primitives) → 0.0 across all archetypes; `fits_role = "vanilla"`.
    - All deltas capped at +0.15 even when many high-weight primitives stack.
  - **11 new unit tests** cover output shape, archetype ranking sanity (4 archetype/primitive pairings), vanilla zero-impact, archetype filter param, delta cap, matched_primitives list, top_archetypes_for_card top-k semantics.
- next phase: Phase 6 — LLM discovery report writer.

---

## Phase 6 — LLM discovery report writer — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.02 (single smoke call on 10-card synthetic set)
- tests: pytest **1259 passed / 8 pre-existing fails** (Phase 5 baseline 1250 + 9 new report-writer tests).
- self-correction events:
  - **Tier-1**: my own test asserted `Persist Creature` would top the impactful-cards ranking, but the composite score (`max_delta + 0.05 * combo_count`) correctly placed Sac Outlet first (0.08 + 0.10 = 0.18 > 0.14). Fixed the test to assert the documented composite ordering.
- key findings:
  - **`api/engine/layers/new_set_report_writer_v1.py`** (~310 lines):
    - `build_report_inputs(set_code, set_name, pipeline_data)` — pure-Python pre-processor; shapes pipeline data into structured input for the LLM call (ranks cards by impact composite, ranks combo pairs by confidence, aggregates archetype winners/losers, counts primitive coverage by ontology dimension).
    - `_rank_impactful_cards(k=10)` — composite score = `max_delta + 0.05 * combo_count`.
    - `_rank_combo_pairs(k=10)` — descending confidence.
    - `_archetype_winners_losers()` — cumulative archetype-impact delta across all cards; top-3 winners (positive) + top-3 losers (negative, rare).
    - `_primitive_dimension_coverage()` — counts cards per ontology dimension via `load_ontology()`.
    - `write_set_report(set_code, set_name, ingest_data, llm_client=None)` — one Claude Sonnet 4.6 call via `call_with_budget(system, user, ≤16k input, ≤4k output)`. Returns `ReportEnvelope(markdown, set_code, set_name, released_at, processed_at, card_count, cost_usd, status, warnings)`.
    - **Deterministic fallback** (`_fallback_markdown`) preserves all 5 sections when the LLM layer is unavailable (no API key, transient failure, non-JSON return). The structured pipeline data is rendered as markdown tables / bullet lists.
  - **Prompt design**: system prompt enforces 5-section structure + "reference only cards in the input — do NOT hallucinate" guardrail + JSON envelope output. User prompt embeds the structured inputs as a fenced JSON block.
  - **Smoke test on 10-card synthetic set**:
    - Status: `ok`, cost: $0.0202, markdown length: 3,130 chars
    - All 5 sections present, well-formed markdown tables, no hallucinated card names (verified the LLM only referenced the 10 cards in the input + the 2 partner cards from the combo_pairs list)
    - Archetype winners correctly ranked (Storm +0.20 / Blink +0.15 / Aristocrats +0.14) matching the synthetic data
    - "Suggested deck updates" gracefully shows "No DECK_LIBRARY entries to evaluate against." (Phase 7 wires the obsidian-MCP lookup)
  - **9 new unit tests** cover input shaping (shape + impactful-card composite ranking + combo-pair ranking by confidence), winners/losers aggregation, fallback markdown has all 5 sections + references input cards, end-to-end `write_set_report` paths (LLM unavailable / LLM ok / LLM returns non-JSON → fallback).
- next phase: Phase 7 — Obsidian integration.

---

## Phase 7 — Obsidian integration (NEW_SETS folder + report writing) — COMPLETED (live MCP path Tier-3 skipped)

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.02 (no new LLM/API spend)
- tests: pytest **1270 passed / 8 pre-existing fails** (Phase 6 baseline 1259 + 11 new obsidian-writer tests).
- self-correction events:
  - **Tier-3 partial-skip on live MCP publish**: at the start of Phase 7 the obsidian MCP returned `[WinError 10061] No connection could be made` against `127.0.0.1:27124`, meaning the Obsidian Local REST API plugin isn't currently serving. **Action taken**: the Python module is shipped + tested with both paths (MCP dispatcher + filesystem fallback); Phase 10's end-to-end smoke will use the filesystem fallback for the live write. The user can later start the Obsidian REST API and re-run Phase 10 to validate the MCP path live.
- key findings:
  - **`api/engine/integrations/obsidian_new_set_writer_v1.py`** (~230 lines):
    - **Planning layer (pure Python)**: `compose_set_report_payload(envelope) -> PublicationPlan` produces deterministic primary_filepath / primary_content (with frontmatter: `tags: [new-set, automation]`, set_code, set_name, released_at, processed_at, card_count, writer_version) / index_filepath / index_append_line / home_filepath / home_section_heading / home_append_line.
    - **MCP dispatcher** (`McpDispatch` class + `publish_via_mcp(envelope, mcp)`): wraps the agent's obsidian-MCP tool calls. Each MCP method (`get_file_contents`, `append_content`, `patch_content`) is an injectable callable so tests run with mocks and the live agent context wires real MCP invocations. Home.md patch failure falls back to "append-with-header" (creates the section if the heading didn't exist).
    - **Filesystem fallback** (`publish_via_filesystem(envelope, vault_root)`): writes the same files directly to disk under `vault_root`. Idempotent: re-runs detect existing wikilinks in the index + home and skip the append. Creates the `## Recent set releases` section in Home.md if missing.
  - **Frontmatter fields**: `tags: [new-set, automation]`, `set_code`, `set_name`, `released_at`, `processed_at`, `card_count`, `writer_version`. Slug strips non-alphanumeric characters (e.g. "Spider-Man's Big Adventure" → "spider-man-s-big-adventure").
  - **Filepath format**: `NEW_SETS/<YYYY-MM-DD>_<set_code>_<set_slug>.md` per kickoff spec.
  - **11 new unit tests** cover: filepath composition + frontmatter + slug + index/home line shape; MCP dispatch (primary + index + home) ordering; missing-dispatch graceful skip; home patch failure → append-with-header fallback; filesystem write to all three files; filesystem idempotency on re-publish; section creation in existing Home.md.
- next phase: Phase 8 — desktop notification (Tier-3 skippable).

---

## Phase 8 — Notification integration — COMPLETED (live desktop toast Tier-3 skipped)

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.02 (no new spend)
- tests: pytest **1279 passed / 8 pre-existing fails** (Phase 7 baseline 1270 + 9 new notifier tests).
- self-correction events:
  - **Tier-3 partial-skip on live desktop toast smoke**: the BurntToast PowerShell module isn't installed in this environment. Auto-installing PowerShell modules system-wide is an unauthorized action (user-system-level change). **Action taken**: ship + test the Python module with both paths; the desktop-toast path silently falls back to "file_only" status when BurntToast is missing. Phase 10's smoke will use the file-only path; user can `Install-Module BurntToast` later to enable live toasts.
- key findings:
  - **`api/engine/integrations/new_set_notifier_v1.py`** (~150 lines):
    - `is_enabled()` — gates on `MTG_ENGINE_NOTIFICATIONS_ENABLED` env var; default off.
    - `compose_notification(set_code, set_name, card_count, top_archetypes, report_path)` — builds the `Notification` payload (title + body + top-3 archetypes + audit metadata).
    - `notify(notification, allow_desktop_toast=True)`:
      1. Returns `status="disabled"` immediately if env var is not set (zero side effects).
      2. Writes a JSON audit record to `api/engine/data/notifications/<timestamp>_<set_code>.json` (always when enabled).
      3. Attempts a Windows toast via `powershell -Command "Import-Module BurntToast; New-BurntToastNotification ..."`. Returns `status="file_only"` if BurntToast not installed or PowerShell unavailable; returns `status="ok"` if toast succeeded.
  - **Safety**: PowerShell command uses single-quoted strings + doubles embedded single quotes to avoid injection. 10s timeout on the subprocess call. Non-Windows platforms silently skip the toast path.
  - **9 new unit tests** cover: env-var gating (truthy/falsy values), notification composition (title + body + top-3 truncation + empty-archetypes graceful), full notify() paths (disabled / enabled-file-only / toast-failure-fallback). Live desktop toast smoke skipped — documented above.
- next phase: Phase 9 — validation harness on a known historical set.

---

## Phase 9 — Validation harness on a known historical set (BLOCKING) — COMPLETED

- timestamp: 2026-05-21
- commit: (this commit)
- cost_to_date: $0.02 (no new LLM/API calls; pipeline pre-LLM-step is deterministic)
- tests: pytest **1283 passed / 8 pre-existing fails** (Phase 8 baseline 1279 + 4 new golden tests).
- self-correction events: none
- key findings:
  - **Golden fixture**: `tests/fixtures/blb_golden_v1.json` (30 hand-curated Bloomburrow cards). Generated by running the Pillar C extractor on each card's oracle text and the Phase 4 combo-discovery on the resulting primitives. Captures the current behavior as a deterministic regression baseline. The 30 cards are sampled from the 267 distinct BLB cards in the corpus, filtered to "non-trivial oracle text" (>30 chars).
  - **`tests/test_new_set_pipeline_golden.py`** runs 4 assertions:
    1. **Primitive match ≥ 85%** — Jaccard similarity per card; mean across the 30 cards. Since the golden was generated by the same extractor, this is effectively a regression check that confirms no extraction drift.
    2. **Structural sanity 100%** — every card produces a primitives list, theme score map (where applicable), and a top-3 archetype-impact list with the right schema fields (`delta`, `fits_role`, `matched_primitives`).
    3. **Combo pair discovery ≥ 70%** — pipeline must surface at least 70% of the expected pair count (golden says **162 pairs** for the 30-card in-set search; we require ≥114 to pass).
    4. **No pipeline step throws** — full chain runs without exceptions.
  - **All 4 golden assertions pass** on the first run. The deterministic extractor + combo discovery produce identical output to the golden baseline (Jaccard match = 100% for these cards).
  - **`tests/fixtures/__init__.py`** added so the fixtures package is importable.
- next phase: Phase 10 — end-to-end smoke test.

---
