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
