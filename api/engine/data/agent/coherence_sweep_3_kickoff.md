# Coherence Sweep #3: Project-wide health audit after mega-tasks v1-v5

Single self-contained kickoff. You are authorized to run autonomously from start to finish without further user interaction unless a hard halt condition triggers. Self-correct using the tiered escalation. Atomic commit per phase. Maintain a running progress log throughout.

---

## What this sweep delivers

A comprehensive health audit covering **EVERYTHING IN THE PROJECT, NOT JUST WHAT MEGA-TASK v5 WORKED ON.** After 60+ commits and dozens of new modules + architectural rules across mega-tasks v1-v5, the codebase grew faster than the integration/health-check infrastructure. This sweep catches the drift before it compounds into iter 7+ work, and produces a categorized punch-list separating "fix inline now" from "queue for iter 7 mega-task v6."

**Precedent:** Coherence Sweep #1 (PILLAR C.3 in the task list, Pillar A+B+C foundation close-out) and Coherence Sweep #2 (FINAL in the task list, end of v1.0 ship) both completed in prior sessions. This is the third.

**Output deliverable:** `repo/api/engine/data/agent/coherence_sweep_3_health_report.md` with:
- Executive summary
- One section per audit area (10 total)
- Categorized punch list: "Fixed inline" / "Queued for iter 7" / "Out-of-scope / wontfix" with rationale per item

**Scope tightness:** sweep + comprehensive health report + small inline fixes for things found broken during the sweep. Larger architectural rework gets QUEUED for iter 7, not done inline. Estimated ~1-2 days CC wall-clock, ~$5-15 API spend.

Read these files at Phase 0:

1. `spaces/.../memory/project_iter_6_prep_notes_2026-05-21.md` (Coherence Sweep #3 spec — 10 audit areas defined in this file under "Coherence Sweep #3 — QUEUED for IMMEDIATE post-iter-6 dispatch")
2. `spaces/.../memory/MEMORY.md` (full memory index — needed for audit area #3 memory↔code alignment)
3. `spaces/.../memory/project_mega_task_v5_shipped_2026-05-22.md` (mega-task v5 ship state)
4. `repo/api/engine/data/agent/mega_task_v5_final_report.md` (v5 final report)
5. `spaces/.../memory/project_5_pillar_forward_plan.md` (overall roadmap)
6. `repo/api/main.py` (uvicorn entry + endpoint registration)
7. `repo/requirements.txt` (current dep declarations)

---

## Substrate state

Mega-task v5 shipped 2026-05-22 (final commit on top of v4's `e97589870`). 26 atomic commits in mega-task v5. Test baselines: pytest 1489 / vitest 758. Total CC spend across mega-tasks v1-v5: ~$18-20 ANTHROPIC_API_KEY usage.

**Inheritance from v5 you must not regress:**

- iter 6's 9/12 sweep result + the 3 known iter-7 priorities (semantic-injection guarantee, ontology v2, eval-script multi-primitive counting)
- All 26 mega-task v5 commits + their associated tests
- UX bundle (SSE streaming, auto-snapshot, cancel button, timeout, uvicorn 2-workers)
- Pillar E v0.3 curve smoother + v0.4 interaction designer
- Tiered opposition registry (54 entries across Tier 0/1/2 per bracket)
- Graduated playtest Stage 1 with UI report card
- Substrate fixes (venv 3.10 + corpus disk cache)

**Architectural rules locked in feedback memories (must be honored by current code paths):**

- Corpus is descriptive not prescriptive (`feedback_corpus_descriptive_not_prescriptive`)
- User intent locks deck shape — corpus optimum is not the target (`feedback_user_intent_locks_deck_shape_not_corpus_optimum`)
- Mana base serves spells, computed last not locked first (`feedback_mana_base_serves_spells_not_reverse`)
- Pool ranking score does not drive LLM picking (`feedback_pool_score_does_not_drive_llm_picking`)

---

## Authority and scope

You are AUTHORIZED to:

- Run all 11 phases (0-10) autonomously without halting except on hard halt conditions.
- Self-correct using the tiered escalation when a phase's audit hits unexpected gaps.
- Make atomic commits per phase: `git commit -m "Coherence Sweep #3 Phase X: <audit area>"`.
- Inline-fix small findings: <50 LOC changes, no new modules, no architectural impact, no schema changes.
- Read every file in the repo + cowork memory directory + Obsidian vault.
- Use the mtg-engine MCP for endpoint smoke tests.
- Use the obsidian MCP for vault doc verification.

You are NOT authorized to:

- Make architectural changes inline (new modules, new endpoints, schema migrations, multi-file refactors). Queue for iter 7 mega-task v6 instead.
- Modify Pillar A endpoints or `combo_brackets_v1.json`.
- Touch the Phase 5b MPA substrate.
- Roll back any commit. Forward fixes only.
- Modify iter 1-6 baseline test cases in ways that change their behavior.
- Upgrade the Anthropic model from Sonnet 4.6.
- Touch the v3 per-set automation scheduled task.

---

## Hard halt conditions (NARROW — halt only on these)

1. **Catastrophic finding** that breaks iter 6 baseline functionality. Halt immediately with full diagnosis.
2. **Critical regression**: any iter 1-6 success criterion breaks at any phase boundary. Halt with diff.
3. **Resource exhaustion**: API spend reaches $50 (sweep ceiling is lower than feature mega-tasks because most work is read-only).
4. **Cumulative test suite regression**: pytest drops below 1489 OR vitest drops below 758. Halt and diagnose.
5. **>3 inline fixes fail to land cleanly** (e.g., revert because they introduced regressions). Halt — too much breakage from "small" fixes suggests bigger underlying issues.
6. **Architectural contradiction** discovered between memory and code that fundamentally changes how the system should be understood. Halt and surface for user direction.

You do NOT halt for:

- Individual audit findings (even substantial ones) — those go in the queue-for-iter-7 punch list.
- Documentation drift (write to the health report; iter 7 fixes if substantial).
- Memory updates needed (update inline as discovered).
- Single test failures fixable on next attempt.

---

## Self-correction protocol (tiered escalation)

**Tier 1** — Re-read the audit-area spec, try alternate investigation path. Up to 3 attempts.

**Tier 2** — Search the codebase for similar patterns. Up to 2 attempts.

**Tier 3** — Mark the audit area "incomplete" in the health report with explanation; continue to next phase. Only allowed for non-blocking phases.

**Tier 4** — Halt for user direction.

**Blocking phases that cannot Tier-3-skip:**

- Phase 1 (substrate cache audit) — directly motivated by the iter 6 strength_check 111-min cold-start finding
- Phase 3 (memory↔code alignment) — foundational for trust in all subsequent work
- Phase 10 (final synthesis + health report)

Other phases are non-blocking.

---

## Progress log

Write to `repo/api/engine/data/agent/coherence_sweep_3_progress_log.md` from Phase 0 onward. Append-only, timestamped sections per phase.

---

## Resource budget

- **Total API spend ceiling: $50.** Alarm at $35; hard halt at $50. Most sweep work is read-only investigation; LLM calls only for inline fix iteration + per-pillar smoke tests.
- **Wall-clock budget**: 12-36 hours.

---

## Test discipline

After EVERY commit:

```bash
cd "E:\MTG Root\mtg-engine\repo"
pytest -q
cd "E:\MTG Root\mtg-engine\repo\ui_harness"
npm test -- --run
```

Both must pass. Baselines: pytest 1489 + new tests; vitest 758 + new tests.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read the 7 files listed in "What this sweep delivers." Confirm env (Python 3.10, ANTHROPIC_API_KEY + VOYAGE_API_KEY set, MCPs connected, pytest 1489 baseline, vitest 758 baseline, git clean, disk >5GB free).

Create `repo/api/engine/data/agent/coherence_sweep_3_progress_log.md` and `repo/api/engine/data/agent/coherence_sweep_3_health_report.md` (skeleton with sections for each audit area; populate as you go).

Commit: "Coherence Sweep #3 Phase 0: pre-flight + report skeleton".

---

### Phase 1 — Substrate cache audit (BLOCKING)

Directly motivated by iter 6's strength_check 111-min cold-start finding.

**Audit:**

Search the codebase for all in-memory caches that cost significant time to populate on cold start. Patterns to grep:
- `_CACHE = {}` or similar module-level dict caches
- `lru_cache` decorators with high maxsize
- "warm" / "lazy" / "ensure" function names
- `_load_*` or `_ensure_*` patterns that compute-and-store

For each cache found, characterize:
- What it stores
- How expensive cold-start is (estimate via row count × per-row cost)
- Whether it persists to disk
- What invalidation strategy it uses (or whether it's invalidation-broken)

**Inline fix policy:** if a cache costs >30s cold-start AND has no disk persistence AND the fix is <50 LOC, fix it inline (mirror the corpus_disk_cache pattern from iter 6). If cold-start is <30s or fix needs new architecture, queue for iter 7.

**Output:** Section 1 of health report listing all caches found + per-cache disposition.

---

### Phase 2 — Cross-pillar integration verification

**Audit:**

For each pillar (A / C / D / E / F + Track 5 per-set automation), verify the documented integration points actually work:

- **Pillar A** (9 endpoints): every endpoint loadable + responds to a sample request without error
- **Pillar C** (primitive extractor): producing tags on cards consumed by Pillar D + Pillar F per claim
- **Pillar D** (agent build_deck_v1): all 4 LLM injection points (B2/C2.1/C2.2/D2) firing; theme profile cascade through phases (per `feedback_user_intent_locks_deck_shape`)
- **Pillar E** (v0.1 mana base + v0.2 card advantage + v0.3 curve + v0.4 interaction): all 4 optimizers fire on a sample build
- **Pillar F** (v0.1 statistical approximator + Graduated playtest Stage 1): both produce sane output on a sample build
- **Track 5** (per-set automation): Scryfall watcher + ingestion + Pillar C extractor + Voyage incremental update + Pillar F archetype scoring + LLM report writer all chained correctly

**Method:** Run a single full Edgar B3 build via the streaming endpoint. Capture which phases fire + what their outputs look like. Compare against mega-task v5 final report's claimed architecture.

**Output:** Section 2 of health report — per-pillar wiring status + any drift found.

---

### Phase 3 — Memory ↔ code alignment (BLOCKING)

**Audit:**

For each of the ~17 memory entries in cowork memory directory, verify the code state matches the description:

- For "shipped" memories (project_mega_task_vN_shipped_*, project_pillar_a_c_shipped_*, etc.): does the named module/feature exist at the claimed location?
- For feedback rules: is the rule actually honored by current code? (Pick 2 lines of code touched by each rule and verify they implement what the rule says.)
- For "queued" / "prep notes" memories: are the listed items still genuinely pending, or have some quietly shipped?

**Inline fix policy:** Memory drift (description doesn't match reality) gets fixed inline. Code that doesn't honor a feedback rule gets queued for iter 7.

**Output:** Section 3 of health report — per-memory-entry alignment status + any drift discovered.

---

### Phase 4 — Test coverage gaps

**Audit:**

For each major feature shipped in mega-tasks v1-v5, check:
- Does it have unit tests? (Direct module tests)
- Does it have integration tests? (End-to-end through the agent build flow)
- Are there regression tests for known iter 4/5/6 architectural learnings?

Conversely:
- Any test files referencing removed/superseded code? (Dead tests)
- Any test files that are slow / flaky / regularly skipped?

**Output:** Section 4 of health report with two sub-lists — "Missing test coverage" (features without tests) and "Dead tests" (tests for removed code). No inline fixes here unless removing a clearly-dead test file is trivial.

---

### Phase 5 — Database + schema integrity

**Audit:**

The `cards` table has been migrated multiple times (released_at in iter 3, primitives_json in iter 4, possibly more in v5). Verify:

- All snapshots have consistent column populations (run `SELECT snapshot_id, COUNT(*), COUNT(released_at), COUNT(primitives_json) FROM cards GROUP BY snapshot_id` and report counts)
- Tagpass snapshot inheritance still works for new columns added in iter 5/6
- The active snapshot (`20260217_190902_tagpass_20260222`) is consistent across cards / cards_raw / any derived tables

Also check the corpus + opposition deck registries:
- `corpus_v1.json`: 13,408 entries per the iter 6 finding — verify still consistent
- `opposition_decks_v1.json`: 54 entries per mega-task v5 — verify all parseable

**Output:** Section 5 of health report with schema-state summary + any inconsistencies.

---

### Phase 6 — UI ↔ endpoint contract drift

**Audit:**

For every endpoint the UI calls (grep AIBuildView.tsx + any other view files for `fetch(...)`):
- Does the endpoint exist?
- Does the endpoint return the schema the UI expects?
- Are there any endpoints UI calls that aren't documented in `repo/api/main.py`?

Phase 3 SSE streaming added new contract surface in v5; verify UI consumes it correctly. Also verify the auto-snapshot endpoint from v5 Phase 2 actually returns the latest snapshot ID.

**Output:** Section 6 of health report listing UI→endpoint contracts + any mismatches.

---

### Phase 7 — Documentation drift

**Audit:**

For each module added in mega-tasks v1-v5, check its docstring describes its current behavior. For each major area:
- Pillar A: README or doc file describing the 9 endpoints — current?
- Pillar C: ontology spec at `repo/api/engine/data/primitives/ontology_v1.md` — matches actual extractor behavior?
- Pillar D: agent flow documented somewhere?
- Pillar E: per-optimizer docstrings + integration explanation
- Pillar F: approximator docstring + Stage 1 graduated playtest spec
- Track 5: per-set pipeline runbook at `repo/api/engine/data/scripts/`

Also check the Obsidian vault entries:
- `13_AI_AGENT_SURFACE/ENGINE_API_GUIDE.md` — reflects post-v5 endpoint surface?
- `15_PILOT/MPA_SPEC.md` — still accurate?
- `00_SYSTEM_CORE/DESIGN_DECISIONS.md` — reflects current architectural rules?

**Inline fix policy:** Update docstrings inline if they're 1-2 paragraph fixes. Larger doc overhauls go to iter 7 punch list.

**Output:** Section 7 of health report.

---

### Phase 8 — Orphan code detection

**Audit:**

Beyond the `build_primitive_tag_index_v0.py` dead code found earlier today, run a fresh repo-wide dead-code scan:

- Use a tool like `vulture` (pip install vulture) OR a manual grep approach
- For each `.py` file in `repo/api/engine/layers/`, `repo/api/engine/extractors/`, `repo/api/engine/integrations/`, `repo/tools/`, check if it's imported anywhere in the codebase
- Files not imported anywhere are orphan candidates

**Inline fix policy:** if an orphan file is clearly dead (no imports, no historical significance, no comments suggesting future use), recommend removal in the health report — but do NOT delete inline (per the policy that you can't delete files; that's the user's call).

**Output:** Section 8 of health report listing orphan candidates with confidence levels.

---

### Phase 9 — External-dep audit

**Audit:**

Today's venv-broken/voyageai-missing finding was a real architectural failure mode. Verify:

- Every dep in `requirements.txt` actually installs cleanly on Python 3.10 (current venv version)
- Every dep imports without error (`python -c "import <name>"` for each)
- No deps are version-pinned to releases that have been pulled / yanked
- The venv has the exact deps required for all of mega-tasks v1-v5's work (cross-reference requirements.txt against what the modules import)

Additionally:
- Check if there's a `dev-requirements.txt` or similar separate file
- Check for any pip / system-level deps not documented

**Output:** Section 9 of health report — dep health + any missing/extra deps.

---

### Phase 10 — Per-pillar smoke tests + final synthesis (BLOCKING)

**Audit:**

Each pillar gets a standalone smoke test that exercises its primary entry points end-to-end without depending on other pillars where possible:

- Pillar A: call each of the 9 endpoints with sample inputs; verify response shapes
- Pillar C: extract primitives from a sample 20-card payload; verify output format
- Pillar D: run a single Edgar B3 build via the streaming endpoint; verify completion
- Pillar E: call each of v0.1/v0.2/v0.3/v0.4 on a sample partial-deck input; verify recommendations format
- Pillar F: call statistical approximator + graduated playtest on a sample 100-card deck; verify report card format

**Inline fix policy:** smoke test failures get fixed inline if <50 LOC; otherwise queued.

**Final synthesis:** consolidate findings from all 10 audit areas into the executive summary + categorized punch list at the top of `coherence_sweep_3_health_report.md`. Three categories:

1. **Fixed inline** — small things found and fixed during the sweep
2. **Queued for iter 7 mega-task v6** — substantive findings that need real engineering
3. **Out-of-scope / wontfix** — things that look like findings but are actually intentional

**Memory update:** save `project_coherence_sweep_3_shipped_<date>.md` summarizing what shipped. Update MEMORY.md to add the index entry. Update `project_iter_7_prep_notes_2026-05-22.md` to add the "Queued for iter 7 from Coherence Sweep #3" punch list items.

**Commit:** "Coherence Sweep #3 Phase 10: synthesis + health report + memory update".

---

## Coherence Sweep #3 success criteria

The sweep is "done" when:

1. All 11 phases (0-10) committed.
2. Health report `coherence_sweep_3_health_report.md` complete with all 10 audit area sections + executive summary + categorized punch list.
3. Memory entry `project_coherence_sweep_3_shipped_<date>.md` saved + MEMORY.md indexed.
4. iter 7 prep notes updated with new queue items from the sweep.
5. pytest + vitest baselines preserved.
6. API spend under $50.
7. Any "fixed inline" items have their tests passing.

---

## What NOT to do

- Don't make architectural changes inline. Queue them.
- Don't delete files (policy boundary — recommend for user removal in health report).
- Don't run new mega-task v5-style validation sweeps; this sweep is read-only audit + small fixes.
- Don't churn memory.
- Don't pad the health report — be specific and actionable.
- Don't dispatch iter 7 mega-task v6 from inside this sweep; that's a separate dispatch after the user reviews the health report.

---

## Coherence Sweep #3 → iter 7 mega-task v6 hand-off

When the health report is complete, the user reviews + decides what from the "Queued for iter 7" punch list goes into iter 7 mega-task v6 scope. The 3 iter-7 priorities already locked (semantic-injection guarantee, ontology v2, eval-script multi-primitive counting) + Pillar E v0.5/v0.6 + Coherence Sweep #3 punch list items form the iter 7 mega-task v6 scope.

---

## You are go for launch

Run from Phase 0 to Phase 10 autonomously. Halt only on the narrow hard-halt conditions. Self-correct. Atomic commits per phase. Log progress throughout.

When you hit Phase 10's synthesis, paste the executive summary + the categorized punch list inline in your response to the user.

Expected total wall-clock: 12-36 hours. Expected total API spend: $5-15.

Begin with Phase 0 pre-flight.
