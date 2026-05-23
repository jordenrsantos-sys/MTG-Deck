# Mega-task v11 — per-card oracle compilation seed — progress log

Running ledger for the v11 arc dispatched 2026-05-23. Companion to
`mega_task_v11_kickoff.md`. One entry per phase ship with the actual
work + the contents-of-commit pointers (since v10's parallel commits
sometimes bundle v11 work into v10 commits).

---

## Phase 0 — Pre-flight + corpus pull + categorization

**Shipped commit:** `d635a249f` (2026-05-23 19:20)
**Bundling note:** clean — all 19 files in this commit are v11 cards/ +
tests/pillar_f_v0_2_cards/ work.

What landed:
  - `api/engine/pillar_f/v0_2/cards/` package — 9-subpackage scaffold
    (simple/, etb/, ltb/, activated/, continuous/, replacement/,
    triggered/, spell/, complex/). Each subpackage's `__init__.py`
    documents its phase responsibility.
  - `cards/_meta/build_top_500.py` — corpus pull + Scryfall enrichment
    + per-card regex categorization into the 9 buckets. Run from repo
    root: `python api/engine/pillar_f/v0_2/cards/_meta/build_top_500.py`.
  - `cards/_meta/top_500_edh_cards.json` — generated ranked top-500
    list. Schema: `{rank, name, oracle_id, usage_rate, deck_count,
    handler_type, mana_cost, cmc, type_line, colors, color_identity,
    keywords, power, toughness, loyalty, oracle_text, archetype_hint,
    primary_function}`.
  - `cards/_meta/_categorization_overrides.json` — per-card bucket
    overrides for cards the regex misclassifies.
  - `cards/simple/basic_lands.py` — registers
    `basic_tap_W/U/B/R/G/C` resolvers via the substrate's
    `register_resolver` API. `BASIC_LAND_RESOLVERS` map provides
    name → resolver lookup for 5 colors + colorless + snow basics
    (11 entries).
  - `cards/simple/mana_dorks.py` — Llanowar Elves, Birds of Paradise,
    Noble Hierarch, Avacyn's Pilgrim wired to the same resolver
    pattern. Any-color stub defaults to colorless (iter-11+ will
    plumb the LLM choice).
  - `tests/pillar_f_v0_2_cards/test_phase0_scaffold.py` — 9 smoke
    tests: imports clean, resolvers registered, basic-land tap-mana
    actually mutates the controller's mana pool through the substrate's
    push_to_stack→resolve_top flow, top-500 metadata parses.

Baseline gate:
  - pytest 1852 → 1899 (+9 mine + 38 from v10 concurrent commits)
  - pillar_f_v0_2 substrate suite: 224/224 — substrate NOT modified
  - vitest 774 + 2 pre-existing fails (metricPillHeader source-grep
    drift documented in v9 final report)
  - parent commit confirmed: 955f3c3fc (v9 ship)

---

## Phase 1 — Simple permanents

**Shipped commit:** files bundled into v10's `e124aca13` commit
("Phase 4 (mega-task v10)") at 2026-05-23 19:26.
**Bundling note:** v10's parallel commit appears to have used
`git add -A` (or equivalent) which swept in my staged v11 Phase 1
files. Files that landed in `e124aca13` from the v11 arc:
  - `api/engine/pillar_f/v0_2/cards/_meta/_categorization_overrides.json` (expanded ~60 overrides)
  - `api/engine/pillar_f/v0_2/cards/_meta/top_500_edh_cards.json` (regenerated)
  - `tests/pillar_f_v0_2_cards/test_phase1_simple.py` (19 new tests)

The content is correct + tests green. Attribution-only issue. This
progress log is the explicit v11 record so the bundling doesn't
disappear in history.

What landed (functional summary):
  - `tests/pillar_f_v0_2_cards/test_phase1_simple.py` — 19 per-card
    unit tests covering:
    * 5 in-top-500 basic lands tap-for-correct-color via the
      substrate's push_to_stack → resolve_top flow.
    * Wastes + snow-covered basics (registered Phase 0; verified here).
    * 6 in-top-500 mana dorks (Llanowar Elves, Birds of Paradise,
      Elvish Mystic, Fyndhorn Elves, Avacyn's Pilgrim, Arbor Elf).
    * Any-color dorks default to C when no color specified; honor
      explicit choice when caller picks. Documents the iter-11+ LLM
      choice plumbing point.
    * Multi-permanent sequencing (2× Forest + 1× Plains → 2 G + 1 W).
    * Cross-controller credit (Forest controlled by P2 → P2's pool).
    * Coverage gates: every top-500 basic-land + mana-dork name has a
      registered resolver.

  - Expanded `_categorization_overrides.json` — moved ~60 cards out
    of the regex's `simple` bucket where they were misclassified
    (Lightning Greaves → activated, Medallions → continuous, fetches
    → activated, Bolas's Citadel → complex, Urborg → continuous, etc.).

  - Re-ran `build_top_500.py`. New histogram:
    * activated   199 (was 168)
    * spell       139 (unchanged)
    * triggered    66 (unchanged)
    * complex      42 (was 30)
    * etb          21 (was 19)
    * continuous   19 (was 3)
    * replacement   9 (was 8)
    * simple        5 (was 68; now just the 5 basic lands)

Gate observations:
  - Phase 0 + Phase 1: 28/28 v11 cards-tests green
  - pillar_f_v0_2 substrate suite: 224/224
  - Full pytest after Phase 1: 1940 passed
  - vitest: 774 + 2 pre-existing fails (unchanged)

Phase 1 spec note:
  - Kickoff sized Phase 1 at "~50 cards (~2 days)" assuming the top
    500 would include vanilla creatures. The corpus shows the top 500
    skews to activated/continuous/triggered effects; truly vanilla
    creatures aren't in the top 500. Simple bucket therefore covers
    only the 5 basic lands.
  - Mana dorks (Llanowar, Birds, etc.) live in the activated bucket
    but the resolvers are registered in `cards/simple/mana_dorks.py`
    per the substrate convention of grouping closely-related
    primitives.

---

## Coordination note for parallel v10 arc

To minimize future bundling: per CLAUDE.md instructions, stage by
explicit file paths (`git add <file> <file>`) rather than `git add -A`
or `git add .`. The repo-wide stage sweeps in v10 commits.

If v11 work lands in a v10 commit again, append to this log noting the
commit hash + file list so the v11 contributions don't disappear in
history attribution.
