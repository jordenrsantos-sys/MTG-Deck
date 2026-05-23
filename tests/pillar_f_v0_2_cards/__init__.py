"""Test suite for mega-task v11 per-card oracle compilation.

Mirrors the production package layout under
`api/engine/pillar_f/v0_2/cards/`: one test module per phase, plus
`fixtures/` for multi-card scenario tests added in Phase 9.

The v10 (LLM strategic policy) parallel arc lives in
`tests/pillar_f_v0_2_policy/`. The iter-10 substrate tests live in
`tests/pillar_f_v0_2/`. The three test trees are independent and
should not import each other.
"""
