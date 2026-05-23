# Mega-task v10 / iter 11 / Pillar F v0.2 sub-mega-task B — progress log

Iter 11 dispatch on top of v9 ship (`955f3c3fc`). Kickoff:
`mega_task_v10_kickoff.md`. Sub-mega-task B only — LLM strategic policy
plugged into iter-10 substrate. Sub-mega-task C (Stage 2 playtest)
dispatches separately in iter 12+.

**Parallel-arc note:** v11 (per-card oracle compilation seed) ships in
parallel in a separate CC instance. v11 modifies the iter-10 substrate's
effect registries (per-card replacement/layer/activated-ability handlers).
v10 (this task) does NOT modify the substrate — only plugs into existing
callback hooks. Disjoint module trees: v11 lives in
`pillar_f/v0_2/cards/`, v10 in `pillar_f/v0_2/policy/`.

Append-only, timestamped sections per phase.

---

## Phase 0 — Pre-flight + scoping read + module scaffold (2026-05-23)

**Substrate snapshot:**
- HEAD: `955f3c3fc` (v9 Phase 9 — SHIPPED).
- pytest baseline: 1852 / 25 skipped / 0 failed (v9 ship; verified
  re-run of `pillar_f_v0_2/` = 224/224 green).
- vitest baseline: 774 (unchanged from v9 ship).
- ANTHROPIC_API_KEY: SET.
- LLM client: `api/engine/layers/agent_llm_client_v1.py` provides
  `AnthropicClient` with `call_with_budget(system, user, max_input_tokens,
  max_output_tokens)` → `CallResult(ok, text, parsed_json, input_tokens,
  output_tokens, cost_usd, latency_ms, ...)`. Reusable for sub-B.

**Scoping doc read:** `MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_b_llm_policy_scoping.md`
(10 sections covering compact_view, 4 prompt types, politics schema,
threat-vector, cost guardrails, integration plug-points).

**Module scaffold created** at `api/engine/pillar_f/v0_2/policy/`:
```
policy/
  __init__.py        — POLICY_VERSION constant
  prompts/__init__.py
  parsers/__init__.py
  politics/__init__.py
  cost/__init__.py
```
Plus `tests/pillar_f_v0_2_policy/__init__.py` for sub-B test tree.

**Substrate verification:** iter-10's `pillar_f/v0_2/` untouched. Sub-B
work confined to `pillar_f/v0_2/policy/` (new tree).

**Phase 0 cost:** $0 (no LLM calls).

**Commit message:** "Phase 0 (mega-task v10): pre-flight + Pillar F v0.2 policy scaffold".
