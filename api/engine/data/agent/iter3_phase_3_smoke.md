# Pillar D Iteration 2 — Validation Report

Generated: 2026-05-20 21:13:45
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 4 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5**
- ✅ **mean_creativity_delta_count_geq_8** — value `42.0` (threshold `8.0`)
- ✅ **at_least_1_novel_combo_in_any_deck** — value `8` 
- ⚠️ **rationale_substantively_different** — _Sampled rationales below — human-score each for deck-context awareness._
- ✅ **mean_cost_usd_leq_0_50** — value `0.3241` (threshold `0.5`)
- ❌ **mean_wallclock_s_leq_45** — value `138.8` (threshold `45.0`)
- ⚠️ **ur_dragon_creativity_envelope_held**

## Per-case summary

| Case | iter1 pass | wall (s) | cost ($) | LLM calls | creativity Δ | novel combos | theme coh. | must-inc resolved |
|---|---|---|---|---|---|---|---|---|
| atraxa_b2_proliferate | ✅ | 138.8 | $0.3241 | 6 | 42 | 8 | 0.50 | 2/2 |

## Per-case detail

### atraxa_b2_proliferate

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Doubling Season', 'Pir, Imaginative Rascal']`
- must-includes dropped: `[]`
- theme_coherence_score: `0.5`
- wall-clock (s): `138.8`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | False | 3305 | 0 | $0.0000 | 0.0 | INPUT_TOKEN_BUDGET_EXCEEDED |
| C2_1_candidate_critic | True | 9957 | 2619 | $0.0692 | 44.0 | - |
| C2_2_wild_combo_discovery | True | 37366 | 1134 | $0.1291 | 26.0 | - |
| D2_final_critic_batch_0 | True | 9037 | 1493 | $0.0495 | 62.3 | - |
| D2_final_critic_batch_1 | True | 8953 | 793 | $0.0388 | 16.7 | - |
| D2_final_critic_batch_2 | True | 8948 | 718 | $0.0376 | 14.8 | - |

- LLM total cost (this case): `$0.3241`
- LLM total tokens: `77566 in / 6757 out`

- creativity_delta_count (cards NOT in top-30 staples): `42`
- novel_combo_count (LLM combos with in_spellbook=false): `8`

**Novel combo flags:**
- Tekuthal, Inquiry Dominus + Atraxa, Praetors' Voice (Spellbook)  — Every end step Atraxa proliferates twice instead of once, doubling all counter advancement each turn cycle.
- Doubling Season + Brokers Ascendancy (NOVEL)  — Each upkeep Brokers Ascendancy places two +1/+1 counters on each creature instead of one, creating exponential board growth.
- Flux Channeler + Inexorable Tide (NOVEL)  — Casting any non-creature spell triggers both Flux Channeler and Inexorable Tide for two separate proliferate triggers off a single spell.
- Tekuthal, Inquiry Dominus + Tezzeret's Gambit (NOVEL)  — Tezzeret's Gambit becomes draw-2-and-proliferate-twice with Tekuthal on board, rapidly accumulating counters for four mana.
- Pir, Imaginative Rascal + Brokers Ascendancy (Spellbook)  — Brokers Ascendancy's upkeep trigger puts an additional +1/+1 counter on each creature via Pir, effectively granting +2/+2 per creature each upkeep.
- Tekuthal, Inquiry Dominus + Atraxa, Praetors' Voice (NOVEL)  — Tekuthal doubles each proliferate trigger — Atraxa's end-step proliferate now advances every counter type twice per turn cycle, including planeswalker loyalties, +1/+1 counters, and poison counters simultaneously. With Doubling Season also on board, counters placed during proliferate are doubled on top of that.
- Angelic Sleuth + Atraxa, Praetors' Voice (NOVEL) [applied as swap] — Angelic Sleuth triggers an Investigate whenever a permanent you control leaves the battlefield with counters on it. In this deck, creatures frequently carry +1/+1 counters and planeswalkers carry loyalty counters — any removal, sacrifice, or bounce of these permanents generates Clue tokens, converting natural attrition into card advantage. Notably, even Atraxa leaving (via removal or being recast from command zone) will itself generate a Clue if she had counters. Non-obvious because the payoff is a permanent leaving, not counters being placed.
- Felisa, Fang of Silverquill + Doubling Season (NOVEL) [applied as swap] — Felisa creates X Inkling tokens when a nontoken creature with counters dies, where X equals the number of counters on that creature. With Doubling Season, any creature that accumulated +1/+1 counters (via Atraxa proliferate) will have had those counters doubled, so when it dies Felisa creates double the Inklings — and Doubling Season also doubles those token creations again. A creature with just 2 counters becomes 4 counters via Doubling Season, then dies to create 4 Inklings doubled to 8. This is a non-obvious engine pairing that converts proliferate accumulation into a token army.
- Contaminant Grafter + Metastatic Evangel (NOVEL) [applied as swap] — Contaminant Grafter proliferates whenever one or more creatures deal combat damage to players. Metastatic Evangel also proliferates on combat damage. With Atraxa already proliferating at end step, this creates three independent proliferate triggers per combat turn with damage — all stacking with Tekuthal's doubling. The Corrupted bonus on Grafter (card draw if an opponent has 3+ poison) is a realistic reach in a deck that can put poison counters on players via Venser, Corpse Puppet and Bloated Contaminator. Contagious Vorrac is the weakest redundant proliferator (6-mana sorcery-speed vanilla body) and is the cleanest cut.
- Brokers Confluence + Tekuthal, Inquiry Dominus (NOVEL) [applied as swap] — Brokers Confluence can be cast choosing Proliferate three times (or a mix of Proliferate and removal/counter). With Tekuthal on board, each of those three Proliferate choices doubles, effectively giving six proliferate effects from one spell. This is a non-obvious synergy because each 'choose Proliferate' on Brokers Confluence is a separate proliferate event that Tekuthal sees individually. Fuel for the Cause requires countering a spell (reactive, situational); Brokers Confluence is proactive and can be used at instant speed politically or aggressively.

**5 rationale samples (verbatim):**
- **Doubling Season** (user_intent|llm_rationale_rewrite): _With Pir, Imaginative Rascal already adding an extra counter whenever a counter would be placed, Doubling Season stacks multiplicatively so that a single proliferate trigger from Atraxa can turn two +1/+1 counters into four or more, snowballing creatures like Bloated Contaminator and Park Heights Maverick into game-ending threats._
- **Pir, Imaginative Rascal** (user_intent|llm_rationale_rewrite): _Pir adds one extra counter on top of every counter placement, so when Doubling Season is also on board each proliferate from Atraxa or Grateful Apparition effectively triples rather than doubles the counters accumulated on planeswalkers like Vraska, Betrayal's Sting._
- **Proud Pack-Rhino** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_rationale_rewrite): _Connives on entering the battlefield and whenever it attacks, which distributes +1/+1 counters that Atraxa's end-step proliferate and Tekuthal double-trigger will compound into a very large attacker over just a few turns._
- **Carnivorous Canopy** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_rationale_rewrite): _Provides a main-phase proliferate trigger stapled to instant-speed graveyard hate that disrupts reanimator opponents, and the proliferate uptick feeds Doubling Season-amplified counters on creatures like Bloated Contaminator already in play._
- **Glistening Sphere** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_rationale_rewrite): _A three-mana artifact that taps for any color and proliferates when it enters, satisfying Atraxa's demanding WUBG cost while simultaneously adding counters on Astral Cornucopia or any planeswalker like Vraska, Betrayal's Sting._

**Summary narrative:**

> The deck's primary plan is an exponential counter engine: Atraxa proliferates at end step, Pir adds an extra counter to every placement, and Doubling Season doubles both, creating a compounding curve where planeswalkers like Vraska, Betrayal's Sting and creatures like Bloated Contaminator spiral out of reach within a few turns. Tekuthal, Inquiry Dominus serves as the critical multiplier, doubling every proliferate trigger so that even cheap spells like Contentious Plan and Whisper of the Dross perform outsized work while drawing Ezuri, Stalker of Spheres's card-draw repeatedly. A secondary threat axis leverages Felisa, Fang of Silverquill to convert counter-laden creatures that die into armies of flying Inkling tokens, rewarding the deck's natural tendency to stack counters on everything. Notable tech includes Angelic Sleuth generating Clue tokens off the constant stream of countered permanents entering the battlefield, and Metastatic Evangel providing instant-speed on-demand proliferate for as little as one mana whenever an oil-counter creature like Skrelv is in play.

**Consider adding (LLM flagged, not added):**
- `Nesting Grounds` — This land lets you move counters off Astral Cornucopia or other artifacts onto Atraxa or planeswalkers, giving the deck a free way to redistribute accumulated counters each turn without spending cards.
- `Lolth, Spider Queen` — In a deck where creatures die regularly — especially when Felisa is making Inklings that trade in combat — Lolth generates loyalty extremely fast and her ultimate produces a spider army that complements the wide-board plan.
- `Sword of Hours` — Equipping Sword of Hours to Atraxa or Bloated Contaminator adds a +1/+1 counter on hit and then the damage trigger proliferates, stacking naturally with Pir and Tekuthal without needing any other setup.

**Fallback events / LLM-layer warnings:**
- `INTENT_INTERPRETER_FAILED`: LLM call #1 (intent interpreter) failed: INPUT_TOKEN_BUDGET_EXCEEDED: Estimated input tokens 3305 exceeds budget 3000. Trim the prompt context (candidate pool, deck list) and retry.. Falling back to iteration-1 deterministic pool build.

---

## Iteration 2 → Iteration 3 hand-off

Filled in based on the actual measured behavior above. The iteration-3 work plan should start from the items below.

### Where the LLM under-performed

- _Inspect the per-case detail above; flag any phase that consistently failed (e.g. C2.2 wild-combo returning 0 flags) or under-delivered (e.g. creativity_delta < 8)._

### Which prompts need revision in iteration 3

- _Based on the rationale samples above — note any phase whose output reads template-y or generic._

### Is a model upgrade likely to help (Opus 4.6 / 4.7), or does prompt-engineering ceiling come first?

- _If the rationales are uniformly excellent and the only miss is creativity_delta or wallclock, prompt-engineering is likely the cheaper next step. If they're flat / template-y, model upgrade probably helps more._

### Is the candidate pool wide enough, or does call #2.5 need a broader pool?

- _Inspect novel_combo_count + the C2.2 latency / cost. If C2.2 is consistently returning 0 novel flags despite a 350-card pool, expand or rethink._
