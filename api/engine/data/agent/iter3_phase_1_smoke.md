# Pillar D Iteration 2 — Validation Report

Generated: 2026-05-20 20:37:28
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 4 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5**
- ✅ **mean_creativity_delta_count_geq_8** — value `35.0` (threshold `8.0`)
- ✅ **at_least_1_novel_combo_in_any_deck** — value `7` 
- ⚠️ **rationale_substantively_different** — _Sampled rationales below — human-score each for deck-context awareness._
- ✅ **mean_cost_usd_leq_0_50** — value `0.2471` (threshold `0.5`)
- ❌ **mean_wallclock_s_leq_45** — value `146.0` (threshold `45.0`)
- ⚠️ **ur_dragon_creativity_envelope_held**

## Per-case summary

| Case | iter1 pass | wall (s) | cost ($) | LLM calls | creativity Δ | novel combos | theme coh. | must-inc resolved |
|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ✅ | 146.0 | $0.2471 | 4 | 35 | 7 | 1.00 | 2/2 |

## Per-case detail

### edgar_b3_vampire_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Vito, Thorn of the Dusk Rose', 'Bloodthirsty Conqueror']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `146.0`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 610 | 1103 | $0.0184 | 24.2 | - |
| C2_1_candidate_critic | True | 5689 | 2800 | $0.0591 | 50.1 | - |
| C2_2_wild_combo_discovery | True | 33483 | 912 | $0.1141 | 17.8 | - |
| D2_final_critic | True | 5997 | 2500 | $0.0555 | 50.0 | - |

- LLM total cost (this case): `$0.2471`
- LLM total tokens: `45779 in / 7315 out`

- creativity_delta_count (cards NOT in top-30 staples): `35`
- novel_combo_count (LLM combos with in_spellbook=false): `7`

**Novel combo flags:**
- Vito, Thorn of the Dusk Rose + Bloodthirsty Conqueror (Spellbook)  — Bloodthirsty Conqueror drains opponents and creates tokens; each drain triggers Vito to drain again, which triggers Conqueror again in a chain that escalates damage to all opponents simultaneously.
- Edgar Markov + Skullclamp (Spellbook)  — Edgar's eminence creates 1/1 vampire tokens; equipping Skullclamp to a 1/1 immediately kills it and draws two cards, turning every vampire spell into a cantrip.
- Sanctum Seeker + Captivating Vampire (NOVEL)  — With five or more vampires in play, attack to drain with Sanctum Seeker and steal a blocker with Captivating Vampire, creating an incremental lock on combat.
- Malakir Bloodwitch + Vito, Thorn of the Dusk Rose (Spellbook)  — Malakir ETB drains each opponent equal to your vampire count; Vito doubles that damage as a triggered life-gain conversion, potentially killing multiple opponents on entry.
- Cordial Vampire + Viscera Seer (NOVEL)  — Sacrifice a vampire to Viscera Seer to scry 1; Cordial Vampire puts a +1/+1 counter on every vampire, then repeat to grow the entire board while sculpting draws.
- Mirkwood Bats + Bloodthirsty Conqueror (NOVEL) [applied as swap] — Every vampire token Edgar and Bloodthirsty Conqueror create also triggers Mirkwood Bats ('whenever you create or sacrifice a token, each opponent loses 1 life'), turning the flood of 1/1 eminence tokens into a scalable drain engine that layers on top of Vito.
- Ayara, First of Locthwain + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Every black creature (virtually the entire vampire army) entering triggers Ayara to drain 1 life per opponent and gain 1 life. That life gain then triggers Vito to deal 1 damage to each opponent simultaneously. With a wide board of tokens entering, this creates a rapid doubling drain-loop on each cast/token creation step.
- Vein Ripper + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Vein Ripper drains 2 life per creature death (any creature) and gains you 2 life. That life gain triggers Vito to deal 2 damage to a chosen opponent. In a board state where vampires trade or are sacrificed (e.g. via Viscera Seer), each death becomes a 4-damage swing (2 drain + 2 Vito) to one opponent, enabling rapid kills.
- Elenda's Hierophant + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Elenda's Hierophant grows a +1/+1 counter for each life gained, then on death spawns X lifelink Vampire tokens equal to its power. With Vito out, each lifelink token that deals combat damage creates a life-gain trigger that Vito converts to damage — and the hierophant itself grew from those life gains, making the death payload exponentially larger the longer Vito was active.
- Vito, Thorn of the Dusk Rose + Sanctum Seeker (NOVEL)  — Sanctum Seeker drains each opponent 1 life (and gains you 1 life) for each attacking vampire. With even 5 vampires attacking, that is 5 life gained, which Vito converts to 5 damage to each opponent simultaneously — functionally doubling Sanctum Seeker's clock on a wide swing.

**Intent analysis (LLM call #1):**

- likely_win_condition: `'Edgar Markov floods the board with vampire tokens via eminence and Bloodthirsty Conqueror, then swings wide to gain massive amounts of life through lifelink, which Vito converts into lethal damage to all opponents simultaneously.'`
- implicit_themes: `['Life gain as a resource and win condition, given both Vito and Bloodthirsty Conqueror reward gaining life', "Go-wide vampire token strategy enabled by Edgar Markov's eminence ability and Bloodthirsty Conqueror's token production", "Drain and bleed as an alternate win condition through Vito's triggered ability plus the life-gain loop", 'Lifelink matters — equipping or granting lifelink to the wide board turns every attack into fuel for both payoff cards']`
- suggested_extensions:
  - `Sanctum Seeker` — Triggers a drain-life effect for each attacking vampire, synergizing directly with the go-wide token strategy and feeding Vito's life-gain payoff simultaneously.
  - `Cordial Vampire` — Grows every vampire on your board whenever any creature dies, rewarding the token-flood plan and turning board wipes into a temporary buff.
  - `Vampire Nocturnus` — Grants flying and +2/+1 to all vampires when the top card is black, dramatically increasing the combat threat of a wide token board.
  - `Indulgent Aristocrat` — Cheap vampire with lifelink that can sacrifice tokens to pump the whole team, generating life for Vito and Bloodthirsty Conqueror while enabling sacrifice synergy.
  - `Reconnaissance` — Lets you attack with the full vampire horde, trigger Edgar's eminence tokens, and then untap creatures before damage to protect them — a high-value engine in any Edgar build.
  - `Vanquisher's Banner` — Names Vampire to draw a card for every vampire cast, sustaining hand advantage in the creature-dense go-wide strategy while also buffing the team.
- conflict_warnings: `['Bloodthirsty Conqueror is a relatively recent card; confirm it is legal and printed with that exact name, as it may appear in supplemental sets with varying legality.', 'Running Coat of Arms at B3 can occasionally backfire in a multi-tribal pod if another player is also running a creature-type-matters deck — consider Shared Animosity as a safer tribal anthem that only buffs your attackers.']`

**5 rationale samples (verbatim):**
- **Bloodthirsty Conqueror** (user_intent): _User must_include_cards (locked, score=INF). [slot=creature]_
- **Vito, Thorn of the Dusk Rose** (user_intent): _User must_include_cards (locked, score=INF). [slot=creature]_
- **Cordial Vampire** (theme:TYPAL_VAMPIRES|llm_intent_extension): _Theme 'TYPAL_VAMPIRES' signal_count=2 (freq_in_corpus=0.82). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=creature]_
- **Blood Petal Celebrant** (theme:TYPAL_VAMPIRES): _Theme 'TYPAL_VAMPIRES' signal_count=3 (freq_in_corpus=0.00). [slot=creature]_
- **Carrier Thrall** (theme:TYPAL_VAMPIRES): _Theme 'TYPAL_VAMPIRES' signal_count=3 (freq_in_corpus=0.00). [slot=creature]_

**Fallback events / LLM-layer warnings:**
- `FINAL_CRITIC_INVALID_JSON`: LLM call #3 returned non-JSON output; per-card reasons unchanged. Raw text head: '```json\n{\n  "card_rationales": [\n    {\n      "card": "Edgar Markov",\n      "reason": "The engine of the entire deck — every vampire cast (including cheap one-drops like Vicious Conquistador and Ichor '

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
