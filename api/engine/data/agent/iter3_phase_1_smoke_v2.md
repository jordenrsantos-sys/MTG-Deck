# Pillar D Iteration 2 — Validation Report

Generated: 2026-05-20 20:41:04
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 4 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5**
- ✅ **mean_creativity_delta_count_geq_8** — value `35.0` (threshold `8.0`)
- ✅ **at_least_1_novel_combo_in_any_deck** — value `6` 
- ⚠️ **rationale_substantively_different** — _Sampled rationales below — human-score each for deck-context awareness._
- ✅ **mean_cost_usd_leq_0_50** — value `0.2644` (threshold `0.5`)
- ❌ **mean_wallclock_s_leq_45** — value `176.6` (threshold `45.0`)
- ⚠️ **ur_dragon_creativity_envelope_held**

## Per-case summary

| Case | iter1 pass | wall (s) | cost ($) | LLM calls | creativity Δ | novel combos | theme coh. | must-inc resolved |
|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ✅ | 176.6 | $0.2644 | 4 | 35 | 6 | 1.00 | 2/2 |

## Per-case detail

### edgar_b3_vampire_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Vito, Thorn of the Dusk Rose', 'Bloodthirsty Conqueror']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `176.6`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 610 | 1261 | $0.0207 | 27.6 | - |
| C2_1_candidate_critic | True | 5729 | 3004 | $0.0622 | 58.7 | - |
| C2_2_wild_combo_discovery | True | 33500 | 1082 | $0.1167 | 23.3 | - |
| D2_final_critic | True | 6016 | 3111 | $0.0647 | 63.1 | - |

- LLM total cost (this case): `$0.2644`
- LLM total tokens: `45855 in / 8458 out`

- creativity_delta_count (cards NOT in top-30 staples): `35`
- novel_combo_count (LLM combos with in_spellbook=false): `6`

**Novel combo flags:**
- Vito, Thorn of the Dusk Rose + Malakir Bloodwitch (Spellbook)  — Malakir Bloodwitch's ETB drains each opponent and gains you life equal to the total lost; Vito then converts that life gain into equal damage to any target, effectively doubling Malakir's drain in one shot.
- Edgar Markov + Markov Baron (NOVEL)  — Every vampire cast generates two bodies — Edgar's 1/1 eminence token and Markov Baron's 2/2 vampire token — doubling board presence from each spell without any additional mana investment.
- Skullclamp + Bloodthrone Vampire (Spellbook)  — Equip Skullclamp to an eminence 1/1 token (making it 1/0 and sacrificing it), draw two cards, and Bloodthrone gets +2/+2 — the sac fuels both card draw and a growing threat simultaneously.
- Sorin, Imperious Bloodlord + Bloodthirsty Conqueror (Spellbook)  — Sorin's -3 puts Bloodthirsty Conqueror directly onto the battlefield from hand for free, bypassing its high mana cost and immediately presenting a massive threat backed by all the eminence tokens already in play.
- Welcoming Vampire + Edgar Markov (Spellbook)  — Each vampire cast triggers Edgar's eminence ability from the command zone creating a 1/1 token; Welcoming Vampire then draws a card off that token's entry (since it's power 1), turning every vampire spell into a cantrip.
- Mirkwood Bats + Anje, Maid of Dishonor (NOVEL) [applied as swap] — Anje creates a Blood token whenever a Vampire enters (once per turn), and Mirkwood Bats pings each opponent whenever any token is created or sacrificed. With Edgar's eminence flooding the board with 1/1 tokens every time you cast a vampire, Mirkwood Bats converts every eminence trigger into incremental drain. Combine with Blood Artist / Sanctum Seeker for a layered damage-on-entry engine that doesn't require attacking.
- Archangel of Thune + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Vito converts any lifegain into opponent life loss; Archangel of Thune puts a +1/+1 counter on every creature you control whenever you gain life. With a wide vampire board and any lifelink attacker (e.g., Legion Lieutenant grants +1/+1 to all vampires, many of which already have lifelink), a single combat step triggers Archangel repeatedly, pumping the entire army. The resulting massive Shared Animosity alpha strike becomes exponentially larger, and Vito drains opponents on every lifelink trigger along the way.
- Ayara, First of Locthwain + Blood Artist (NOVEL) [applied as swap] — Ayara drains each opponent 1 life and gains you 1 life each time a black creature enters under your control. Edgar's eminence tokens are black, so every vampire cast triggers both Ayara AND Blood Artist simultaneously. This stacks with Sanctum Seeker to create a triple-layered drain engine off each attack. Ayara's sac outlet also turns board-stall tokens into card draws, solving the deck's card advantage problem.
- Elenda's Hierophant + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Elenda's Hierophant grows a +1/+1 counter for every life gained, and when she dies she creates X 1/1 lifelink Vampire tokens equal to her power. With Vito turning lifegain into drain, any lifelink trigger grows Elenda, and when she dies in a wrath or chump-block scenario she floods the board with lifelink vampires — each of which triggers Vito again on their attacks. This creates a self-reinforcing loop between combat lifegain, Vito drain, and Elenda growth that doesn't require infinite mana.
- Vito, Thorn of the Dusk Rose + Sanctum Seeker (NOVEL)  — Sanctum Seeker drains each opponent for each attacking vampire and gains you that much life; Vito then converts every point of life gained into an equal amount of damage dealt to target opponent. In a wide attack with even 5 vampires, Sanctum Seeker drains 5 from each opponent (up to 15 life gained in a 4-player game) and Vito deals that 15 damage to one opponent, potentially killing them outright without needing combat damage to connect.

**Intent analysis (LLM call #1):**

- likely_win_condition: `"Flood the board with Edgar Markov's eminence-generated vampire tokens, drain opponents incrementally through Vito's lifegain-to-damage conversion and Sanctum Seeker-style effects, then close the game with an Shared Animosity-boosted alpha attack featuring a massive Bloodthirsty Conqueror at its center."`
- implicit_themes: `['Lifegain as a resource and trigger (Vito rewards life gained from lifelink tokens)', "Wide token generation (Edgar's eminence ability rewards casting vampires to flood the board)", 'Incremental life loss / drain effects (bleeding opponents out rather than single-burst kills)', 'Aristocrats-lite sacrifice and ETB value (vampires entering repeatedly to trigger eminence and payoffs)', 'Anthem and power scaling (buffing a wide board of small vampire tokens to close games)']`
- suggested_extensions:
  - `Sanctum Seeker` — Drains each opponent for each attacking vampire, directly amplifying the drain plan signaled by Vito and feeding Bloodthirsty Conqueror's counter requirement simultaneously.
  - `Cordial Vampire` — Puts +1/+1 counters on all your vampires whenever any creature dies, rewarding the wide Edgar token board and growing Bloodthirsty Conqueror further without extra mana.
  - `Shared Animosity` — Gives every attacking vampire a power boost for each other attacking vampire, converting the wide Edgar token swarm into a lethal alpha strike at B3 speed.
  - `Reconnaissance` — Lets all vampires attack safely and then untap after dealing damage, protecting the wide board while still triggering Bloodthirsty Conqueror's life-loss counter condition every turn.
  - `Twilight Prophet` — Provides repeated life gain (fueling Vito) and permanent card advantage once you achieve city's blessing with a wide token board, fitting both the lifegain and board-wide themes.
  - `Indulgent Aristocrat` — A one-drop vampire that lets you sacrifice a vampire to buff the whole team, bridging the aristocrats-lite signal with the anthem theme for minimal mana investment.
- conflict_warnings: `["Bloodthirsty Conqueror's counter scaling relies on opponents losing life before it enters; in multiplayer stalls where combat is stalemated early, it may enter disappointingly small — ensure sufficient haste enablers or non-combat damage sources.", 'B3 bracket assumes some interaction and value engines; a purely creature-aggro vampire build with minimal removal or disruption may struggle against decks with board wipes, making redundant threat density and recursion important to include.']`

**5 rationale samples (verbatim):**
- **Bloodthirsty Conqueror** (user_intent|llm_rationale_rewrite): _After Shared Animosity is on board and your token army has been buffed by Legion Lieutenant, Bloodthirsty Conqueror attacks as the largest creature in the swarm, doubling the damage dealt to finish a game the drain effects softened open._
- **Vito, Thorn of the Dusk Rose** (user_intent|llm_rationale_rewrite): _Every point of life your lifelink vampires — Vampire Cutthroat, Stromkirk Noble, the entire swarm under Stromkirk Captain — drain from opponents gets converted into an additional equal loss, effectively doubling the damage output of any combat step where your board connects._
- **Cordial Vampire** (theme:TYPAL_VAMPIRES|llm_intent_extension): _Theme 'TYPAL_VAMPIRES' signal_count=2 (freq_in_corpus=0.82). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=creature]_
- **Blood Petal Celebrant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _A one-mana vampire that triggers Edgar's eminence, creates a Blood token for Mirkwood Bats to ping off, and can sacrifice that Blood to rummage into another vampire or the key combo piece you need next._
- **Carrier Thrall** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _When Carrier Thrall dies to a wrath or sacrifice outlet like Viscera Seer, it creates a 2/2 Eldrazi Scion that serves as a second creature for Skullclamp or a surprise blocker, giving your sacrifice package a built-in death trigger payoff._

**Summary narrative:**

> The primary plan is to flood the board with vampire bodies through Edgar Markov's eminence ability — casting cheap one- and two-mana vampires to generate a parallel army of free 1/1 tokens — then drain opponents to death through Sanctum Seeker, Vito, and Blood Artist effects that convert a wide board into multiplied life loss each turn. A secondary plan uses the lifegain generated by an army of lifelink vampires (Dusk Legion Sergeant grants the tokens lifelink, Archangel of Thune stacks counters off every trigger) to grow the board exponentially while Vito converts every point of life gained into direct opponent damage, creating a dual-axis kill through both combat and drain. Notable tech includes Skullclamp on the 1/1 eminence tokens creating a continuous draw engine that replenishes the hand faster than opponents can wrath the board, and Ayara, First of Locthwain silently triggering on each black creature ETB to stack additional drain on top of Sanctum Seeker — meaning a single casting turn with three cheap vampires can drain each opponent for five or more life before attacks even begin.

**Consider adding (LLM flagged, not added):**
- `Reconnaissance` — Lets every attacking vampire trigger Sanctum Seeker's drain in the declare attackers step and then untap before damage — including Edgar himself — effectively granting pseudo-vigilance to your entire wide board while still collecting the combat trigger.
- `Mirror Entity` — With a wide token board built through eminence, pumping Mirror Entity to make every creature a large vampire gives your swarm a surprise lethal combat step that opponents won't see coming from a tribal token strategy.
- `Bloodlord of Vaasgoth` — Whenever you cast a vampire spell — already rewarded by Edgar's eminence — Bloodlord of Vaasgoth places a bloodthirst counter on the next vampire you cast, letting even small creatures like Pulse Tracker enter as outsized threats and accelerating the board's power scaling.

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
