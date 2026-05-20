# Pillar D Iteration 2 — Validation Report

Generated: 2026-05-20 19:16:13
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 4 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5**
- ✅ **mean_creativity_delta_count_geq_8** — value `35.0` (threshold `8.0`)
- ✅ **at_least_1_novel_combo_in_any_deck** — value `7` 
- ⚠️ **rationale_substantively_different** — _Sampled rationales below — human-score each for deck-context awareness._
- ✅ **mean_cost_usd_leq_0_50** — value `0.2741` (threshold `0.5`)
- ❌ **mean_wallclock_s_leq_45** — value `196.9` (threshold `45.0`)
- ⚠️ **ur_dragon_creativity_envelope_held**

## Per-case summary

| Case | iter1 pass | wall (s) | cost ($) | LLM calls | creativity Δ | novel combos | theme coh. | must-inc resolved |
|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ✅ | 196.9 | $0.2741 | 4 | 35 | 7 | 1.00 | 2/2 |

## Per-case detail

### edgar_b3_vampire_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Vito, Thorn of the Dusk Rose', 'Bloodthirsty Conqueror']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `196.9`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 610 | 1258 | $0.0207 | 28.1 | - |
| C2_1_candidate_critic | True | 5799 | 2786 | $0.0592 | 52.1 | - |
| C2_2_wild_combo_discovery | True | 33480 | 911 | $0.1141 | 19.6 | - |
| D2_final_critic | True | 5560 | 4229 | $0.0801 | 89.3 | - |

- LLM total cost (this case): `$0.2741`
- LLM total tokens: `45449 in / 9184 out`

- creativity_delta_count (cards NOT in top-30 staples): `35`
- novel_combo_count (LLM combos with in_spellbook=false): `7`

**Novel combo flags:**
- Vito, Thorn of the Dusk Rose + Malakir Bloodwitch (Spellbook)  — Malakir Bloodwitch ETB drains each opponent; Vito doubles that life gain into additional opponent life loss, often removing one player from a three-opponent table in one action.
- Skullclamp + Edgar Markov (Spellbook)  — Each cheap vampire cast from command zone creates a 1/1 eminence token that can be Skullclamped for two cards, converting the token flood into near-unlimited card draw.
- Viscera Seer + Cordial Vampire (NOVEL)  — Sacrificing any vampire to Viscera Seer triggers Cordial Vampire to place +1/+1 counters on all remaining vampires, rapidly growing the entire board while scrying for gas.
- Bloodline Keeper + Legion Lieutenant (NOVEL)  — Bloodline Keeper generates a 2/2 flying token each turn; once five vampires are on board it flips to Lord of Lineage, and both lord effects stack to give each token +2/+2.
- Sorin, Imperious Bloodlord + Malakir Bloodwitch (Spellbook)  — Sorin's -3 at three loyalty cheats Malakir Bloodwitch into play for free, triggering her ETB drain on all opponents plus Vito amplification without paying five mana.
- Mirkwood Bats + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Every vampire token Edgar Markov creates via eminence triggers Mirkwood Bats (token created = opponent loses 1 life), which triggers Vito to drain each opponent for that much life — creating a cascading drain engine where each eminence token effectively drains for 2 per opponent without attacking. With wide boards this scales explosively.
- Ayara, First of Locthwain + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Every black creature entering (all Edgar tokens are black) triggers Ayara to drain 1 life per opponent, which triggers Vito to drain again for that amount. The deck is already flooding the board with black vampire tokens, so Ayara turns each eminence trigger into a double-drain event and also provides a sac outlet for card draw.
- Bastion of Remembrance + Sanctum Seeker (NOVEL) [applied as swap] — Bastion of Remembrance drains on creature death (tokens dying in combat, sac outlets), while Sanctum Seeker drains on attack. Together they cover both combat and death axes of the drain strategy. Critically, Bastion also enters with a Human Soldier token, giving Edgar another body to generate an eminence vampire, and its drain triggers Vito for a third layer.
- Baron Bertram Graywater + Edgar Markov (NOVEL) [applied as swap] — Baron Bertram's ability triggers whenever one or more tokens enter — Edgar's eminence creates a vampire token when you cast any vampire, then Baron creates a second 1/1 Vampire Rogue with lifelink. This doubles the token output from every vampire cast (once per turn), and each lifelink vampire that connects triggers Vito. A subtle but powerful doubling engine the deck currently lacks.
- Blood Artist + Cruel Celebrant (NOVEL)  — Both trigger on any creature death — with Skullclamp, sacrificing a 1/1 vampire token to Indulgent Aristocrat or Bloodthrone Vampire draws 2 cards and drains 2 life per opponent twice over (Blood Artist + Cruel Celebrant both fire), then Vito doubles the Cruel Celebrant trigger. The deck already has all three pieces (plus Vito) making this a present, functional multi-layer engine worth flagging.

**Intent analysis (LLM call #1):**

- likely_win_condition: `"Flood the board with vampire tokens via Edgar's eminence ability, then close out the game through Vito draining opponents as the wide lifelink or life-gain triggers accumulate, backed by Sanctum Seeker-style attack-step drain effects."`
- implicit_themes: `['Life drain as a win condition — Vito converts incidental life gain into lethal damage, so the deck wants repeated small life-gain triggers, not just big lifelink swings', "Token-based vampire swarm — Edgar Markov's eminence ability rewards casting cheap vampires to flood the board, making go-wide and +1/+1 counter synergies highly relevant", 'Blood token utility — Bloodthirsty Conqueror points toward wanting effects that synergize with artifact tokens, discard outlets, and card-cycling effects', 'Combat aggression — the commander and both must-includes are attack-oriented, implying the deck wants haste enablers, evasion, and combat-centric payoffs', "Incidental life gain — Vito creates pressure to include vampires and spells that gain life as a byproduct of normal play rather than dedicated 'life gain' packages"]`
- suggested_extensions:
  - `Reconnaissance` — Lets your vampire swarm attack freely for triggers and then untap before damage — a high-impact combat enabler that directly amplifies Edgar's go-wide aggression strategy
  - `Sanctum Seeker` — Drains each opponent for each attacking vampire, creating a life-loss engine that stacks beautifully with Vito's 'opponent loses life' conversion on every point gained
  - `Champion of Dusk` — Draws cards equal to your vampire count, solving the hand-refill problem that Blood token card draw from Bloodthirsty Conqueror alone cannot fully address at B3 board states
  - `Indulgent Aristocrat` — Provides a sacrifice outlet to grow the vampire team's power via +1/+1 counters, synergizing with Edgar's token generation and keeping lords relevant on a wide board
  - `Cordial Vampire` — Distributes +1/+1 counters across all vampires whenever any creature dies, turning opponent board wipes and trades into a counter-distribution payoff for your swarm
  - `Olivia's Wrath` — A tribal-skewed board wipe that leaves your vampires standing while clearing non-vampires, ideal for a B3 meta where resetting unfavorable boards is critical
- conflict_warnings: `["Vito's activated lifelink ability costs 3BB and requires black mana — in a three-color (RBW) deck running Edgar, ensure your mana base skews heavily black to avoid being unable to activate it at key moments", "Edgar Markov's go-wide token strategy and Vito's life-drain plan can pull card slots in different directions; at B3 you may face decks with consistent wraths, so overcommitting to either subtheme without redundancy risks losing momentum after removal"]`

**5 rationale samples (verbatim):**
- **Bloodthirsty Conqueror** (user_intent|llm_rationale_rewrite): _Creating Blood tokens on attack feeds Bartolomé del Presidio's sacrifice hunger and gives Furyblade Vampire discard fodder to activate Falkenrath Gorger's madness grants._
- **Vito, Thorn of the Dusk Rose** (user_intent|llm_rationale_rewrite): _The primary kill condition — every lifelink hit from Vampire Cutthroat, every Sanctum Seeker drain, and every Blood Artist trigger doubles as a damage source aimed directly at opponents._
- **Cordial Vampire** (theme:TYPAL_VAMPIRES|llm_intent_extension|llm_rationale_rewrite): _When Viscera Seer cracks a token as a free sac, every surviving vampire on board grows, turning incremental sac loops into a lethal power spike._
- **Blood Petal Celebrant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _A one-drop that triggers Edgar's eminence and produces a Blood token for Bartolomé del Presidio to sacrifice or Furyblade Vampire to discard._
- **Carrier Thrall** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _Dying produces a 2/2 Eldrazi Scion which can be sacrificed to Viscera Seer or Indulgent Aristocrat while also triggering Blood Artist and Bastion of Remembrance._

**Summary narrative:**

> The deck's primary plan is to cast cheap vampires in rapid succession, using Edgar Markov's eminence ability to double every creature drop into a board-flooding token swarm, then close the game by swinging the entire attack-step drain suite — Sanctum Seeker, Vito, Blood Artist, Cruel Celebrant, and Bastion of Remembrance — until opponents collapse under simultaneous life-loss triggers. The secondary kill path is a self-contained aristocrats loop: Viscera Seer or Indulgent Aristocrat sacrifices tokens to fire Cordial Vampire counter bursts and Blood Artist drains, while Skullclamp converts the tokens into card advantage to sustain the engine through disruption. Notable tech includes Mirkwood Bats converting every artifact-token creation (Blood tokens from Voldaren Epicure, Bloodthirsty Conqueror, Vampire // Treasure) into additional direct life loss before Vito even enters the equation, and Falkenrath Gorger granting the entire board madness so Furyblade Vampire's discard ability triggers eminence tokens at instant speed on opponents' end steps, ambushing blockers and growing the board at the least telegraphed moment.

**Consider adding (LLM flagged, not added):**
- `Reconnaissance` — Untapping all attacking vampires after combat damage is dealt lets Sanctum Seeker's drain trigger resolve while also protecting the swarm from all blockers, an effect no card in the current deck replicates.
- `Stensia Masquerade` — Giving all vampires first strike combined with Stromkirk Captain's first-strike grant means the swarm consistently kills blockers without trading, and counters stack on each connecting vampire to fuel Knight of the Ebon Legion-style scaling.
- `Shared Animosity` — Pumping each attacking vampire by one for each other attacking vampire of the same type converts the wide go-broad board into a one-shot kill threat that doesn't require Edgar Markov to be in play.

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
