# Pillar D Iteration 2 — Validation Report

Generated: 2026-05-20 20:52:58
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 5 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5**
- ✅ **mean_creativity_delta_count_geq_8** — value `40.0` (threshold `8.0`)
- ✅ **at_least_1_novel_combo_in_any_deck** — value `6` 
- ⚠️ **rationale_substantively_different** — _Sampled rationales below — human-score each for deck-context awareness._
- ✅ **mean_cost_usd_leq_0_50** — value `0.2706` (threshold `0.5`)
- ❌ **mean_wallclock_s_leq_45** — value `176.4` (threshold `45.0`)
- ✅ **ur_dragon_creativity_envelope_held**

## Per-case summary

| Case | iter1 pass | wall (s) | cost ($) | LLM calls | creativity Δ | novel combos | theme coh. | must-inc resolved |
|---|---|---|---|---|---|---|---|---|
| ur_dragon_b3_dragon_tribal | ✅ | 176.4 | $0.2706 | 4 | 40 | 6 | 1.00 | 2/2 |

## Per-case detail

### ur_dragon_b3_dragon_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Dragon Tempest', 'Tiamat']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `176.4`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 714 | 1314 | $0.0219 | 30.2 | - |
| C2_1_candidate_critic | True | 5757 | 2883 | $0.0605 | 51.9 | - |
| C2_2_wild_combo_discovery | True | 35700 | 1100 | $0.1236 | 24.1 | - |
| D2_final_critic | True | 6425 | 3025 | $0.0646 | 65.9 | - |

- LLM total cost (this case): `$0.2706`
- LLM total tokens: `48596 in / 8322 out`

- creativity_delta_count (cards NOT in top-30 staples): `40`
- novel_combo_count (LLM combos with in_spellbook=false): `6`

**Novel combo flags:**
- Terror of the Peaks + Scourge of Valkas (Spellbook)  — Each Dragon ETB triggers both Terror of the Peaks (damage equal to power) and Scourge of Valkas (damage equal to Dragon count) simultaneously, creating cascading lethal damage to all opponents as Dragons are cast.
- Archwing Dragon + Dragon Tempest (NOVEL)  — Archwing Dragon returns to hand at end step and can be recast every turn, each time triggering Dragon Tempest's haste grant and its damage ping, repeatedly dealing damage for just 4 mana per iteration.
- Old Gnawbone + Savage Ventmaw (Spellbook)  — Savage Ventmaw untaps six mana during combat, and Old Gnawbone creates Treasures for every combat damage dealt, producing effectively unlimited mana to cast Dragons post-combat in the same turn.
- Ebondeath, Dracolich + Dragon Tempest (NOVEL)  — Whenever any non-token creature dies, Ebondeath recasts itself from the graveyard for B, each re-entry triggering Dragon Tempest's 1-damage ping plus haste grant, enabling repeated value in attrition-heavy games.
- Tiamat + Terror of the Peaks (Spellbook)  — Tiamat's 7/7 body entering with Terror of the Peaks on board deals 7 damage immediately, then the five Dragons tutored to hand are cast to deal their collective power as damage, often ending the game.
- Ziatora, the Incinerator + Scourge of Valkas (NOVEL) [applied as swap] — At end of turn, sacrifice a large dragon (e.g. Smaug at 6 power) to Ziatora dealing 6 damage to any target and creating 3 Treasures. If Scourge of Valkas is out, the Treasure tokens are not dragons, but the real payoff is sacrificing a dragon that has already triggered Scourge on ETB — then Ziatora re-uses that power as a damage vector plus ramp. With Old Gnawbone also in play, combat damage from Ziatora creates Treasures, and sacrificing one of those fatties closes games without needing to get attacks through.
- Nesting Dragon + Scourge of Valkas (NOVEL) [applied as swap] — Nesting Dragon is a strict upgrade to Dragon Egg in this shell: every land drop creates a 0/2 Dragon Egg token, and when that egg dies (through any sac outlet, combat, or board wipe) it produces a 2/2 flying Dragon — each of which triggers Scourge of Valkas and Dragon Tempest on arrival. With The Ur-Dragon's eminence, casting Nesting Dragon itself is discounted, and subsequent land drops create a continuous stream of Dragon ETBs to ping opponents even outside your attack step.
- Akoum Hellkite + Dragon Tempest (NOVEL) [applied as swap] — Akoum Hellkite provides a second independent damage-on-land-drop engine. With Dragon Tempest already giving haste and pinging on ETB, Akoum Hellkite adds an ongoing Landfall trigger dealing 1-2 damage per land drop. In a five-color deck that fetches and shocks heavily, this translates to free chip damage every turn cycle. Slumbering Dragon is very slow to become relevant and rarely attacks; Akoum Hellkite is immediately impactful.
- Sarkhan the Masterless + Dragon Tempest (NOVEL) [applied as swap] — Arcades has no synergy with this deck (no defender subtheme). Sarkhan the Masterless gives every planeswalker you control the ability to ping attacking creatures for 1 (via the static passive), and his +1 turns all your planeswalkers into 4/4 Dragons until end of turn — each one created this way triggers Dragon Tempest for haste AND deals damage equal to the dragon count via Scourge of Valkas. Turning even two planeswalkers into Dragons mid-combat is a non-obvious burst of ETB damage.

**Intent analysis (LLM call #1):**

- likely_win_condition: `"Cast a critical mass of Dragons in one or two turns (enabled by The Ur-Dragon's cost reduction and Tiamat assembling the perfect hand), trigger Dragon Tempest and Scourge of Valkas repeatedly to deal lethal damage to all opponents, then swing with a hasted aerial army to finish anyone who survived the ETB damage."`
- implicit_themes: `['Haste enablement — Dragon Tempest already provides it, suggesting the deck wants multiple redundant haste sources so large Dragons can attack or trigger immediately', 'ETB payoffs — both must-includes reward Dragons entering the battlefield, implying the deck values repeated cast and bounce effects or flicker-adjacent strategies', "Ramp acceleration — The Ur-Dragon's nine mana cost and five-color identity demand significant mana acceleration to deploy threats ahead of curve", 'Evasive combat — Dragons are naturally flying; the deck implicitly wants to close games through combat damage in the air rather than through non-combat win conditions', 'Dragon density and curve — Tiamat tutors five specific Dragons, implying the deck runs a broad, intentional Dragon package with high-impact targets at varying mana values']`
- suggested_extensions:
  - `Temur Ascendancy` — A second haste-granting enchantment that also draws a card whenever a large Dragon (power 4+) enters, directly doubling down on Dragon Tempest's role and recovering hand advantage as you deploy threats
  - `Utvara Hellkite` — Each attacking Dragon spawns a 6/6 Dragon token, creating a geometric board-wide explosion that turns Dragon Tempest into a game-ending damage source on the same combat step
  - `Scourge of Valkas` — A second Dragon Tempest–style damage trigger that scales with the total number of Dragons you control, rewarding the high Dragon density that Tiamat's tutor naturally assembles
  - `Bladewing the Risen` — Recurs a Dragon from the graveyard on ETB and pumps all Dragons, giving the toolbox assembled by Tiamat a recursion safety valve within the tribal shell
  - `Lathliss, Dragon Queen` — Generates a Dragon token every time a non-token Dragon enters, amplifying the ETB count that Dragon Tempest converts into free damage and widening the attack force
  - `Dragon's Hoard` — Accumulates Gold counters on each Dragon cast and converts them into card draw, providing mana fixing and long-game refueling that complements Tiamat's one-shot tutor
- conflict_warnings: `["Dragon Tempest's damage trigger requires a meaningful Dragon density to function as a win condition; dipping too heavily into non-Dragon utility creatures to fix mana or answer threats will dilute the payoff at Bracket 3 power level", "The Ur-Dragon's nine-mana cost is a legitimate liability in a Bracket 3 environment with interaction; without sufficient early ramp or cost-reduction, the commander may be too slow to matter before opponents develop threatening boards"]`

**5 rationale samples (verbatim):**
- **Tiamat** (user_intent|llm_rationale_rewrite): _When it enters, it assembles the exact five Dragons needed to close the game — typically fetching Scourge of Valkas, Terror of the Peaks, Old Gnawbone, Savage Ventmaw, and a situational answer — turning one resolved spell into a complete win-condition hand._
- **Dragon Tempest** (user_intent|llm_rationale_rewrite): _Combined with Scourge of Valkas, each Dragon that enters triggers both enchantments simultaneously, letting the ETB damage stack compound while the haste grant means those same Dragons can attack the same turn to feed Old Gnawbone's Treasure generation._
- **Savage Ventmaw** (archetype_staple|llm_rationale_rewrite): _Attacking with Savage Ventmaw produces six mana floating into your second main phase, which is often exactly enough to cast a second Dragon and trigger Dragon Tempest's haste grant again on the same turn._
- **Dragonspeaker Shaman** (archetype_staple): _Corpus staple for The Ur-Dragon (usage_pct=0.53). [slot=flex]_
- **Dragonborn Immolator** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Whenever it deals combat damage, you may sacrifice it to deal that much damage to any target — in a deck where Dragon Tempest gives everything haste, you can attack with a pumped Dragonborn Immolator and immediately convert that damage into direct removal._

**Summary narrative:**

> The deck's primary plan is to deploy Dragons with enough velocity — accelerated by The Ur-Dragon's eminence discount and ramp like Savage Ventmaw — that Scourge of Valkas and Terror of the Peaks deal lethal ETB damage to all opponents before combat even resolves; Dragon Tempest ensures every Dragon arrives with haste to enable the attack step as a finishing blow. Tiamat is the critical turn: resolving it at seven mana tutors the exact five Dragons needed to complete the kill, typically assembling the Scourge-plus-Terror damage stack in a single cast chain. The secondary plan leverages Old Gnawbone's Treasure generation to fund a second Dragon wave after a stalled board, with Boneyard Scourge and Ebondeath, Dracolich providing graveyard recursion so removal tax doesn't stop the engine. Notable tech choices include Archwing Dragon as a repeating ETB ping source that triggers Scourge of Valkas every turn for only four mana, and Ziatora, the Incinerator converting end-of-turn Dragon sacrifices into direct damage plus Treasure mana, ensuring every Dragon that would otherwise die to opponents' interaction generates value on the way out.

**Consider adding (LLM flagged, not added):**
- `Temur Ascendancy` — Provides a redundant haste grant for all Dragons alongside Dragon Tempest, and drawing a card whenever a power-4-or-greater creature enters means every Dragon in this deck replaces itself — giving you card advantage the deck currently lacks outside of Tiamat.
- `Crucible of Fire` — Giving every Dragon in the deck +3/+3 dramatically increases Old Gnawbone's Treasure output per attacker and raises Scourge of Valkas's kill threshold, turning even small utility Dragons like Sprite Dragon into meaningful combat threats.
- `Kindred Discovery` — Drawing a card whenever a Dragon you control enters or attacks turns the deck's mass ETB Dragon chains into massive hand refills, letting you sustain the kill combo through counterspells and removal rather than running dry after a Tiamat is answered.

**Ur-Dragon creativity-envelope check:**
- tiamat_present: `True`
- old_gnawbone_present: `True`
- hellkite_charger_present: `False`
- creativity_envelope_held: `True`

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
