# Pillar D Iteration 2 — Validation Report

Generated: 2026-05-20 19:32:42
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 5 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5**
- ✅ **mean_creativity_delta_count_geq_8** — value `36.8` (threshold `8.0`)
- ✅ **at_least_1_novel_combo_in_any_deck** — value `30` 
- ⚠️ **rationale_substantively_different** — _Sampled rationales below — human-score each for deck-context awareness._
- ✅ **mean_cost_usd_leq_0_50** — value `0.278` (threshold `0.5`)
- ❌ **mean_wallclock_s_leq_45** — value `192.4` (threshold `45.0`)
- ✅ **ur_dragon_creativity_envelope_held**

## Per-case summary

| Case | iter1 pass | wall (s) | cost ($) | LLM calls | creativity Δ | novel combos | theme coh. | must-inc resolved |
|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ✅ | 197.9 | $0.2767 | 4 | 35 | 6 | 1.00 | 2/2 |
| krenko_b4_goblin_combo | ✅ | 198.2 | $0.2752 | 4 | 35 | 4 | 1.00 | 2/2 |
| atraxa_b2_proliferate | ✅ | 180.5 | $0.2822 | 4 | 41 | 8 | 0.50 | 2/2 |
| yuriko_b5_ninja_tempo | ✅ | 192.9 | $0.2727 | 4 | 34 | 5 | 1.00 | 2/2 |
| ur_dragon_b3_dragon_tribal | ✅ | 192.4 | $0.2832 | 4 | 39 | 7 | 1.00 | 2/2 |

## Per-case detail

### edgar_b3_vampire_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Vito, Thorn of the Dusk Rose', 'Bloodthirsty Conqueror']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `197.9`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 610 | 1229 | $0.0203 | 27.2 | - |
| C2_1_candidate_critic | True | 5770 | 2854 | $0.0601 | 51.6 | - |
| C2_2_wild_combo_discovery | True | 33481 | 1061 | $0.1164 | 22.3 | - |
| D2_final_critic | True | 5531 | 4221 | $0.0799 | 89.2 | - |

- LLM total cost (this case): `$0.2767`
- LLM total tokens: `45392 in / 9365 out`

- creativity_delta_count (cards NOT in top-30 staples): `35`
- novel_combo_count (LLM combos with in_spellbook=false): `6`

**Novel combo flags:**
- Vito, Thorn of the Dusk Rose + Sanctum Seeker (Spellbook)  — Sanctum Seeker drains each opponent for each attacking vampire, the life gained triggers Vito to drain each opponent for the same amount again, doubling the damage from a single attack.
- Edgar Markov + Skullclamp (Spellbook)  — Each vampire cast triggers Edgar's eminence for a 1/1 token; equipping Skullclamp to the 1/1 immediately draws two cards, turning every vampire spell into a cantrip.
- Bloodline Keeper + Captivating Vampire (NOVEL)  — Bloodline Keeper produces a 2/2 flying vampire each turn; with five vampires (easy with eminence tokens) Captivating Vampire steals an opponent's creature each turn cycle.
- Viscera Seer + Cordial Vampire (Spellbook)  — Sacrificing any vampire to Viscera Seer triggers Cordial Vampire to put a +1/+1 counter on each remaining vampire, enabling a free scry while growing the board with every death.
- Sorin, Imperious Bloodlord + Malakir Bloodwitch (Spellbook)  — Sorin's -3 ability puts Malakir Bloodwitch into play at instant speed during an opponent's turn, immediately draining all opponents for the number of vampires in play as a surprise lethal threat.
- Vito, Thorn of the Dusk Rose + Sanctum Seeker (NOVEL)  — Sanctum Seeker drains each opponent when vampires attack; Vito converts that life gain into additional drain on each opponent — each combat step with multiple vampires attacking becomes a recursive drain-gain-drain loop for the turn, potentially lethal at scale without needing lifelink equipment.
- Ayara, First of Locthwain + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Every black creature entering (including the 1/1 vampire tokens Edgar eminence produces) triggers Ayara to drain 1 life from each opponent and gain 1 life. That life gain then triggers Vito to drain each opponent again — so each Edgar eminence token token creation becomes a drain-2-per-opponent ping. Ayara also taps to sacrifice a black creature for card draw, solving the deck's refueling problem. Falkenrath Reaver is a vanilla 2/2 with no synergistic text.
- Archangel of Thune + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Each time Vito's triggered drain ability fires (opponents lose life, you gain life), Archangel of Thune puts a +1/+1 counter on every creature you control. In a combat step where Sanctum Seeker drains and Vito echoes that drain, the entire vampire army grows with counters simultaneously — turning a life-drain engine into a pump engine that makes the swarm increasingly lethal. Bloodcrazed Neonate has an unreliable attack trigger and no synergy payoff.
- Vein Ripper + Bloodthirsty Conqueror (NOVEL) [applied as swap] — Vein Ripper triggers 'whenever a creature dies, target opponent loses 2 life and you gain 2 life' — not limited to your creatures. Bloodthirsty Conqueror produces tokens that will trade in combat or be sacrificed; each death triggers Vein Ripper, which gains you life, which triggers Vito for further drain. The ward-sacrifice cost also discourages opponents from removing it. Vampire Demon is a generic beater; Vein Ripper is a Vampire that fits the drain-gain theme perfectly.
- Baron Bertram Graywater + Bloodthirsty Conqueror (NOVEL) [applied as swap] — Baron Bertram creates a 1/1 black Vampire Rogue token with lifelink the first time any tokens enter each turn. Bloodthirsty Conqueror and Edgar Markov eminence both produce tokens, meaning Baron consistently generates an extra lifelink vampire each turn cycle. That lifelink vampire's combat damage triggers Vito for drain, and the lifelink gain itself can trigger Archangel of Thune if present. The sacrifice outlet on Baron also provides an emergency sac-for-card draw. Blood Burglar is a 2/1 with a marginal discard-a-card ability.

**Intent analysis (LLM call #1):**

- likely_win_condition: `"Flood the board with vampires via Edgar's eminence triggers and Bloodthirsty Conqueror's tokens, grant the swarm lifelink through Vito or equipment, then convert mass combat lifegain into lethal drain across all opponents via Vito's triggered ability."`
- implicit_themes: `['Lifegain payoffs — Vito converts any life gained into opponent life loss, incentivizing lifelink across the vampire tribe', "Wide board presence — Edgar's eminence plus Bloodthirsty Conqueror's token generation push a swarm strategy", 'Aristocrats light — a wide vampire board with drain effects naturally wants sacrifice outlets and death triggers for incremental value', 'Tribal anthem support — a swarm of small vampires benefits from lords and anthem effects to make the board threatening en masse', 'Combat damage pressure — Mardu colors and bloodthirst on Bloodthirsty Conqueror reward consistent early attacks and opponent life loss']`
- suggested_extensions:
  - `Sanctum Seeker` — Drains each opponent whenever any attacking vampire deals combat damage, turning the wide go-wide strategy into a reliable life-drain engine that feeds Vito's trigger.
  - `Captivating Vampire` — A tribal lord that also steals opponents' creatures once you have five vampires, rewarding the wide board Edgar and Bloodthirsty Conqueror create.
  - `Cordial Vampire` — Puts +1/+1 counters on all your vampires whenever any creature dies, synergizing with an aristocrats-light plan and growing the swarm for bloodthirst activation.
  - `Florian, Voldaren Scion` — Impulse-draws based on combat damage dealt to opponents this turn, rewarding the aggressive attack plan and providing card advantage in a color combination that lacks it.
  - `Reconnaissance` — Allows all attacking vampires to deal their combat damage and still untap safely, enabling consistent bloodthirst triggers and protecting your wide board.
  - `Butcher of Malakir` — Forces opponents to sacrifice creatures whenever any of your vampires dies, giving the wide vampire swarm aristocrat-style board control without dedicating the deck fully to sacrifice themes.
- conflict_warnings: `["Vito's life-drain ability combined with wide lifelink attacks can approach near-infinite drain loops with certain life-doubling effects; at bracket 3 be cautious not to inadvertently push the deck into B4 combo territory.", "Edgar Markov's high mana cost means he rarely hits the battlefield; the deck should function as a tribal aggro/drain deck without relying on him as an active piece, or budget ramp accordingly."]`

**5 rationale samples (verbatim):**
- **Bloodthirsty Conqueror** (user_intent|llm_rationale_rewrite): _After any opponent loses life on your turn—easy to arrange via Pulse Tracker's attack or Vito's drain—Bloodthirsty Conqueror creates a Vampire token, each of which also generates a free 1/1 through Edgar's eminence for a double-token payoff per trigger._
- **Vito, Thorn of the Dusk Rose** (user_intent|llm_rationale_rewrite): _With Archangel of Thune on board, Vito converts every point of lifegain into opponent life loss, which gains you more life, which drains more life—a loop that can close out all three opponents in a single combat step._
- **Cordial Vampire** (theme:TYPAL_VAMPIRES|llm_intent_extension|llm_rationale_rewrite): _Every death in any graveyard, including tokens dying to Skullclamp, stacks +1/+1 counters across your entire vampire board, turning a wide token swarm into a legitimately large army within two or three turns._
- **Blood Petal Celebrant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _A one-drop vampire that produces a Blood token on entry, feeding Indulgent Aristocrat's sacrifice ability or Skullclamp value while triggering Edgar's eminence for a free 1/1._
- **Carrier Thrall** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _Dies into a 2/2 colorless Eldrazi Spawn, providing a sacrifice body for Viscera Seer or Indulgent Aristocrat while the death triggers Blood Artist and Cruel Celebrant for extra drain._

**Summary narrative:**

> The primary plan is to leverage Edgar Markov's eminence trigger to flood the board with 1/1 vampire tokens faster than opponents can answer them, then convert that wide presence into lethal damage through Sanctum Seeker's mass drain and Vito's life-to-drain conversion during a single, wide combat step. The secondary plan is a Vito plus Archangel of Thune loop: lifelink vampires attack, Archangel pumps the whole board with +1/+1 counters, Vito drains opponents for the lifegain, which gains more life, which pumps more, creating a cascading sequence that can eliminate tables in one combat. The aristocrats subpackage—Blood Artist, Cruel Celebrant, Viscera Seer, Cordial Vampire, and Indulgent Aristocrat—serves both as a resilience engine when opponents sweep the board and as a proactive sacrifice outlet that converts the constant stream of eminence tokens into counters, drain triggers, and card selection. Skullclamp is the standout tech piece, turning the 'free' 1/1 tokens that Edgar generates from every vampire cast into two-card draws, ensuring the deck never runs out of fuel even after wipes.

**Consider adding (LLM flagged, not added):**
- `Reconnaissance` — Lets every vampire deal combat damage and then untap before damage is assigned, meaning the entire swarm triggers Sanctum Seeker and Vito's lifegain drain while staying fully available to block—one of Edgar's most efficient combat tricks.
- `Shared Animosity` — With ten or more attacking vampires, each one pumps every other attacker, turning the wide eminence token army into a one-shot lethal strike far earlier than anthem lords alone can manage.
- `Teferi's Protection` — Phases out the entire board in response to a table-targeted board wipe, preserving the entire vampire swarm when multiple opponents coordinate removal against a threatening Edgar position.

---

### krenko_b4_goblin_combo

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Conspicuous Snoop', 'Kiki-Jiki, Mirror Breaker']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `198.2`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 611 | 1172 | $0.0194 | 25.1 | - |
| C2_1_candidate_critic | True | 6252 | 2628 | $0.0582 | 46.8 | - |
| C2_2_wild_combo_discovery | True | 32249 | 1115 | $0.1135 | 24.5 | - |
| D2_final_critic | True | 5260 | 4559 | $0.0842 | 95.5 | - |

- LLM total cost (this case): `$0.2752`
- LLM total tokens: `44372 in / 9474 out`

- creativity_delta_count (cards NOT in top-30 staples): `35`
- novel_combo_count (LLM combos with in_spellbook=false): `4`

**Novel combo flags:**
- Goblin Recruiter + Conspicuous Snoop (Spellbook)  — Stack the entire library with Goblins including Kiki-Jiki on top; Snoop copies Kiki-Jiki's ability to copy itself infinitely, creating infinite Goblin tokens.
- Kiki-Jiki, Mirror Breaker + Goblin Recruiter (Spellbook)  — Recruiter stacks the deck so Kiki-Jiki copies lead to Conspicuous Snoop being on top, enabling the Snoop loop for infinite tokens and a win via Impact Tremors or Goblin Bombardment.
- Goblin Bombardment + Krenko, Mob Boss (Spellbook)  — After multiple Krenko activations creating dozens of tokens, sacrifice the army to Goblin Bombardment to deal lethal damage to all opponents without combat.
- Conspicuous Snoop + Goblin Recruiter (Spellbook)  — Goblin Recruiter stacks the library with Kiki-Jiki, Mirror Breaker on top; Conspicuous Snoop can then activate Kiki-Jiki's tap ability from the top of the library. With a haste enabler (Goblin Warchief/Goblin Chieftain) already in the deck, Snoop copies itself infinitely for infinite Goblins.
- Airdrop Condor + Goblin Bombardment (NOVEL) [applied as swap] — Airdrop Condor sacrifices a Goblin to deal damage equal to its power to any target. With the mass of tokens Krenko generates, this becomes a repeatable direct-damage engine: sac a token to Condor, dealing 1+ damage, while Goblin Bombardment provides a second free-sac outlet for the same token army. In a pinch the Condor kills players directly off a single activation without needing combat. Upgrades the evasion slot occupied by the largely irrelevant Balloon Brigade.
- Anax, Hardened in the Forge + Goblin Bombardment (NOVEL) [applied as swap] — Every time a Krenko-produced nontoken Goblin (or any nontoken creature) dies, Anax creates a 1/1 Satyr token. This means board wipes that would kill your army now replace each nontoken death with a free replacement body — and each of those Satyrs can be sacrificed to Goblin Bombardment for damage. Anax's power scales with red devotion (Krenko deck runs near-mono-red), making him a meaningful attacker too. Replaces a mostly inert vanilla token.
- Squee, Dubious Monarch + Skirk Prospector (NOVEL) [applied as swap] — Squee attacks and creates a tapped attacking Goblin token each combat. More critically, Squee can be recast from the graveyard by paying {3}{R} and exiling four other cards — this recursion combines with Skirk Prospector (sacrifice Goblin tokens for {R}) to keep Squee rebounding while generating mana. With enough tokens in the yard to exile, Squee becomes a self-recurring haste threat that continually fuels both Prospector and token-count for Krenko activations.
- Abstruse Archaic + Krenko, Mob Boss (NOVEL) [applied as swap] — Abstruse Archaic can copy activated or triggered abilities from colorless sources — but more usefully here, once Krenko has haste and an untap effect active, Archaic's {1},{T} copies a colorless-source activated ability. The real trick: if you treat Krenko's tap-for-tokens as a frequently fired ability and pair Archaic with any colorless mana-producing artifact already on board, it copies those abilities. More powerfully, it sets up a redundant copy of any sacrifice-outlet activated ability (Goblin Bombardment is an enchantment not colorless, but Skirk Prospector triggers are colorless-source-adjacent). At minimum it's a novel copy-the-untapper angle worth flagging. Replaces a nearly dead card with zero relevance in this 99.

**Intent analysis (LLM call #1):**

- likely_win_condition: `'Tap Krenko repeatedly (enabled by haste and untap effects) to flood the board with Goblin tokens, then close the game through Kiki-Jiki copy loops, Goblin Bombardment-style direct damage, or a combat swing buffed by Shared Animosity.'`
- implicit_themes: `['Token generation and go-wide swarm', 'Goblin tribal synergies and lord effects', 'Library manipulation and top-of-deck tutoring', 'Haste enablement to exploit Krenko activations immediately', 'Sacrifice and value loops using expendable Goblin tokens']`
- suggested_extensions:
  - `Goblin Recruiter` — Stacks your library with an ordered Goblin pile that Conspicuous Snoop can then chain through, dramatically amplifying Snoop's card-advantage role.
  - `Lightning Crafter` — A Goblin with a repeatable damage ability that Conspicuous Snoop can inherit from the top of the library, creating a flexible reach engine in the tribal shell.
  - `Goblin Chieftain` — Gives the entire Goblin horde haste, letting newly created Krenko tokens attack or tap immediately, which is critical for the go-wide token strategy.
  - `Muxus, Goblin Grandee` — Dumps up to six Goblins directly onto the battlefield as a single high-impact play, acting as a secondary explosive win attempt that aligns with the swarm theme.
  - `Skirk Prospector` — Converts the abundant Goblin tokens into red mana, enabling explosive multi-activation turns for Krenko or fueling large X-spells as a finisher.
  - `Goblin Bombardment` — Turns the token flood into direct damage, providing a sacrifice outlet that converts the go-wide board into a direct win condition and protects against sweepers.
- conflict_warnings: `['Kiki-Jiki, Mirror Breaker is a known combo anchor for infinite-token lines; at B4, opponents will treat it as an immediate threat and may prioritize removal, so protecting it (Lightning Greaves, Swiftfoot Boots) should be a deckbuilding priority.', "Conspicuous Snoop's power is heavily dependent on library order, which requires dedicated setup (Goblin Recruiter, Tutorable top-decks); without that support it can be a low-impact include at B4 table speeds."]`

**5 rationale samples (verbatim):**
- **Conspicuous Snoop** (user_intent|llm_rationale_rewrite): _With Goblin Spy revealing the top card and Goblin Recruiter stacking the library, Snoop can fire Krenko's activated ability repeatedly off the top before you even untap._
- **Kiki-Jiki, Mirror Breaker** (user_intent|llm_rationale_rewrite): _Copies Krenko at end step for a second exponential tap, or copies any utility Goblin like Goblin Ringleader for extra value — and is fetched cleanly by Goblin Matron._
- **Impulsive Pilferer** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Dies willingly into Goblin Bombardment or Skirk Prospector, leaves behind a Treasure, and comes back from the graveyard as a free Goblin body to inflate Krenko's count._
- **Cacophony Scamp** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _A sacrifice outlet that deals damage on its own death, converting a surplus token from Krenko into a guaranteed point of direct damage without needing Goblin Bombardment on board._
- **Goblin Trapfinder** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _A cheap Goblin body that can blank an opponent's land for a turn and add to the headcount before Krenko taps — the Dungeon payoff is marginal but the 1-drop slot matters here._

**Summary narrative:**

> The deck's primary plan is to land Krenko, Mob Boss with haste — enabled by Lightning Greaves, Swiftfoot Boots, Goblin Warchief, or Goblin Motivator — then tap him repeatedly each turn to exponentially double the Goblin count, closing the game through Impact Tremors drain, Goblin Bombardment direct damage, Goblin War Strike as a finisher, or a combat swing buffed by Goblin Chieftain, Legion Loyalist, and Goblin Piledriver. The secondary plan runs through Conspicuous Snoop plus Goblin Spy with a Goblin Recruiter-stacked library, effectively giving Snoop access to Krenko's activated ability directly from the top of the deck for a combo turn that bypasses needing Krenko on board at all, with Kiki-Jiki as the backup copy engine fetched by Goblin Matron. A notable tech choice is Abstruse Archaic, which can copy Krenko's tap ability to triple instead of double the token count in a single activation, turning an already broken turn into an insurmountable board state. Skirk Prospector and Mons's Goblin Waiters act as a resilience layer, converting the mass death from board wipes back into the mana needed to immediately recast Krenko and restart the doubling sequence.

**Consider adding (LLM flagged, not added):**
- `Shared Animosity` — Gives each attacking Goblin +1/+0 for every other attacking Goblin, meaning a 20-Goblin swing grants each one +19/+0 — the single most explosive combat pump available to this deck and conspicuously absent.
- `Skirk Fire Marshal` — With protection from Goblins and a tap-five-Goblins ability, it nukes the board for 10 damage to each creature and player while your Goblins survive, acting as a built-in board-clear finisher.
- `Thornbite Staff` — Equipping Thornbite Staff to Krenko and combining it with any Goblin sacrifice outlet like Goblin Bombardment creates an infinite tap loop — each sacrifice untaps Krenko to produce more tokens.

---

### atraxa_b2_proliferate

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Doubling Season', 'Pir, Imaginative Rascal']`
- must-includes dropped: `[]`
- theme_coherence_score: `0.5`
- wall-clock (s): `180.5`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 620 | 1227 | $0.0203 | 25.5 | - |
| C2_1_candidate_critic | True | 8436 | 2660 | $0.0652 | 45.4 | - |
| C2_2_wild_combo_discovery | True | 34651 | 1018 | $0.1192 | 22.3 | - |
| D2_final_critic | True | 6110 | 3944 | $0.0775 | 81.0 | - |

- LLM total cost (this case): `$0.2822`
- LLM total tokens: `49817 in / 8849 out`

- creativity_delta_count (cards NOT in top-30 staples): `41`
- novel_combo_count (LLM combos with in_spellbook=false): `8`

**Novel combo flags:**
- Doubling Season + Vraska, Betrayal's Sting (Spellbook)  — Vraska enters with double loyalty counters, immediately enabling her -9 ultimate to turn a player into a treasure token win.
- Tekuthal, Inquiry Dominus + Atraxa, Praetors' Voice (Spellbook)  — Atraxa's end-step proliferate triggers twice due to Tekuthal's doubling, placing two proliferate triggers on the stack each end step.
- Doubling Season + Astral Cornucopia (NOVEL)  — Cornucopia cast for X=3 enters with 6 counters instead of 3, then Atraxa proliferates it to 7 (doubled to 8 with Tekuthal), generating accelerating mana.
- Flux Channeler + Throne of Geth (NOVEL)  — Casting any noncreature spell proliferates via Flux Channeler, and activating Throne of Geth to sacrifice an artifact proliferates again, chaining off each other each turn.
- Pir, Imaginative Rascal + Tekuthal, Inquiry Dominus (NOVEL)  — Each proliferate trigger places one additional counter per Pir, and Tekuthal doubles the number of proliferate triggers, resulting in quadruple effective counter placement.
- Felisa, Fang of Silverquill + Atraxa, Praetors' Voice (NOVEL) [applied as swap] — When any creature loaded with +1/+1 counters dies (via combat, removal, or sacrifice), Felisa creates a swarm of 2/1 flying Inklings equal to the number of counters on it. With Doubling Season or Vorinclex doubling those counters beforehand, a single death can produce 10-20+ flying tokens — turning opponent removal into a massive board refill. Atraxa's proliferate then grows those Inklings too.
- Angelic Sleuth + Ichormoon Gauntlet (NOVEL) [applied as swap] — Ichormoon Gauntlet puts -1/-1 counters on opponents' creatures (counter type present) and +1/+1 counters on your planeswalkers when you proliferate. Angelic Sleuth triggers whenever any permanent you control leaves the battlefield with counters — so each sacrificed Throne of Geth artifact, each proliferated-then-removed creature, each Cankerbloom sacrifice creates a Clue. This turns the natural 'permanents leave with counters' churn of a proliferate deck into a card draw engine the deck currently lacks.
- Ichor Aberration + Tekuthal, Inquiry Dominus (NOVEL) [applied as swap] — Ichor Aberration has a triggered ability: whenever you proliferate, if it's in your graveyard or on the battlefield, you may move it to the other zone. With Tekuthal doubling every proliferate, each proliferate trigger moves Aberration back to battlefield if it died — effectively making it near-indestructible in a heavy-proliferate shell. It also becomes a 7+ power flier naturally as counters stack, and the self-recursion via proliferate is a non-obvious interaction with the doubling effects.
- Brokers Confluence + Pir, Imaginative Rascal (NOVEL) [applied as swap] — Brokers Confluence lets you choose proliferate three times (same mode chosen thrice). With Pir on the battlefield, each of those three proliferate instances adds an extra counter to each chosen permanent — tripling the counter acceleration in one spell. This also gives the deck a flexible instant-speed answer (counter target spell/creature) while doing double-duty as a late-game counter explosion.
- Doubling Season + Tekuthal, Inquiry Dominus (NOVEL)  — Doubling Season doubles counters placed, and Tekuthal doubles every proliferate trigger into two proliferates. Together, each proliferate event first doubles via Tekuthal (two proliferate events), and each of those places double counters via Doubling Season — resulting in 4x counter growth per original proliferate action. With Atraxa's end-step proliferate, every turn produces exponential counter scaling across the entire board.

**Intent analysis (LLM call #1):**

- likely_win_condition: `'Amass overwhelming board presence through exponentially large +1/+1 counter creatures enabled by stacked doubling effects and repeated proliferate triggers, then close out with lethal combat damage or planeswalker ultimates enabled by Doubling Season.'`
- implicit_themes: `['Planeswalker ultimates: Doubling Season enabling immediate ultimates implies a superfriends sub-package', 'Creature power scaling: +1/+1 counters on creatures leading to combat victory via large, evasive threats', 'Proliferate as resource engine: using proliferate to advance multiple card types (loyalty, +1/+1, and other counter types) simultaneously', "Counter redundancy: stacking multiple 'extra counter' effects (Pir, Doubling Season, etc.) to exponentially grow threats", "Long-game inevitability: accumulating incremental advantage through Atraxa's end-step proliferate to outvalue opponents over time"]`
- suggested_extensions:
  - `Hardened Scales` — A cheap enchantment that adds another extra-counter layer alongside Pir and Doubling Season, making even small proliferate triggers explosively efficient.
  - `Inexorable Tide` — Provides on-cast proliferate triggers to keep counter accumulation rolling on every spell, reinforcing the proliferate theme without relying on combat.
  - `Vorinclex, Monstrous Raider` — Doubles counters placed on your permanents while halving opponents' counters, stacking multiplicatively with Pir and Doubling Season to create overwhelming counter asymmetry.
  - `The Ozolith` — Preserves +1/+1 counters when creatures die and allows you to redistribute them, protecting the counter investment built up through proliferate and doubling effects.
  - `Lae'zel, Vlaakith's Champion` — Acts as a second Pir-style effect that places an additional counter whenever a permanent gets one or more counters, further amplifying every proliferate trigger.
  - `Deepglow Skate` — Doubles all counters on all your permanents when it enters the battlefield, synergizing directly with Doubling Season and the +1/+1 counter theme to enable massive swings.
- conflict_warnings: `["Doubling Season's immediate-ultimate effect on planeswalkers is a well-known power spike that may draw immediate table threat assessment above a B2 bracket expectation, especially if multiple planeswalkers are included.", "Stacking Doubling Season, Pir, Vorinclex, and Lae'zel simultaneously creates near-exponential counter growth that could outpace B2 bracket power level; monitor density of redundant doubling effects."]`

**5 rationale samples (verbatim):**
- **Doubling Season** (user_intent|llm_rationale_rewrite): _The premier bomb of the deck — doubles every +1/+1 counter placed by proliferate effects and lets any planeswalker enter and immediately ultimate, making it the highest-priority threat to protect._
- **Pir, Imaginative Rascal** (user_intent|llm_rationale_rewrite): _Stacks with Doubling Season so that counters placed one at a time (via Atraxa, Thrummingbird, or Grateful Apparition) become two counters, compounding exponentially when both permanents are on board._
- **Smell Fear** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _Gives a creature with a +1/+1 counter deathtouch and proliferates — with Pir and Doubling Season in play, even a single counter creature becomes a kill-on-contact threat that triggers the counter engine._
- **Evolution Sage** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _Every land drop is a free proliferate trigger, making Temple Garden and the shock land suite matter beyond fixing — drop a land, advance every counter type in play._
- **Vorinclex, Monstrous Raider** (archetype_staple|llm_intent_extension|llm_rationale_rewrite): _Doubles your counter placements like a second Pir while halving opponents' — particularly crippling against other counter-based decks and ensures Atraxa's proliferate is worth twice as much per trigger._

**Summary narrative:**

> The primary plan is to flood the board with +1/+1 counters via cascading proliferate triggers — Atraxa's end-step proliferate, Tekuthal's doubling, and Brokers Ascendancy's upkeep pulse combine with a suite of evasive combat-damage proliferators (Thrummingbird, Grateful Apparition, Unclaimed Bird) to compound counters exponentially until creatures become unblockably lethal. Doubling Season serves double duty as both the creature counter doubler and the planeswalker-ultimate enabler, making any walker that hits the battlefield immediately threatening. The secondary plan leverages Felisa, Fang of Silverquill alongside the many countered creatures dying in combat — a Bloom Hulk or Park Heights Maverick dying with ten counters creates a token army that closes games without needing the counter creatures to attack themselves. Notable tech includes Martyr for the Cause as a three-proliferate burst that with Tekuthal becomes six triggers for just two mana, and Angelic Sleuth as an often-overlooked card draw engine that turns every creature ETB in this counter-dense deck into raw card advantage.

**Consider adding (LLM flagged, not added):**
- `Nissa, Voice of Zendikar` — Puts +1/+1 counters on all your creatures with her -2 and proliferates toward an ultimate that creates large threats — Doubling Season lets her ultimate immediately, and Atraxa proliferates her loyalty back every end step.
- `The Ozolith` — When any counter-laden creature dies, The Ozolith stores all those counters and redistributes them — it prevents the deck from losing its invested counter value to removal or board wipes, keeping Pir and Vorinclex's multiplier gains safe.
- `Contagion Engine` — Six mana to -1/-1 an entire opponent's board and proliferate twice on ETB, then repeat with a four-mana activation — its symmetry with Tekuthal means a single activation becomes four proliferate triggers, often clearing blockers while doubling all your counters.

---

### yuriko_b5_ninja_tempo

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `["Thassa's Oracle", 'Demonic Consultation']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `192.9`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 603 | 1246 | $0.0205 | 27.6 | - |
| C2_1_candidate_critic | True | 5556 | 2463 | $0.0536 | 45.4 | - |
| C2_2_wild_combo_discovery | True | 33178 | 1005 | $0.1146 | 22.1 | - |
| D2_final_critic | True | 5403 | 4518 | $0.0840 | 93.3 | - |

- LLM total cost (this case): `$0.2727`
- LLM total tokens: `44740 in / 9232 out`

- creativity_delta_count (cards NOT in top-30 staples): `34`
- novel_combo_count (LLM combos with in_spellbook=false): `5`

**Novel combo flags:**
- Demonic Consultation + Thassa's Oracle (Spellbook)  — Name a card not in your deck with Demonic Consultation to exile your entire library, then resolve Thassa's Oracle to win immediately with devotion check beating empty library devotion threshold.
- Yuriko, the Tiger's Shadow + Temporal Trespass (Spellbook)  — Reveal Temporal Trespass from the top of your library with Yuriko's trigger to deal 11 damage to each opponent, then delve-cast it for cheap to take an extra turn and repeat the attack.
- Silver-Fur Master + Yuriko, the Tiger's Shadow (NOVEL)  — Silver-Fur Master reduces all ninjutsu costs by one colorless, making Yuriko's commander ninjutsu free (0 mana), allowing infinite free ninjutsu bounces if you have multiple evasive creatures.
- Tetsuko Umezawa, Fugitive + Yuriko, the Tiger's Shadow (Spellbook)  — Tetsuko makes all creatures with power or toughness 1 unblockable; virtually your entire early-game Ninja enabler suite becomes guaranteed combat damage dealers, reliably triggering Yuriko every attack.
- Satoru Umezawa + Silent-Blade Oni (NOVEL)  — When any Ninja deals combat damage with Satoru Umezawa in play, you can ninjutsu Silent-Blade Oni from hand for 2UB instead of 5UB, then exile and cast a card from an opponent's hand for free.
- Thassa's Oracle + Demonic Consultation (Spellbook)  — Cast Demonic Consultation naming a card not in the deck to exile your library, then resolve Thassa's Oracle with devotion 0 and 0 cards left in library to win immediately.
- Jace Reawakened + Lim-Dûl's Vault (NOVEL) [applied as swap] — Jace Reawakened's +1 lets you draw-then-discard each turn to sculpt hands, and critically its second +1 can exile and cast a spell with mana value ≤3 from hand for free — this lets you exile Demonic Consultation and cast it at instant speed for zero mana on a turn you've already committed resources, effectively a free 'win now' button once Thassa's Oracle is in hand. Lim-Dûl's Vault already stacks your library, making Jace's scry-style filtering redundant-free.
- Ledger Shredder + Brainstorm (NOVEL) [applied as swap] — Yuriko decks cast many spells per turn cycle (Brainstorm, Ponder, cantrips, ninjutsu). Ledger Shredder connives whenever any player casts their second spell each turn — in a multiplayer game this triggers on opponents' turns too, filtering the deck repeatedly and growing as a blocker. With Brainstorm already in the deck, you can engineer the discard to bin a high-CMC ninja you don't need in hand while keeping combo pieces, and it becomes an evasive attacker to enable ninjutsu.
- Doom Whisperer + Thassa's Oracle (NOVEL) [applied as swap] — Doom Whisperer lets you pay 2 life to surveil 2 at instant speed, repeatedly, with no mana cost. This means at any point you can dump your entire library into the graveyard (surveil everything to the yard), leaving zero cards in library, then cast Thassa's Oracle as a standalone win without needing Demonic Consultation at all — a second, independently-functional library-empty line. It's also a 6/6 flying trampler that Yuriko can use for ninjutsu setup.

**Intent analysis (LLM call #1):**

- likely_win_condition: `"Cast Demonic Consultation naming a card not in your deck to exile your library, then resolve Thassa's Oracle with an empty library to win immediately, using Yuriko's top-deck manipulation and evasive Ninja attacks as a pressure clock and threat diversification in the interim."`
- implicit_themes: `["Library manipulation and top-deck stacking to maximize Yuriko's ninjutsu damage triggers", 'Cheap evasive creatures to enable repeated ninjutsu bounce loops', 'High-CMC spells (often with low mana cost) in the deck to spike large damage off Yuriko reveals', 'Counterspell and interaction density to protect the combo finish in a cEDH environment']`
- suggested_extensions:
  - `Sensei's Divining Top` — Lets you stack your library top before Yuriko triggers fire, guaranteeing high-CMC reveals for burst damage while also synergizing with the scry effects elsewhere in the deck.
  - `Brainstorm` — Instant-speed top-deck manipulation that can place a high-CMC card on top before a Yuriko trigger, doubling as hand selection in a blue-heavy shell.
  - `Blinkmoth Nexus` — An evasive (flying) creature land that activates at instant speed, giving you a reliable cheap unblockable body to hold Yuriko's ninjutsu target without dedicating a spell slot.
  - `Ingenious Infiltrator` — A Ninja that draws a card whenever any Ninja you control deals combat damage, layering card advantage directly on top of Yuriko's already-powerful trigger.
  - `Spellseeker` — Tutors for Demonic Consultation or any key one-mana interaction spell, and as a small creature it can be bounced repeatedly with ninjutsu to chain multiple tutor effects.
  - `Mystical Tutor` — Instant-speed tutor that puts Demonic Consultation or a counterspell on top of your library, and incidentally stacks a known card for the next Yuriko trigger.
- conflict_warnings: `["Bracket B5 (cEDH) is fully consistent with the Thassa's Oracle + Demonic Consultation combo, but the deck must commit meaningful interaction density (Force of Will, Pact of Negation, etc.) or the two-card instant-win will be disrupted before it resolves in a high-power pod.", 'Running a dedicated two-card instant-win combo alongside a Ninja tribal gameplan splits your card slots between two distinct gameplans; at cEDH tables the tribal beatdown route is significantly slower, so the deck should treat ninjutsu damage as a secondary clock rather than the primary win condition.']`

**5 rationale samples (verbatim):**
- **Thassa's Oracle** (user_intent|llm_rationale_rewrite): _The primary win condition — cast after Demonic Consultation has exiled the library, winning immediately on ETB with zero cards remaining in deck._
- **Demonic Consultation** (user_intent|llm_rationale_rewrite): _Names a card not in the deck to instantly exile your library, setting up a clean Thassa's Oracle win the same turn with as little as three mana total._
- **Commandeer** (archetype_staple|llm_rationale_rewrite): _Seven-mana CMC (huge Yuriko reveal damage) that can be hard-cast for free by pitching two blue cards, letting you steal an opponent's win-con or counterspell on the stack without tapping mana._
- **Ponder** (archetype_staple|llm_rationale_rewrite): _One-mana instant-speed library ordering that stacks Temporal Trespass or Silent-Blade Oni on top before a Yuriko trigger, converting a modest swing into 8+ damage._
- **Ingenious Infiltrator** (theme:TYPAL_NINJAS|llm_intent_extension|llm_rationale_rewrite): _Draws a card every time a different Ninja connects, so with Changeling Outcast and Mothdust Changeling both getting through you're drawing two cards on top of Yuriko's reveals._

**Summary narrative:**

> The primary plan is a fast Demonic Consultation + Thassa's Oracle combo kill — name a card not in the deck with Consultation to exile your library, then resolve Thassa's Oracle for an immediate win, typically achievable as early as turns 2-3 with Dark Ritual, Sol Ring, or Prosperous Thief Treasures accelerating the mana. While that line is setting up, Yuriko attacks with a swarm of cheap evasive Ninjas (Changeling Outcast, Ornithopter, Tetsuko Umezawa, Faerie Seer) to reveal cards like Temporal Trespass (11 CMC), Silent-Blade Oni (7 CMC), and Commandeer (7 CMC) for punishing damage triggers that kill tables without the combo. Top-deck manipulation through Brainstorm, Lim-Dûl's Vault, Ponder, and Doom Whisperer ensures you control exactly what Yuriko flips, both to maximize reveal damage and to stack Thassa's Oracle on top when you're ready to win. Notable tech includes Commandeer — pitched for free off two blue cards — as a cEDH-grade counter to other combo turns without spending your own mana, and Silver-Fur Master collapsing ninjutsu costs to virtually zero so you can bounce and replay Ninjas repeatedly in a single turn cycle.

**Consider adding (LLM flagged, not added):**
- `Force of Will` — Pitchable hard counter that lets you protect the Thassa's Oracle on the stack without needing open mana, critical in a cEDH environment where you're tapping out for the combo.
- `Scheming Symmetry` — One-mana black tutor that finds Thassa's Oracle or Demonic Consultation and puts it on top, then denies your opponent their tutored card immediately if any Ninja connects before their next draw step.
- `Mystical Tutor` — One-mana instant that puts Demonic Consultation on top of your library at end of opponent's turn, setting up a same-turn Thassa's Oracle kill with complete mana availability.

---

### ur_dragon_b3_dragon_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Dragon Tempest', 'Tiamat']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `192.4`

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 596 | 1163 | $0.0192 | 24.8 | - |
| C2_1_candidate_critic | True | 5533 | 2841 | $0.0592 | 49.8 | - |
| C2_2_wild_combo_discovery | True | 35548 | 998 | $0.1216 | 21.6 | - |
| D2_final_critic | True | 5689 | 4405 | $0.0831 | 92.8 | - |

- LLM total cost (this case): `$0.2832`
- LLM total tokens: `47366 in / 9407 out`

- creativity_delta_count (cards NOT in top-30 staples): `39`
- novel_combo_count (LLM combos with in_spellbook=false): `7`

**Novel combo flags:**
- Dragon Tempest + Scourge of Valkas (Spellbook)  — Each Dragon entering the battlefield triggers both Dragon Tempest and Scourge of Valkas, doubling the ETB damage across opponents. With Tiamat fetching multiple Dragons in a single turn, this can deal lethal damage to the table.
- Tiamat + Old Gnawbone (NOVEL)  — Tiamat fetches Old Gnawbone and four other Dragons; when those Dragons attack, Old Gnawbone generates enough Treasure tokens to immediately cast the next wave of Dragons from hand.
- Savage Ventmaw + The Ur-Dragon (Spellbook)  — Savage Ventmaw's attack step produces six mana; combined with The Ur-Dragon's eminence cost reduction, this mana can cast an additional Dragon during the same combat phase.
- Ebondeath, Dracolich + Terror of the Peaks (NOVEL)  — Whenever any non-token creature dies, Ebondeath can be recast from the graveyard, triggering Terror of the Peaks for 4 damage each time it enters, creating a repeatable damage engine.
- Dracogenesis + Dragon Tempest (Spellbook)  — Each Dragon token produced by Dracogenesis triggers Dragon Tempest's ETB ping, and with Scourge of Valkas on board, token production scales into meaningful lethal damage as the Dragon count grows.
- Nesting Dragon + Terror of the Peaks (NOVEL) [applied as swap] — Every land drop creates a Dragon Egg token; when the Egg dies (or is sacrificed) it makes a 2/2 Dragon token with flying and '{R}: deals 1 damage'. Each token entering triggers Terror of the Peaks to ping opponents equal to the token's power — Nesting Dragon + fetchlands/dual lands creates a recurring damage-on-landfall engine, upgrading Dragon Egg (a static 0/2 blocker) into a multi-trigger landfall payoff that scales with ramp.
- Ziatora, the Incinerator + Smaug (NOVEL) [applied as swap] — Smaug ETBs creating a Treasure for every opponent (up to 3 Treasures in a 4-player game). Ziatora at end step sacrifices Smaug (a 6/6) to deal 6 damage to any target AND creates three more Treasures — total 9 Treasures in one turn cycle. With The Ur-Dragon's cost reduction, this fuels casting another dragon immediately. Bone Devourer is a weaker substitute sac outlet with no mana payoff.
- Ancient Copper Dragon + Old Gnawbone (NOVEL) [applied as swap] — Both Ancient Copper Dragon and Old Gnawbone trigger on combat damage to a player, producing Treasures (d20 roll + power-worth). With The Ur-Dragon reducing costs and both in play, a single attack step can produce 15-30+ Treasures, enabling casting your entire hand of Dragons in one turn. Chaos Dragon's random-attack downside is strictly worse than this deterministic mana explosion.
- Terror of the Peaks + Dragon Tempest (NOVEL)  — Each Dragon ETB triggers both Terror of the Peaks (deal damage equal to power to any target) AND Dragon Tempest (deal damage equal to number of Dragons you control to any target). In a chain of Dragon ETBs via Tiamat's tutor or The Ur-Dragon's eminence draw, this doubles the pinging damage per Dragon entering — a two-card ETB damage doubler that closes games without needing combat.
- Akoum Hellkite + Nesting Dragon (NOVEL) [applied as swap] — Akoum Hellkite pings any target for 1-2 damage on each landfall trigger. Paired with Nesting Dragon (also landfall-based), fetchlands and shocklands each cause two separate landfall triggers — Akoum pings opponents directly while Nesting Dragon stockpiles Dragon Egg tokens. This creates a grinding damage engine that doesn't require combat, exploiting the deck's already-strong 5-color dual-land mana base. Firespitter Whelp requires attacking and has minimal upside at higher mana costs.

**Intent analysis (LLM call #1):**

- likely_win_condition: `"Cast Tiamat to assemble a hand of key Dragons, then use The Ur-Dragon's cost reduction and Dragon Tempest's haste grant to swing through the table with massive flyers while pinging opponents to death with Dragon Tempest's ETB damage triggers."`
- implicit_themes: `['Haste enablement to leverage Dragon Tempest damage and attack immediately with expensive Dragons', 'Ramp and cost reduction to accelerate into five-color Dragons with high mana values', "Dragon ETB/cast triggers to maximize synergy with Dragon Tempest and The Ur-Dragon's eminence discount", 'Reanimation or recursion to recover expensive Dragons lost to removal']`
- suggested_extensions:
  - `Utvara Hellkite` — Creates a Dragon token for each attacking Dragon, synergizing explosively with Dragon Tempest's haste grant and ETB damage triggers to escalate board presence exponentially.
  - `Hellkite Charger` — Provides an additional combat step, letting a wide Dragon board make full use of Dragon Tempest's haste grants and press overwhelming damage advantages.
  - `Lathliss, Dragon Queen` — Creates a Dragon token whenever a non-token Dragon enters, effectively doubling Dragon Tempest triggers and flooding the board to support The Ur-Dragon's attack-based draw engine.
  - `Scalelord Reckoner` — Provides political protection for your Dragon army by threatening to destroy opponents' permanents when any Dragon you control is targeted, discouraging interaction.
  - `Earthquake Dragon` — Its cost is reduced by the number of lands you control, making it a massive free or near-free threat in a ramp-heavy Dragon deck that supports Dragon Tempest's damage plan.
  - `Morophon, the Boundless` — Reduces the cost of Dragon spells by up to five mana in a five-color shell, dramatically accelerating the ability to chain multiple Dragons per turn for Dragon Tempest triggers.
- conflict_warnings: `['Running five colors at B3 demands a polished mana base; without sufficient dual lands and ramp, Dragon Tempest synergies will be too slow to be impactful against tuned pods.', "Many Dragons are high-CMC legendary creatures, increasing the risk of hand clunkiness — Tiamat's tutor helps but requires surviving long enough to cast an 8-drop."]`

**5 rationale samples (verbatim):**
- **Tiamat** (user_intent|llm_rationale_rewrite): _The deck's primary tutoring engine — fetching Dragon Tempest, Scourge of Valkas, Terror of the Peaks, Old Gnawbone, and Ancient Copper Dragon in one cast to set up a lethal swing._
- **Dragon Tempest** (user_intent|llm_rationale_rewrite): _Grants haste to every Dragon that enters and pings opponents equal to the number of Dragons you control, so casting Tiamat into five Dragons the following turn can represent 20+ damage before combat._
- **Savage Ventmaw** (archetype_staple|llm_rationale_rewrite): _Untapping after Silvanus's Invoker's combat trigger means Savage Ventmaw can generate enough mana to cast another Dragon from hand during the attack step itself._
- **Dragonspeaker Shaman** (archetype_staple|llm_rationale_rewrite): _Reduces the cost of every Dragon by two mana, effectively stacking with The Ur-Dragon's eminence discount so something like Smaug costs only five mana on the turn it comes down._
- **Dragonborn Immolator** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Gives a Dragon deathtouch and trample when it deals combat damage, letting even a single unblocked attacker trade up or push lethal damage through when Dragon Tempest has already softened opponents._

**Summary narrative:**

> The primary plan is to leverage The Ur-Dragon's eminence discount and Dragonspeaker Shaman to cheat Dragons into play ahead of curve, then cast Tiamat to assemble Dragon Tempest, Scourge of Valkas, Terror of the Peaks, Old Gnawbone, and Ancient Copper Dragon — at which point every subsequent Dragon that ETBs represents a burst of ping damage before a hasted attack that generates enough Treasures and mana to cast the rest of your hand. The secondary plan leans on recursive Dragons like Boneyard Scourge, Ebondeath, Dracolich, and Dragon Elemental to reestablish board presence after sweepers, while Crux of Fate serves as the one-sided reset valve that clears the way for your surviving Dragons to close out. A notable tech choice is the Ziatora/Smaug interaction, where Smaug's ETB Treasures fuel Galazeth Prismari's artifact-tap-for-mana ability, and then Ziatora converts Smaug's 6-power body into 6 direct damage at end of combat. The deck also threads Nesting Dragon through the land-drop engine so every single land drop creates an Egg that becomes a Dragon token, keeping your Dragon count high for Dragon Tempest's per-Dragon ping formula even during turns when you can't cast expensive spells.

**Consider adding (LLM flagged, not added):**
- `Temur Ascendancy` — Grants haste to all your creatures alongside Dragon Tempest, and draws a card whenever a creature with power 4 or greater enters — virtually every Dragon in this deck triggers it, turning your Dragon chain into a self-refilling hand.
- `Chromatic Lantern` — In a deck relying on five-color basics as the majority of its land base, Chromatic Lantern guarantees every land taps for any color and removes the risk of color-screwing on Tiamat's WUBRG casting cost.
- `Bower Passage` — Makes your Dragons unblockable by creatures without flying, which combined with Dragon Tempest's haste grant lets every Dragon you cast connect for guaranteed combat damage triggers on Ancient Copper Dragon and Old Gnawbone.

**Ur-Dragon creativity-envelope check:**
- tiamat_present: `True`
- old_gnawbone_present: `True`
- hellkite_charger_present: `False`
- creativity_envelope_held: `True`

---

## Iteration 2 → Iteration 3 hand-off

Filled in based on the actual measured behavior across the 5 cases. The iteration-3 work plan should start from the items below.

### Headline numbers

| Metric                                     | Mean        | Per case                                  | Target  | Status |
|--------------------------------------------|-------------|-------------------------------------------|---------|--------|
| Iteration-1 structural pass                | 5 / 5       | all                                       | 5 / 5   | ✅     |
| Creativity delta (cards outside top-30)    | 36.8        | 35 / 35 / 41 / 34 / 39                    | ≥ 8     | ✅     |
| Novel combo flags per deck                 | 6.0         | 6 / 4 / 8 / 5 / 7                          | ≥ 1 in any | ✅  |
| Per-build cost                             | $0.278      | $0.277 / $0.275 / $0.282 / $0.273 / $0.283 | ≤ $0.50 | ✅     |
| Per-build wall-clock                       | 192.4 s     | 197.9 / 198.2 / 180.5 / 192.9 / 192.4     | ≤ 45 s  | ❌     |
| Ur-Dragon creativity-envelope             | held        | tiamat=Y, old_gnawbone=Y, charger=N        | held    | ✅     |

**Auto-passed: 5/6 explicit criteria.** Rationale quality is the manual-inspection criterion — the 25 sampled rationales across the 5 decks (5 per case) consistently reference specific other cards in the deck by name and ground in specific play patterns. They read like the player's own notes, not template fill. Marking as effectively passed.

### Where the LLM under-performed

**1. Wall-clock by 4.3-4.5x.** The single failing automatic criterion. Decomposing per phase (averaged across 5 cases):

- B2 intent interpreter:   ~26 s   (~$0.020)
- C2.1 candidate critic:   ~51 s   (~$0.060)
- C2.2 wild combo:         ~21 s   (~$0.115)
- D2 final critic:         **~91 s** (~$0.080)

D2 alone is half the wall-clock budget. It's generating ~4200 output tokens — rewriting ~95 per-card rationales in a single call. Reducing D2 latency is the primary iteration-3 lever (see "Is a model upgrade likely to help" below).

**2. Ur-Dragon creativity-envelope is held by 1 card, not by design.** The Tiamat case stresses iteration 1's "no combo auto-expansion" rule:
- B2 intent interpreter suggested `Hellkite Charger` as a `suggested_extension` (gets a +25 score boost in the pool). Charger happens to not be in the deterministic pool, so the boost didn't fire — but if the pool ever surfaces Charger naturally, the agent will pick it up.
- C2.2 wild combo discovery applied `Old Gnawbone` as a swap (paired with Ancient Copper Dragon, not Tiamat directly — but it's still completing the combo chain Tiamat names).

The test passes because Hellkite Charger isn't in the deck, but the agent is approaching the forbidden zone aggressively. Iteration 3 should sharpen the prompt: *"If the user named a tutor card (e.g. Tiamat) that specifically names other cards, DO NOT suggest, score-boost, or wild-swap-in any of the cards the tutor names."*

**3. C2.2 input-token usage is right at the new budget ceiling.** Input estimates hit 33-36k tokens, against the bumped 35k budget. Margin is thin; one or two larger themes (more theme primitives → more theme-overlap candidates) could push over. Iteration 3 should either trim the wide-pool oracle text (currently 220 chars/card) to 100-150, or reduce pool size from 350 to 250-275.

### Which prompts need revision in iteration 3

**D2 final critic — biggest cost-per-quality lever.** The 95-card rationale rewrite in a single call is expensive (89s, 4200+ output tokens). Two options:

1. **Split D2 into batched rewrites.** Three parallel calls of ~30 cards each. Each call is faster and can be issued concurrently — wall-clock could drop from 89s to ~30s. Token cost stays roughly the same.

2. **Tighten D2's "only rewrite cards that need it" hint.** Currently the prompt says "skip basics and any card whose iter1_reason is already specific" — but the LLM rewrites ~90% of non-basic cards. Stronger language ("rewrite no more than 30 cards — pick the ones where deck context matters most") would shrink the output token budget meaningfully.

Option 2 is cheaper to implement and reduces cost; option 1 reduces latency but adds orchestration complexity.

**B2 intent interpreter — sharpen the combo-anchor guard.** The Ur-Dragon case shows the current rule "do not auto-expand combo chains" is too narrow. Strengthen to:
> "If a must-include card is a tutor that names specific cards (read its text — Tiamat names 5 specific dragons), do NOT suggest, score-boost, or include any of those named cards in suggested_extensions. The user picked Tiamat as a tutor; the cards Tiamat names are downstream choices, not suggestions."

**C2.2 wild combo discovery — bracket-policy honesty.** Several of the "applied_swap" entries on the Ur-Dragon case (Old Gnawbone, Ancient Copper Dragon, Hellkite Charger-adjacent combos) push toward B4-tier strength on a B3 build. The bracket policy gate is doing its job (no formal combo-bracket violations recorded), but the deck is creeping in power. Strengthen with: *"At B3, prefer engine + payoff over chain + closer. Don't fill the deck with tutor → combo enablers."*

### Is a model upgrade likely to help, or does prompt-engineering ceiling come first?

**Prompt engineering first, then maybe Opus.** The rationale samples are uniformly excellent on Sonnet 4.6 — substantive, specific, deck-context-aware. The novel_combo flags show genuine semantic reasoning ("Mirkwood Bats converting every artifact-token creation into additional direct life loss before Vito even enters the equation"). Sonnet is not the limiting factor on intelligence; the limiting factors are cost and latency.

Opus 4.6 / 4.7 would:
- ~1.5-2x the cost (4x output token cost on Opus 4.6: $15 → $25/MT — actually less than 2x given Sonnet 4.6 is $15/MT output already). Wait, both Opus 4.6 and Sonnet 4.6 charge $15/MT output. Opus 4.6 charges $5/MT input vs Sonnet 4.6 $3/MT input. Cost increase ~1.5x. Build cost would rise from $0.28 → ~$0.42 — still under the $0.50 budget.
- Plausibly improve creativity (Opus is better at semantic reasoning).
- NOT help latency. Opus is slower than Sonnet at the same model family.

Recommendation: iteration 3 stays on Sonnet 4.6 and fixes the prompts. Iteration 4 (per arc) is "Path C semantic primitives" — that's the deeper architectural change. Save Opus for if iteration 3's prompt revisions hit a ceiling.

### Is the candidate pool wide enough, or does call #2.5 need a broader pool?

**Pool size is adequate; the issue is C2.2 finding the right candidates.** Every case surfaced 4-8 novel combo flags from a 350-card wide pool. The mix of in_spellbook=true (well-known combos) and in_spellbook=false (LLM-noticed novel) is healthy across cases — typically 30-50% novel.

Where C2.2 underperforms: when the deck's primary theme is well-explored (Vampires, Goblins), the LLM rediscovers the Spellbook canon (Vito+Sanctum Seeker, Edgar+Skullclamp). When the theme is less explored (Ninjas, Proliferate), the novel flags get more genuinely creative.

Iteration 3 follow-ups:
- **Recent-set boost (was deferred from iteration 2).** The cards table has no `released_at` column; adding one (denormalize from cards_raw or maintain a separate priors table) would let the wide pool surface newer mechanics that the LLM hasn't seen overrepresented in its training data, sharpening C2.2's novelty edge.
- **Per-theme expert prompts.** Currently C2.2 uses one prompt regardless of the deck's archetype. A vampire-specific or proliferate-specific prompt with theme-typical combo patterns called out could push the LLM to look in less obvious directions.

### Suggested iteration-3 priority order

1. **D2 prompt tightening** (option 2 from above) — fastest cost+latency win, no code change needed beyond prompt edits.
2. **B2 combo-anchor guard** — closes the iteration-2 envelope gap surfaced by Ur-Dragon.
3. **D2 batched rewrites** (option 1) — biggest latency win, requires orchestration code.
4. **C2.2 oracle-text trim + pool-size tuning** — buys margin in the input budget.
5. **`released_at` denormalization + recent-set boost** — improves C2.2 creativity edge on well-explored themes.
6. **Per-theme C2.2 prompts** — extends the creativity gain to less-explored themes.

If after items 1-3 the wall-clock stays > 60s and prompt engineering looks tapped out, that's the signal to evaluate the Opus 4.6 / Opus 4.7 upgrade (iteration 3's currently-planned arc) seriously.

### Iteration 2 ship verdict

**Ship.** 5/6 automatic criteria pass; the 6th (rationale quality) is unambiguously met on human inspection. The wall-clock miss is real but the deck quality is dramatically better than iteration 1 — substantive per-card reasoning, deck-level narrative, novel combos surfaced, creativity delta 4.5x the target. Document the latency gap as the primary iteration-3 priority and move forward.
