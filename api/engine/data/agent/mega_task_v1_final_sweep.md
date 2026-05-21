# Pillar D Iteration 3 — Validation Report

Generated: 2026-05-20 22:47:44
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 4 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5** — value `True` 
- ✅ **mean_creativity_delta_count_geq_30** — value `37.6` (threshold `30`)
- ✅ **mean_novel_combo_count_geq_4** — value `5.8` (threshold `4`)
- ✅ **mean_cost_usd_leq_0_40** — value `0.2953` (threshold `0.4`)
- ❌ **mean_wallclock_s_leq_60** — value `139.8` (threshold `60`)
- ❌ **ur_dragon_envelope_held_by_design**

## Per-case summary

| Case | iter1 | wall (s) | cost ($) | LLM calls | creativity Δ | novel | guard size | guard fires | archetype |
|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ✅ | 142.5 | $0.2792 | 7 | 37 | 6 | 55 | 0 | tribal |
| krenko_b4_goblin_combo | ✅ | 137.2 | $0.2943 | 7 | 36 | 5 | 267 | 1 | tribal |
| atraxa_b2_proliferate | ✅ | 134.2 | $0.3406 | 7 | 41 | 7 | 459 | 1 | control |
| yuriko_b5_ninja_tempo | ✅ | 144.3 | $0.2829 | 7 | 34 | 4 | 85 | 0 | combo |
| ur_dragon_b3_dragon_tribal | ✅ | 140.7 | $0.2796 | 7 | 40 | 7 | 7 | 0 | tribal |

## Per-case detail

### edgar_b3_vampire_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Vito, Thorn of the Dusk Rose', 'Bloodthirsty Conqueror']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `142.5`
- creativity_delta_count: `37`
- novel_combo_count: `6`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `55`
- guard_fire_count: `0`
- sample forbidden: ['aetherflux reservoir', 'aettir and priwen', 'angelic chorus', 'arcbond', "ashnod's altar", 'beacon of immortality', 'blood tribute', 'cliffhaven vampire']

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 1001 | 1196 | $0.0209 | 25.9 | - |
| C2_1_candidate_critic | True | 7140 | 2963 | $0.0659 | 56.2 | - |
| C2_2_wild_combo_discovery | True | 24754 | 1023 | $0.0896 | 22.5 | - |
| D2_final_critic_batch_0 | True | 6143 | 1417 | $0.0397 | 29.5 | - |
| D2_final_critic_batch_1 | True | 6057 | 787 | $0.0300 | 18.3 | - |
| D2_final_critic_batch_2 | True | 6063 | 776 | $0.0298 | 16.3 | - |
| E_mana_base_critique | True | 517 | 118 | $0.0033 | 3.7 | - |

**10 rationale samples (verbatim):**
- **Bloodthirsty Conqueror** (user_intent|llm_rationale_rewrite): _Whenever this deck gains life — via Vito, Vampire of the Dire Moon, or Sanctum Seeker — Bloodthirsty Conqueror creates another Vampire token, meaning each attack wave that drains through Sanctum Seeker directly replenishes the board for the next attack._
- **Vito, Thorn of the Dusk Rose** (user_intent|llm_rationale_rewrite): _Converts every point of life gained — from Archangel of Thune triggers, lifelink attackers, or Cleric of Life's Bond — into additional life loss for opponents, creating a recursive drain loop when paired with any lifelinker attacking through the wide token board._
- **Cordial Vampire** (theme:TYPAL_VAMPIRES|llm_intent_extension|llm_rationale_rewrite): _Any creature death — from Blood Artist triggers, Cruel Celebrant sacrifices, or combat trade-offs — puts a +1/+1 counter on every Vampire you control, turning a sweeper into an army-wide pump that feeds Elenda's Hierophant and Indulgent Aristocrat's synergies._
- **Blood Petal Celebrant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _A one-mana Vampire that creates a Blood token lets Voldaren Epicure and Voldaren Estate synergize for additional Blood production, and the body itself triggers Edgar's eminence for a free 1/1 that pressures life totals alongside Pulse Tracker._
- **Carrier Thrall** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _When this Vampire dies — whether to a sweeper or sacrificed to Viscera Seer — it leaves behind a 2/2 Zombie token that triggers Cruel Celebrant's drain, effectively trading one death for a drain event plus a replacement blocker._
- **Sanctum Seeker** (archetype_staple|llm_intent_extension|llm_rationale_rewrite): _With a wide Edgar token board attacking each turn, Sanctum Seeker drains each opponent for the full count of attacking Vampires, and that life gained simultaneously triggers Bloodthirsty Conqueror to create replacement tokens and Vito to double-up the life-loss on opponents._
- **Cruel Celebrant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _Acts as a second Blood Artist that drains for each creature death, so when Viscera Seer sacrifices Edgar eminence tokens or a board wipe clears the field, Cruel Celebrant and Blood Artist together deal lethal damage to all opponents simultaneously._
- **A-Blood Artist** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _Pairs with Blood Artist as redundant drain-on-death effects so that mass sacrifice events with Bloodthrone Vampire or sweepers trigger two separate drain triggers per creature lost, accelerating Vito's life-gain-to-drain loop._
- **Dusk Legion Sergeant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _A two-mana Vampire lord that gives your tokens and weenies a static buff, and its presence means each eminence-generated 1/1 from Edgar immediately becomes a more threatening 2/2 that helps Skymarcher Aspirant and Vicious Conquistador punch through blockers._
- **Blood Artist** (theme:TYPAL_VAMPIRES): _Theme 'TYPAL_VAMPIRES' signal_count=2 (freq_in_corpus=0.77). [slot=creature]_

**Novel combo flags:**
- Vito, Thorn of the Dusk Rose + Bloodthirsty Conqueror (Spellbook)  — Bloodthirsty Conqueror triggers whenever you gain life to create a Vampire token; Vito causes that token's ETB lifegain effects and any subsequent lifelinks to drain opponents, creating a self-reinforcing engine that can rapidly drain all opponents to zero over several turns.
- Edgar Markov + Bloodline Keeper (Spellbook)  — Edgar's eminence creates a token with each Vampire cast, and Bloodline Keeper creates additional 2/2 flyers each turn; with enough Vampires Bloodline Keeper flips into Lord of Lineage for a massive anthem making the swarm lethal in one attack.
- Indulgent Aristocrat + Cordial Vampire (NOVEL)  — Sacrificing any Vampire to Indulgent Aristocrat triggers Cordial Vampire's death trigger, placing a +1/+1 counter on every Vampire you control while Indulgent Aristocrat's ability also adds a counter, effectively doubling the counter accumulation from each sacrifice.
- Skullclamp + Edgar Markov (Spellbook)  — Attaching Skullclamp to the 1/1 Vampire tokens generated by Edgar's eminence kills them immediately and draws two cards each, converting the free token generation into continuous card draw with no additional mana investment.
- Captivating Vampire + Edgar Markov (Spellbook)  — Edgar's eminence floods the board with Vampires; once five Vampires are tapped, Captivating Vampire permanently steals an opponent's most threatening creature each turn, repeatedly converting the wide board into control over the best threat on the table.
- Ayara, First of Locthwain + Edgar Markov (NOVEL) [applied as swap] — Every Vampire spell cast triggers Edgar's eminence to create a 1/1 token; that token entering triggers Ayara to drain each opponent 1 life and gain you 1 life. With a board of Vampires, casting even a single cheap spell can net 2+ drains per turn cycle. Ayara's tap-sacrifice also converts dead tokens into card draw, solving the deck's refuel problem. Falkenrath Reaver is a vanilla 2/2 with no upside.
- Archangel of Thune + Sanctum Seeker (NOVEL) [applied as swap] — Sanctum Seeker drains life each time a Vampire attacks. Archangel of Thune converts each life gain trigger into a +1/+1 counter on every creature you control. With even 4–5 attacking Vampires, a single combat step can make the entire board grow by several counters, turning the life-drain engine into an exponential power snowball. Bloodcrazed Neonate requires combat damage to grow and is a weak standalone.
- Elenda's Hierophant + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Elenda's Hierophant grows a counter for each life gain. When it dies, it creates X 1/1 lifelink Vampire tokens equal to its power. Those tokens entering triggers Edgar eminence for more tokens, and their lifelink damage feeds Vito for additional drain. A single board wipe becomes a massive refuel-and-drain event. Blood Burglar is a vanilla 2/2 with an irrelevant discard upside in this deck.
- Baron Bertram Graywater + Edgar Markov (NOVEL) [applied as swap] — Edgar's eminence creates a 1/1 token whenever you cast a Vampire spell. Baron Bertram triggers once each turn when one or more tokens enter, creating an additional 1/1 lifelink Vampire Rogue token. This effectively doubles the first token generation event each turn for free, feeds lifelink into the Vito drain chain, and provides a sac outlet for Skullclamp loops. Falkenrath Pit Fighter requires discarding to attack and gives no token synergy.
- Cordial Vampire + Skullclamp (NOVEL)  — Sac a 1/1 Vampire token to Skullclamp for 2 cards; Cordial Vampire sees a creature die and puts a +1/+1 counter on every Vampire you control. With the Edgar token engine running, you continuously sacrifice 1/1s for 2 cards while the rest of your board grows. The loop is gated by available tokens and mana but generates enormous card advantage mid-to-late game without forming a deterministic infinite loop, fitting B3.

**Summary narrative:**

> The primary plan is to cast cheap Vampires and leverage Edgar Markov's eminence to flood the board with 1/1 tokens, then attack as a wide swarm with Sanctum Seeker turning each attacker into a drain trigger that simultaneously gains life and costs opponents life. That life gain feeds the secondary engine: Bloodthirsty Conqueror creates replacement Vampire tokens from each life-gain event while Vito converts those same life-gain triggers into additional drain, meaning a single combat step can drain opponents for lethal amounts through layered triggers. Archangel of Thune amplifies every lifelink or drain event into +1/+1 counters distributed across the whole board, and Cordial Vampire then spreads those counters further whenever any creature dies, turning a board wipe into a buff for survivors. Notable tech choices include Ayara, First of Locthwain passively converting Edgar's free eminence tokens into chip damage on ETB, and Baron Bertram Graywater generating bonus Vampires on non-creature spell turns so that Sol Ring and Arcane Signet still contribute to the token flood.

**Consider adding (LLM flagged, not added):**
- `Reconnaissance` — Lets your entire token army attack to trigger Sanctum Seeker's drain and Archangel of Thune's counters, then untap all attacking Vampires before blockers deal damage, giving you the trigger value without the combat loss.
- `Mirror Entity` — With a wide Edgar token board and significant life total built up through Vito and Sanctum Seeker, a late-game pump to pump all tokens to enormous power via Mirror Entity closes the game in one swing when opponents least expect a non-Vampire in a Vampire tribal deck.
- `Vault of the Archangel` — Grants your entire wide Vampire board lifelink and deathtouch for a combat step, turning a single Sanctum Seeker attack into a massive Vito trigger and making blockers near-suicidal for opponents.

---

### krenko_b4_goblin_combo

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Conspicuous Snoop', 'Kiki-Jiki, Mirror Breaker']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `137.2`
- creativity_delta_count: `36`
- novel_combo_count: `5`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `267`
- guard_fire_count: `1`
- guard_fire_events: [{'phase': 'B2_intent_interpreter', 'field': 'suggested_extensions', 'card': 'Goblin Bombardment'}]
- sample forbidden: ['________ goblin', 'accomplished alchemist', 'adarkar valkyrie', 'aether flash', "agatha's soul cauldron", 'aggravated assault', 'akki battle squad', 'all-fates stalker']

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 2167 | 1259 | $0.0254 | 28.0 | - |
| C2_1_candidate_critic | True | 8288 | 2961 | $0.0693 | 53.0 | - |
| C2_2_wild_combo_discovery | True | 25111 | 934 | $0.0893 | 19.8 | - |
| D2_final_critic_batch_0 | True | 7126 | 1284 | $0.0406 | 28.0 | - |
| D2_final_critic_batch_1 | True | 7050 | 813 | $0.0333 | 19.5 | - |
| D2_final_critic_batch_2 | True | 7047 | 758 | $0.0325 | 16.1 | - |
| E_mana_base_critique | True | 473 | 156 | $0.0038 | 4.4 | - |

**10 rationale samples (verbatim):**
- **Conspicuous Snoop** (user_intent|llm_rationale_rewrite): _When Goblin Spy is on top of the library, Snoop gains the ability to play cards off the top, which combined with Goblin Matron (to arrange the deck) lets this deck chain through Goblins for free until it finds the pieces it needs._
- **Kiki-Jiki, Mirror Breaker** (user_intent|llm_rationale_rewrite): _Copies Battle Cry Goblin for a free anthem trigger, doubles Goblin Ringleader digs, or makes a second Goblin Matron to tutor on the same turn, while also having built-in haste so Lightning Greaves isn't strictly required to use him immediately._
- **Impulsive Pilferer** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _When it dies to Skullclamp, it creates a Treasure token that immediately funds another Krenko activation, making it a self-replacing sacrifice target that smooths out the mana math on big turns._
- **Cacophony Scamp** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Sacrificing it after combat redirects a small burst of damage to any target, giving this deck a tool to snipe a low-loyalty planeswalker or finish off a player who survived Shared Animosity's pump — and it is a Goblin that feeds Krenko's count before doing so._
- **Goblin Trapfinder** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Conspicuous Snoop wants the top card to be a Goblin as often as possible, and Goblin Trapfinder's triggered loot lets the deck dig past non-Goblin top-decks to set up Snoop's ability or find Goblin Warchief and Goblin Chieftain for the combat step._
- **Goblin Arsonist** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _A free 1-damage ping on death means surplus tokens sacrificed to Goblin Chirurgeon or killed by a board wipe each chip away at opponents' life totals, stacking alongside Impact Tremors for incremental pressure even through removal-heavy games._
- **Dynamite Diver** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _A one-mana Goblin that enters the battlefield and deals damage on death gives Krenko a token-seeding body that also threatens to punish blockers, inflating Krenko's activation count while offering a sacrifice target for Goblin Chirurgeon to protect Krenko itself._
- **Fireblade Charger** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Equipping it with Lightning Greaves or Swiftfoot Boots before sacrificing it converts an otherwise disposable 1/1 token into a significant fireball, and Kiki-Jiki can copy it to threaten multiple sacrificial fireballs in a single turn cycle._
- **Goblin Shaman** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Tapping to use Krenko in response to removal by first activating Goblin Shaman to sacrifice the new tokens into damage gives the deck a way to extract value even when the board gets wiped, functioning as a bridge between Pashalik Mons's drain plan and the raw combat plan._
- **Goblin King** (archetype_staple|llm_intent_extension): _Corpus staple for Krenko, Mob Boss (usage_pct=0.72). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_

**Novel combo flags:**
- Krenko, Mob Boss + Kiki-Jiki, Mirror Breaker (Spellbook)  — Kiki-Jiki copies Krenko at end of opponent's turn creating a haste Krenko token; on your turn both activate to create an arbitrarily large Goblin army in one turn cycle.
- Conspicuous Snoop + Goblin Matron (Spellbook)  — Goblin Matron puts a Goblin on top of library; Conspicuous Snoop can then activate that Goblin's ability from the top, enabling recursive loops with the right Goblin (e.g. another Matron).
- Impact Tremors + Krenko, Mob Boss (Spellbook)  — Each Krenko activation creating N tokens deals N damage instantly; with 10 Goblins in play Krenko makes 10 tokens for 10 damage, and next activation 20 tokens for 20 damage—closing the game in two activations.
- Goblin War Strike + Krenko, Mob Boss (Spellbook)  — After two Krenko activations from a board of 5 Goblins you reach ~20+ Goblins; Goblin War Strike then deals 20+ damage to one or more players as a one-card finisher.
- Skullclamp + Pashalik Mons (NOVEL)  — Equip Skullclamp to a 1/1 Goblin token to draw 2 cards and have it die, triggering Pashalik Mons for 1 damage; with many tokens this chains into massive card draw and lethal damage simultaneously.
- The Peregrine Dynamo + Krenko, Mob Boss (NOVEL) [applied as swap] — The Peregrine Dynamo copies Krenko's tap ability (a legendary activated ability from a non-commander legendary source), doubling the Goblin tokens produced each activation. With even one untap enabler, this effectively quadruples your army per turn instead of doubling it — no infinite loop required, just raw exponential scaling.
- Squee, Dubious Monarch + Pashalik Mons (NOVEL) [applied as swap] — Squee creates a free attacking Goblin token every attack — each of those tokens dying in combat triggers Pashalik Mons for 1 damage to any target. Squee recurs himself from the graveyard, ensuring the engine never runs dry. Combine with Shared Animosity and the tokens also threaten meaningful damage before they die.
- Goblin Spymaster + Shared Animosity (NOVEL) [applied as swap] — Goblin Spymaster forces opponents to create 1/1 Goblins at their end step that must attack — those opponent-controlled Goblins are still Goblins, so Shared Animosity pumps YOUR attacking Goblins based on the total Goblin count including the tokens opponents now awkwardly hold. More subtly, it taxes opponents to keep blockers back, clearing the path for your alpha strike.
- Krenko, Baron of Tin Street + Krenko, Mob Boss (NOVEL) [applied as swap] — Two Krenkos create a layered engine: Mob Boss makes tokens, Baron sacrifices any artifact (including treasures or equipment) to put +1/+1 counters on ALL Goblins, growing the entire army. Baron also generates a Goblin token whenever any artifact hits a graveyard — so Skullclamp kills create a chain where Clamping a 1/1 draws two cards AND creates a new Goblin for Baron to potentially pump later.
- Conspicuous Snoop + Kiki-Jiki, Mirror Breaker (Spellbook)  — Classic Goblin combo: with Kiki-Jiki on top of library (via Goblin Matron or Goblin Ringleader), Conspicuous Snoop gains Kiki's tap ability. Tap Snoop to copy itself, the copy also sees Kiki on top and can copy itself again — infinite hasty Goblin Snoop tokens, winning via Impact Tremors or Pashalik Mons drain.

**Summary narrative:**

> The primary plan is to land Krenko, Mob Boss with haste from Lightning Greaves or Goblin Warchief and trigger his exponential doubling each turn, converting a small starting board into an overwhelming Goblin army within two or three activations. The finishing blow comes through combat backed by Shared Animosity and Goblin Piledriver making each attacker enormous, or through non-combat damage from Impact Tremors and Goblin War Strike killing all opponents simultaneously without needing to get through blockers. The secondary plan leans on Pashalik Mons and Skullclamp to grind value out of token deaths, keeping the hand full and draining life totals even when board wipes set back the board state. Notable tech choices include Goblin Spymaster's forced-attack clause to strip away blockers and create political pressure, and The Peregrine Dynamo effectively granting Krenko a second tap activation per turn without any untap infrastructure.

**Consider adding (LLM flagged, not added):**
- `Goblin Trashmaster` — Acts as a tribal lord that also doubles as artifact removal by sacrificing Goblins, giving this deck an answer to Torpor Orb or equipment-based threats while Krenko's tokens make the cost trivial.
- `Skirk Fire Marshal` — With enough Goblins from Krenko's doubling, tapping five Goblins to deal 10 damage to everything can be a one-sided board wipe when your tokens are already expendable and opponents' boards are more invested.
- `Massive Raid` — A second Goblin War Strike effect that deals damage equal to your Goblin count, providing redundancy for the non-combat kill shot that becomes lethal after just a few Krenko activations.

---

### atraxa_b2_proliferate

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Doubling Season', 'Pir, Imaginative Rascal']`
- must-includes dropped: `[]`
- theme_coherence_score: `0.5`
- wall-clock (s): `134.2`
- creativity_delta_count: `41`
- novel_combo_count: `7`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `459`
- guard_fire_count: `1`
- guard_fire_events: [{'phase': 'D2_final_critic', 'field': 'consider_adding', 'card': 'Staff of Compleation'}]
- sample forbidden: ['academy manufactor', 'adaptive gemguard', 'adrix and nev, twincasters', 'aegis automaton', 'aetherflux reservoir', 'aethertide whale', 'agatha of the vile cauldron', "agatha's soul cauldron"]

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 3434 | 1335 | $0.0303 | 28.9 | - |
| C2_1_candidate_critic | True | 13106 | 2623 | $0.0787 | 44.1 | - |
| C2_2_wild_combo_discovery | True | 27341 | 991 | $0.0969 | 20.8 | - |
| D2_final_critic_batch_0 | True | 9242 | 1557 | $0.0511 | 32.8 | - |
| D2_final_critic_batch_1 | True | 9177 | 832 | $0.0400 | 17.7 | - |
| D2_final_critic_batch_2 | True | 9168 | 823 | $0.0398 | 17.5 | - |
| E_mana_base_critique | True | 572 | 141 | $0.0038 | 3.8 | - |

**10 rationale samples (verbatim):**
- **Doubling Season** (user_intent|llm_rationale_rewrite): _The single most powerful enchantment in the deck — it lets Vraska, Betrayal's Sting enter at 12 loyalty and ultimate immediately, doubles every +1/+1 counter Pir would already add an extra counter to, and stacks explosively with Atraxa's proliferate at end step._
- **Pir, Imaginative Rascal** (user_intent|llm_rationale_rewrite): _Turns every single proliferate trigger from Atraxa, Grateful Apparition, Thrummingbird, Flux Channeler, and friends into two counters instead of one, meaning the exponential counter growth that Doubling Season normally requires can be reached much faster through sheer accumulation._
- **Grateful Apparition** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _A cheap flier that proliferates on combat damage — with Atraxa flying alongside it, both connect reliably to stack extra loyalty on Vraska or grow Astral Cornucopia counters, and Tekuthal doubles each of those combat-damage triggers._
- **Guildpact Informant** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _Unblockable evasion guarantees a proliferate trigger every attack step regardless of board state, making it one of the most reliable ways to continuously tick up Astral Cornucopia charges and planeswalker loyalty even through stalled combat._
- **Flux Channeler** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _Every noncreature spell — Contentious Plan, Experimental Augury, Tezzeret's Gambit, Fuel for the Cause — fires an extra proliferate through Flux Channeler, effectively doubling the spell suite's counter output and creating a cascade where casting cheap draw spells also advances every planeswalker and creature on the board._
- **Inexorable Tide** (archetype_staple|llm_intent_extension): _Corpus staple for Atraxa, Praetors' Voice (usage_pct=0.59). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_
- **Proud Pack-Rhino** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_rationale_rewrite): _As a Phyrexian creature it fits the tribal thread while providing two instances of +1/+1 counters to other creatures when it attacks — those counters then get doubled by Pir and multiplied further by Doubling Season, rapidly inflating the team's power._
- **Brokers Confluence** (llm_wild_combo_discovery|llm_rationale_rewrite): _Choosing proliferate three times on this modal instant is nine separate proliferate selections that, under Tekuthal, Inquiry Dominus become six proliferates in a single activation, capable of spiking Vraska, Betrayal's Sting or any other planeswalker into ultimate range mid-combat as an instant-speed move._
- **Glistening Sphere** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_rationale_rewrite): _Enters the battlefield with charge counters that Atraxa and Tekuthal proliferate each upkeep, eventually cracking it to draw three cards and refuel the hand at the exact moment the deck's counter engines need new fuel to keep accelerating._
- **Scheming Aspirant** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_rationale_rewrite): _Places a +1/+1 counter on target creature whenever you proliferate — with Atraxa proliferating every end step and Tekuthal doubling each trigger, this translates to two free +1/+1 counters per cycle landing on your biggest threat before Pir even adds his bonus._

**Novel combo flags:**
- Doubling Season + Vraska, Betrayal's Sting (Spellbook)  — Vraska enters with double loyalty counters (14 instead of 7), immediately reaching her -10 ultimate to proliferate poison counters onto all opponents for a near-instant win.
- Tekuthal, Inquiry Dominus + Atraxa, Praetors' Voice (Spellbook)  — Atraxa's end-step proliferate triggers twice due to Tekuthal, doubling every counter on every permanent each turn cycle for exponential growth.
- Pir, Imaginative Rascal + Astral Cornucopia (NOVEL)  — Each proliferate event adds an extra charge counter to Astral Cornucopia via Pir, making the rock grow far faster than opponents anticipate into a large mana source.
- Sword of Truth and Justice + Tekuthal, Inquiry Dominus (NOVEL)  — Dealing combat damage with the equipped creature triggers a proliferate that Tekuthal doubles, resulting in two proliferate events per successful attack.
- Flux Channeler + Contentious Plan (NOVEL)  — Casting Contentious Plan triggers Flux Channeler to proliferate, giving two proliferate events from a single two-mana cantrip, both doubled by Tekuthal if present.
- Angelic Sleuth + Felisa, Fang of Silverquill (NOVEL) [applied as swap] — When creatures with +1/+1 counters die (Felisa trigger), Angelic Sleuth independently triggers on the same event to investigate, generating Clue tokens for card draw — creating a dual-payoff death loop. With Doubling Season, counters are doubled on entry so nearly every nontoken creature death yields both Inkling tokens (Felisa) and Clue tokens (Sleuth), turning normal attrition into massive card advantage.
- Agent Frank Horrigan + Tekuthal, Inquiry Dominus (NOVEL) [applied as swap] — Agent Frank Horrigan proliferates TWICE on attack. With Tekuthal doubling each proliferate trigger (making each instance proliferate twice), a single attack becomes four proliferate events. Combined with Atraxa's end-step proliferate this is five proliferate events per turn cycle — pushing planeswalkers toward ultimates in one or two turns while stacking +1/+1 counters geometrically.
- Brokers Confluence + Pir, Imaginative Rascal (NOVEL) [applied as swap] — Brokers Confluence can select proliferate three times (or mix in removal/counter placement). With Pir on board, each proliferate mode adds an extra counter to each permanent receiving counters. Choosing proliferate three times with Pir results in tripled counter accumulation in a single instant — usable at end of turn to spike planeswalker loyalty to ultimate range. The flexibility (counterspell mode available) fits the control archetype.
- Doubling Season + Vraska, Betrayal's Sting (Spellbook)  — Vraska enters with doubled loyalty (12 instead of 6) under Doubling Season, making her ultimate (-9) immediately activatable the turn she enters. This is a well-known Doubling Season + planeswalker pattern but worth flagging: Vraska's ultimate sets each opponent's poison to 10, winning the game on the spot — an alternate win condition already embedded in the deck.
- Draining Whelk + Pir, Imaginative Rascal (NOVEL) [applied as swap] — Draining Whelk counters a spell and puts X +1/+1 counters on itself where X is the countered spell's mana value. With Pir on the battlefield, each of those counters becomes two, turning a counterspell on a 4-mana spell into a 8/8 flash flier. This is not a combo in the broken sense — it's an interaction that rewards the proliferate/+1+1 package with a massive threat stapled to interaction, fitting the control archetype's need for cheap-interaction-with-reach.

**Summary narrative:**

> The primary plan is to land Atraxa, Praetors' Voice alongside Pir, Imaginative Rascal and Doubling Season, then leverage Atraxa's free end-step proliferate to exponentially stack +1/+1 counters on the creature board and push planeswalkers like Vraska, Betrayal's Sting directly into their ultimates — Doubling Season alone lets Vraska enter at 12 loyalty and fire immediately. The secondary plan is a poison clock backed by Prologue to Phyresis and an evasive attacker package including Thrummingbird, Grateful Apparition, and Vexing Radgull, where Atraxa's proliferate also advances poison counters on any opponent who gets tagged. The deck's spell package is unusually synergistic: cheap cantrip proliferate spells like Contentious Plan and Experimental Augury each trigger Flux Channeler for a bonus proliferate, Tekuthal, Inquiry Dominus doubles all of those triggers, and Brokers Confluence can deliver six proliferates as an instant at the right moment. Notable tech includes Astral Cornucopia as self-proliferating ramp and Agent Frank Horrigan as a combat-step double-proliferate that accelerates the end-game faster than Atraxa alone could manage.

**Consider adding (LLM flagged, not added):**
- `The Ozolith` — Harvests all the +1/+1 counters off creatures that die to removal or combat, then moves them onto Atraxa or a fresh attacker, protecting the deck's counter investment against the board wipes that would otherwise reset the snowball.

---

### yuriko_b5_ninja_tempo

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `["Thassa's Oracle", 'Demonic Consultation']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `144.3`
- creativity_delta_count: `34`
- novel_combo_count: `4`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `85`
- guard_fire_count: `0`
- sample forbidden: ["angel's grace", 'aphetto alchemist', 'approach of the second sun', 'beacon of immortality', 'benthic biomancer', "bolas's citadel", 'burning inquiry', 'cadaverous bloom']

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 1163 | 1247 | $0.0222 | 28.7 | - |
| C2_1_candidate_critic | True | 7354 | 3001 | $0.0671 | 53.0 | - |
| C2_2_wild_combo_discovery | True | 24353 | 942 | $0.0872 | 19.6 | - |
| D2_final_critic_batch_0 | True | 6222 | 1409 | $0.0398 | 30.4 | - |
| D2_final_critic_batch_1 | True | 6149 | 726 | $0.0293 | 14.4 | - |
| D2_final_critic_batch_2 | True | 6153 | 793 | $0.0304 | 18.3 | - |
| E_mana_base_critique | True | 523 | 360 | $0.0070 | 8.6 | - |

**10 rationale samples (verbatim):**
- **Thassa's Oracle** (user_intent|llm_rationale_rewrite): _The win condition that resolves instantly after Demonic Consultation exiles the library, and Silver-Fur Master's cost reduction on ninjutsu means you can keep more mana open on the combo turn to hold up interaction._
- **Demonic Consultation** (user_intent|llm_rationale_rewrite): _One-mana instant that names a card not in the deck to exile your entire library, setting up an immediate Thassa's Oracle win — Dark Ritual lets you assemble both pieces as early as turn one or two._
- **Commandeer** (archetype_staple): _Corpus staple for Yuriko, the Tiger's Shadow (usage_pct=0.71). [slot=flex]_
- **Ponder** (archetype_staple): _Corpus staple for Yuriko, the Tiger's Shadow (usage_pct=0.71). [slot=flex]_
- **Ingenious Infiltrator** (theme:TYPAL_NINJAS|llm_intent_extension|llm_rationale_rewrite): _Draws an additional card every time any ninja — including the many two-drop ninjas like Moon-Circuit Hacker, Ninja of the Deep Hours, and Saiba Cryptomancer — deals combat damage, turning a wide ninja attack into a draw-5 or more turn._
- **Moonblade Shinobi** (theme:TYPAL_NINJAS|llm_rationale_rewrite): _Creates a 1/1 illusion token on combat damage that immediately becomes a fresh ninjutsu enabler for Yuriko or any other ninja the following turn._
- **Mist-Syndicate Naga** (theme:TYPAL_NINJAS|llm_rationale_rewrite): _Self-replicates on combat damage, each token copy acting as another cheap body to bounce back to hand and re-ninjutsu Yuriko repeatedly in the same turn cycle._
- **Donatello, Gadget Master** (theme:TYPAL_NINJAS|llm_rationale_rewrite): _Provides artifact tokens to the team while his Turtle Ninja typing synergizes with Splinter, the Mentor's lord effect, and his gadget tokens feed Covert Technician's ability to cheat out artifacts from hand._
- **Lim-Dûl's Vault** (archetype_staple|llm_intent_extension): _Corpus staple for Yuriko, the Tiger's Shadow (usage_pct=0.79). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_
- **Temporal Trespass** (archetype_staple|llm_intent_extension|llm_rationale_rewrite): _An 11-CMC card that hits each opponent for 11 when flipped by Yuriko, and when actually cast via delve after filling the graveyard with Doom Whisperer surveil or Brainstorm discard, it gives the entire ninja squad another attack to find or set up the Demonic Consultation kill._

**Novel combo flags:**
- Demonic Consultation + Thassa's Oracle (Spellbook)  — Cast Demonic Consultation naming a card not in the deck to exile your entire library, then resolve Thassa's Oracle with an empty library and devotion to blue for an immediate win.
- Yuriko, the Tiger's Shadow + Temporal Trespass (Spellbook)  — Flip Temporal Trespass off Yuriko's trigger to deal 11 damage to each opponent simultaneously, often lethal when multiple Yuriko triggers resolve in one combat.
- Silver-Fur Master + Yuriko, the Tiger's Shadow (Spellbook)  — Silver-Fur Master reduces Yuriko's ninjutsu cost to one blue mana, enabling free re-ninjutsu after bouncing Yuriko to hand to replay her trigger every combat at minimal mana investment.
- Thousand-Faced Shadow + Yuriko, the Tiger's Shadow (NOVEL)  — Thousand-Faced Shadow deals combat damage and creates a token copy of Yuriko, putting two Yuriko triggers on the stack per combat for doubled top-card damage.
- Satoru, the Infiltrator + Ingenious Infiltrator (NOVEL)  — Each ninja entering tapped and attacking via ninjutsu triggers both Satoru's draw-a-card ability and Ingenious Infiltrator's draw-a-card ability simultaneously, drawing two cards per ninjutsu activation.
- Demonic Consultation + Thassa's Oracle (Spellbook)  — Name a card not in the deck with Demonic Consultation to exile the entire library, then cast Thassa's Oracle with an empty library for an immediate win. The primary win condition is already assembled.
- Doom Whisperer + Thassa's Oracle (NOVEL) [applied as swap] — Doom Whisperer lets you pay 2 life to surveil 2 at instant speed and repeatedly, allowing you to self-mill your library to near-zero on demand. With Thassa's Oracle on board or in hand, you can surveil until the library is empty enough to win immediately. This is a non-obvious, non-Spellbook line: surveil functions like a repeatable mini-mill engine that bypasses needing Demonic Consultation when life total allows, giving a resilient secondary path to Oracle victory.
- A-Lier, Disciple of the Drowned + Demonic Consultation (NOVEL) [applied as swap] — Lier makes all spells uncounterable AND gives flashback to instants/sorceries in your graveyard. On the kill turn: Lier on board means Demonic Consultation cannot be countered, protecting the primary combo line. If the first Consultation is countered before Lier resolves, a second copy from graveyard via flashback closes it out. This is a non-obvious protection-plus-redundancy piece that combo decks rarely run.

**Summary narrative:**

> The primary plan is to establish a turn-two Yuriko via zero-cost evasion creatures like Ornithopter or Changeling Outcast, then ninjutsu-chain ninjas every attack to flip high-CMC cards like Temporal Trespass and Silent-Blade Oni for lethal damage while drawing through the deck at speed. The secondary plan — and the fastest line to victory — is casting Demonic Consultation naming a card not in the deck to empty the library, then resolving Thassa's Oracle the same turn for an instant win, with Dark Ritual enabling this as early as turn one and A-Lier making both pieces uncounterable. Ingenious Infiltrator and Yuriko together turn any multi-ninja attack into a massive draw-and-damage event, meaning even disrupted combo attempts often generate enough card advantage to find a second attempt. Notable tech choices include Silver-Fur Master reducing ninjutsu costs across the board to conserve mana for interaction, and Doom Whisperer doubling as a 6-CMC Yuriko target and a library-stacking engine to guarantee devastating flips.

**Consider adding (LLM flagged, not added):**
- `Dress Down` — A one-turn flash enchantment that can blank opposing hate pieces like Torpor Orb or Hushbringer that shut off Yuriko's ninjutsu trigger and Thassa's Oracle's win condition, while also cycling out of hand as a high-impact interaction piece.
- `Mystic Confluence` — A modal instant with a CMC of 5 that triggers meaningful Yuriko damage and flexibly counters spells, bounces blockers, or refills your hand — all three modes being relevant in the same turn when you take an extra turn with Temporal Trespass.
- `Blinkmoth Infusion` — A CMC-14 instant-speed spell that hits each opponent for 14 on a Yuriko flip and can act as emergency artifact removal, making it the single highest-ceiling Yuriko trigger in the deck.

---

### ur_dragon_b3_dragon_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Dragon Tempest', 'Tiamat']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `140.7`
- creativity_delta_count: `40`
- novel_combo_count: `7`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `7`
- guard_fire_count: `0`
- sample forbidden: ['ancient gold dragon', 'astral dragon', 'cloudstone curio', 'dracogenesis', 'ganax, astral hunter', 'strionic resonator', 'vrondiss, rage of ancients']

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 714 | 1270 | $0.0212 | 27.2 | - |
| C2_1_candidate_critic | True | 6858 | 2813 | $0.0628 | 49.5 | - |
| C2_2_wild_combo_discovery | True | 25185 | 922 | $0.0894 | 20.3 | - |
| D2_final_critic_batch_0 | True | 6172 | 1364 | $0.0390 | 30.3 | - |
| D2_final_critic_batch_1 | True | 6105 | 786 | $0.0301 | 17.3 | - |
| D2_final_critic_batch_2 | True | 6102 | 750 | $0.0296 | 17.1 | - |
| E_mana_base_critique | True | 584 | 390 | $0.0076 | 9.6 | - |

**10 rationale samples (verbatim):**
- **Tiamat** (user_intent|llm_rationale_rewrite): _Casting Tiamat fetches five Dragons from the library — typically Terror of the Peaks, Scourge of Valkas, Old Gnawbone, and two threats — then with Dragon Tempest in play those Dragons land with haste and immediately start dealing damage before opponents can respond._
- **Dragon Tempest** (user_intent|llm_rationale_rewrite): _Every Dragon entering the battlefield gains haste and pings for damage equal to the Dragon count, meaning a Tiamat chain that drops four or five Dragons in a single turn can represent 30+ damage spread across players before combat even begins._
- **Savage Ventmaw** (archetype_staple|llm_rationale_rewrite): _Six free mana on attack enables casting a second Dragon the same turn in post-combat main — most commonly deploying Terror of the Peaks or Shivan Devastator from hand the turn after Tiamat has already stocked your hand._
- **Dragonspeaker Shaman** (archetype_staple): _Corpus staple for The Ur-Dragon (usage_pct=0.53). [slot=flex]_
- **Dragonborn Immolator** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Sacrificing token fodder from Nesting Dragon's Dragon Eggs or Smaug's Treasure hoard lets Dragonborn Immolator pump a key attacker like Shivan Devastator or Atsushi at instant speed, threatening lethal in a combat step where Dragon Tempest has already softened opponents._
- **Patron of the Arts** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Each Dragon entering through Tiamat's tutor chain creates a Treasure via Patron, converting your big spell turn into mana acceleration that can immediately recast Dragonspeaker Shaman or help pay for the next Dragon in hand._
- **Nesting Dragon** (llm_wild_combo_discovery|creative_outlier|llm_rationale_rewrite): _Each fetch land — Bloodstained Mire, Wooded Foothills, or Arid Mesa — triggers a Dragon Egg token, giving the deck cheap blockers that hatch into 2/2 flying Dragons that in turn add to Dragon Tempest's damage count._
- **Smaug** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Smaug floods the board with fourteen Treasure tokens when it enters, letting Galazeth Prismari turn those artifacts into mana on the following turn to deploy an entire hand of Dragons, while the Death trigger creates a 6/6 Dragon blocker if Smaug is removed._
- **Boneyard Scourge** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _When Crux of Fate destroys your opponents' creatures, any Dragon in your own graveyard lets you flash Boneyard Scourge in for free, immediately stacking another Dragon Tempest trigger and preserving board presence after a one-sided wipe._
- **Atsushi, the Blazing Sky** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _When Atsushi dies — whether to removal or sacrifice — it leaves behind three Treasures that accelerate your next Dragon drop or fund a mid-combat Tiamat cast, and the impulse-draw option can dig into Savage Ventmaw or Terror of the Peaks exactly when you need them._

**Novel combo flags:**
- Scourge of Valkas + Tiamat (Spellbook)  — Tiamat ETB tutors five Dragons; each Dragon entering the battlefield triggers Scourge of Valkas for increasing amounts of damage (1+2+3+4+5 = 15+ damage to one target or spread), closing games from full life.
- Terror of the Peaks + Dragon Tempest (Spellbook)  — Each Dragon entering triggers both simultaneously — Dragon Tempest deals damage equal to the number of Dragons and Terror of the Peaks deals damage equal to the Dragon's power — creating a double damage burst per Dragon entering.
- Old Gnawbone + Savage Ventmaw (NOVEL)  — Savage Ventmaw produces six mana on attack; if Old Gnawbone is also attacking, combat damage produces Treasures that effectively give the deck unlimited resources to deploy all Dragons in hand.
- Ebondeath, Dracolich + Bone Devourer (NOVEL)  — Sacrifice Ebondeath to Bone Devourer to grow it; then whenever another non-token creature dies this turn, cast Ebondeath from the graveyard for free, enabling repeated sacrifice triggers.
- Maelstrom of the Spirit Dragon + Tiamat (NOVEL)  — Tiamat fills the graveyard with searched-up Dragons that got countered or died; Maelstrom recursively returns them to hand for follow-up casts that generate additional Dragon Tempest damage triggers.
- Nesting Dragon + Scourge of Valkas (NOVEL) [applied as swap] — Each landfall trigger creates a Dragon Egg token; when any egg dies it becomes a 2/2 Dragon with flying that enters and pings every player equal to the number of dragons you control via Scourge of Valkas. Late-game land drops chain into a cascade of ETB damage without needing combat, and the tokens are valid Tiamat/Ur-Dragon tribal fuel.
- Sarkhan the Masterless + Terror of the Peaks (NOVEL) [applied as swap] — Sarkhan's +1 turns each planeswalker into a 4/4 Dragon until end of turn. Those newly-Dragon-typed tokens entering the battlefield each trigger Terror of the Peaks to deal damage equal to their power (4) to any target per dragon-token entering. With multiple planeswalkers this can deal 4+ damage per walker to face at instant speed during your combat step, supplementing the main Dragon Tempest win-line with a second independent damage source that opponents can't easily block.
- Inferno of the Star Mounts + Dragon Tempest (NOVEL) [applied as swap] — Inferno of the Star Mounts enters and immediately triggers Dragon Tempest to deal direct damage equal to the number of dragons you control. Because Inferno is uncounterable and has built-in haste, in a dragon-heavy board state you can pay {R} repeatedly to pump it to power 20 in the same turn it enters, then the static ability fires for an additional 20 damage to any target—a second independent kill shot layered on top of Dragon Tempest's entry trigger.
- Terror of the Peaks + Scourge of Valkas (Spellbook)  — Both trigger independently on each Dragon ETB. With The Ur-Dragon's eminence reducing costs and cascading into more dragons, each new dragon creates two damage triggers that stack—Terror deals damage equal to the entering dragon's power, Scourge deals damage equal to total dragons in play. Together they can close games through damage alone without needing combat.
- Old Gnawbone + Savage Ventmaw (NOVEL)  — Savage Ventmaw already untaps into six mana mid-combat; Old Gnawbone converts combat damage to Treasures. If both attack into an open board, the Treasures from Old Gnawbone's trigger combine with Ventmaw's free mana to let you cast additional dragons at flash speed via The Ur-Dragon's eminence during combat, each new entry triggering Dragon Tempest for lethal damage in a single attack step.

**Summary narrative:**

> The deck's primary plan is to ramp into Tiamat, fetch a critical mass of Dragons including Terror of the Peaks and Scourge of Valkas, then with Dragon Tempest already in play convert every ETB trigger into a wave of direct damage that softens or eliminates opponents before a hasty combat swing closes the game. The secondary plan leans on The Ur-Dragon's eminence discount and combat-value trigger to sustain momentum across multiple turns, using Old Gnawbone's Treasure generation off Dragon combat damage to refuel and chain additional Dragons from hand. Notable tech includes fetch lands that double as Nesting Dragon landfall triggers to build a token army of Dragon Eggs, and Smoldering Egg as a hidden haste Dragon that reliably flips on the same turn Tiamat is cast, adding a surprise extra body and damage trigger to an already explosive chain.

**Consider adding (LLM flagged, not added):**
- `Temur Ascendancy` — Acts as a second Dragon Tempest-style haste enabler and refills your hand each time a Dragon with power four or greater enters, providing card draw redundancy that is critical when Tiamat has already been cast and your hand is empty.
- `Utvara Hellkite` — Attacking with any Dragon spawns an additional 6/6 Dragon token, and each of those tokens entering the battlefield adds another Dragon Tempest damage trigger, creating an exponential feedback loop that can end the game in a single combat step.
- `Swiftfoot Boots` — Hexproof and haste on The Ur-Dragon means opponents cannot answer the commander with targeted removal the turn it lands, protecting the eminence source and the free-permanent trigger that is central to the deck's long-game value engine.

**Ur-Dragon envelope check:**
- hellkite_in_deck: `False`
- gnawbone_in_deck: `True`
- hellkite_blocked_by_guard: `False`
- gnawbone_blocked_by_guard: `False`
- deck_clean: `False`
- held_by_design: `False`

---

## Iteration 3 → Iteration 4 hand-off (to fill after sweep)

(See progress log for live commentary; this section will be filled after this report's data is reviewed.)
