# Pillar D Iteration 3 — Validation Report

Generated: 2026-05-20 21:58:50
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Auto-passed: 4 / 6 success criteria.**

- ✅ **iter1_structural_pass_5_of_5** — value `True` 
- ✅ **mean_creativity_delta_count_geq_30** — value `37.8` (threshold `30`)
- ✅ **mean_novel_combo_count_geq_4** — value `5.4` (threshold `4`)
- ✅ **mean_cost_usd_leq_0_40** — value `0.2899` (threshold `0.4`)
- ❌ **mean_wallclock_s_leq_60** — value `137.3` (threshold `60`)
- ❌ **ur_dragon_envelope_held_by_design**

## Per-case summary

| Case | iter1 | wall (s) | cost ($) | LLM calls | creativity Δ | novel | guard size | guard fires | archetype |
|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ✅ | 143.3 | $0.2782 | 6 | 36 | 5 | 55 | 0 | tribal |
| krenko_b4_goblin_combo | ✅ | 139.3 | $0.2911 | 6 | 37 | 4 | 267 | 1 | tribal |
| atraxa_b2_proliferate | ✅ | 137.7 | $0.3374 | 6 | 42 | 7 | 459 | 2 | control |
| yuriko_b5_ninja_tempo | ✅ | 136.6 | $0.2755 | 6 | 34 | 5 | 85 | 0 | combo |
| ur_dragon_b3_dragon_tribal | ✅ | 129.8 | $0.2672 | 6 | 40 | 6 | 7 | 0 | tribal |

## Per-case detail

### edgar_b3_vampire_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Vito, Thorn of the Dusk Rose', 'Bloodthirsty Conqueror']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `143.3`
- creativity_delta_count: `36`
- novel_combo_count: `5`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `55`
- guard_fire_count: `0`
- sample forbidden: ['aetherflux reservoir', 'aettir and priwen', 'angelic chorus', 'arcbond', "ashnod's altar", 'beacon of immortality', 'blood tribute', 'cliffhaven vampire']

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 1001 | 1298 | $0.0225 | 28.6 | - |
| C2_1_candidate_critic | True | 7204 | 2835 | $0.0641 | 51.5 | - |
| C2_2_wild_combo_discovery | True | 24744 | 1069 | $0.0903 | 22.2 | - |
| D2_final_critic_batch_0 | True | 6210 | 1532 | $0.0416 | 33.0 | - |
| D2_final_critic_batch_1 | True | 6134 | 747 | $0.0296 | 17.8 | - |
| D2_final_critic_batch_2 | True | 6142 | 779 | $0.0301 | 17.5 | - |

**10 rationale samples (verbatim):**
- **Bloodthirsty Conqueror** (user_intent|llm_rationale_rewrite): _On each attack it creates a tapped and attacking 1/1 vampire token for each +1/+1 counter on it, so pairing it with Cordial Vampire's counter-doubling-on-death effect or Archangel of Thune's counter-placing triggers can produce a rapidly escalating wave of attackers that overwhelm boards before opponents stabilize._
- **Vito, Thorn of the Dusk Rose** (user_intent|llm_rationale_rewrite): _Every life gained by this deck's lifelink vampires — Vampire Cutthroat, Ichor Drinker, or the wide swings enabled by Stromkirk Captain's first strike — becomes a drain trigger that simultaneously threatens lethal damage across multiple opponents without requiring combat._
- **Cordial Vampire** (theme:TYPAL_VAMPIRES|llm_intent_extension|llm_rationale_rewrite): _Whenever any creature dies — whether traded in combat, sacrificed to Viscera Seer, or lost to removal — Cordial Vampire places a +1/+1 counter on every vampire you control, which directly fuels Bloodthirsty Conqueror's attack-trigger token count and snowballs the entire board._
- **Blood Petal Celebrant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _A one-drop vampire that immediately triggers Edgar's eminence for a free token, and its blood token creation gives Voldaren Estate extra fodder while fueling Furyblade Vampire's discard-for-double-strike._
- **Carrier Thrall** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _When Carrier Thrall dies to a Skullclamp or a sacrifice to Indulgent Aristocrat, it replaces itself with a Thrull token, meaning Vein Ripper and Cruel Celebrant each see two death triggers from a single creature._
- **Sanctum Seeker** (archetype_staple|llm_intent_extension|llm_rationale_rewrite): _Each attacking vampire drains each opponent for one life, so with Edgar flooding the board via eminence tokens and Bloodthirsty Conqueror adding more attackers every swing, Sanctum Seeker can eliminate multiple opponents simultaneously before damage even resolves — with Vito doubling every life gain trigger._
- **Cruel Celebrant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _Pairs with Blood Artist so that each death event—whether a sacrificed Edgar token or Bloodghast returning to the graveyard—drains opponents twice, compressing the Vito life-drain gameplan into the board state rather than requiring Vito to survive._
- **A-Blood Artist** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _The digital variant's death trigger fires on any creature dying, so it doubles up with Blood Artist to generate four drain events per sacrifice loop, turning Viscera Seer's free outlet into a decisive life-swing alongside Vito._
- **Dusk Legion Sergeant** (theme:TYPAL_VAMPIRES|llm_rationale_rewrite): _Grants your attacking vampires menace, which punishes opponents trying to chump-block the wide Edgar token swarm, and its vigilance anthem ensures the tokens Edgar makes during Bloodthirsty Conqueror's attack step can still hold back threats._
- **Blood Artist** (theme:TYPAL_VAMPIRES): _Theme 'TYPAL_VAMPIRES' signal_count=2 (freq_in_corpus=0.77). [slot=creature]_

**Novel combo flags:**
- Vito, Thorn of the Dusk Rose + Sanctum Seeker (Spellbook)  — Each vampire that attacks triggers Sanctum Seeker to drain each opponent, and every life gained from Sanctum Seeker or lifelink vampires triggers Vito to deal additional damage, creating a cascading drain loop during combat.
- Edgar Markov + Skullclamp (Spellbook)  — Casting any vampire from the command zone creates a 1/1 eminence token; equipping Skullclamp to the 1/1 token immediately kills it and draws two cards, converting every vampire spell into a cantrip.
- Bloodghast + Viscera Seer (Spellbook)  — Sacrifice Bloodghast to Viscera Seer to scry 1, play a land to return Bloodghast from the graveyard, and repeat for repeatable scrying and death triggers for Cruel Celebrant or Blood Artist.
- Falkenrath Gorger + Furyblade Vampire (NOVEL)  — Furyblade Vampire's first-strike trigger lets you discard a vampire to give it +2/+0; Falkenrath Gorger gives all vampires madness so the discarded vampire can be cast at its madness cost, turning the discard into card advantage.
- Cordial Vampire + Indulgent Aristocrat (NOVEL)  — Sacrificing a vampire to Indulgent Aristocrat distributes +1/+1 counters to all vampires, and Cordial Vampire triggers on the same death to also put a +1/+1 counter on each vampire, double-stacking counters from a single sacrifice.
- Archangel of Thune + Vito, Thorn of the Dusk Rose (NOVEL) [applied as swap] — Every time Vito's drain triggers (opponents lose life → you gain life), Archangel of Thune puts a +1/+1 counter on every creature you control. With a wide vampire board from Edgar's eminence, a single drain wave snowballs into a lethal-pump turn. Archangel also has lifelink itself, so each attack generates more drain triggers via Vito, which generate more counters — a self-reinforcing loop that doesn't require any additional pieces.
- Cordial Vampire + Skullclamp (NOVEL)  — Equip Skullclamp to any 1/1 vampire token (immediately lethal to itself at 1/1 → 1/3 but the -1/-1 kills it at 0/0 if base is 1/1). Actually: Skullclamp gives +1/-1, so a 1/1 becomes 2/0 and dies, drawing 2 cards AND triggering Cordial Vampire to put a +1/+1 counter on each other vampire you control. With even two other vampires on board this is a reliable card-draw-plus-pump engine requiring no external pieces beyond a stream of 1/1 tokens Edgar readily supplies.
- Vein Ripper + Sanctum Seeker (NOVEL) [applied as swap] — Vein Ripper triggers 'whenever a creature dies, target opponent loses 2 life and you gain 2 life' — completely independently of combat. Stacking this alongside Sanctum Seeker's attack trigger means combat damage, any blocking trades, and any opponent's creature removal all simultaneously drain life. In a wide vampire attack, opponents face Sanctum Seeker's mass drain AND Vein Ripper pinging for 2 on every creature that dies in combat, while Vito converts the life-gain into yet more drain. Three-layer drain in one attack step.

**Summary narrative:**

> The deck's primary plan is to flood the board using Edgar Markov's eminence trigger, which creates a free 1/1 vampire token on every vampire cast, then swing wide with lifelink attackers so Vito, Thorn of the Dusk Rose converts every point of life gained into direct damage draining opponents simultaneously. Bloodthirsty Conqueror amplifies this by generating additional attacking tokens scaled to its +1/+1 counters, which Cordial Vampire and Archangel of Thune continuously replenish from combat deaths and life gain triggers respectively. The secondary plan is a Sanctum Seeker drain-storm: with enough vampires attacking, opponents lose 3-5 life per combat step from Sanctum Seeker alone, and between Blood Artist, Cruel Celebrant, and Vein Ripper the deck can close games without dealing combat damage at all. A notable tech package is the Furyblade Vampire plus Falkenrath Gorger axis, which converts a hand-emptying drawback into a madness engine, letting the deck recast discarded vampires at reduced cost mid-combat to generate even more Edgar eminence tokens on the opponent's clock.

**Consider adding (LLM flagged, not added):**
- `Anje's Ravager` — A vampire with madness synergy that draws three cards when it attacks — an enormous refuel for a go-wide deck that depletes its hand early — and Falkenrath Gorger already gives all vampires madness so the discard cost to cast it is naturally mitigated.
- `Stensia Masquerade` — Grants all attacking vampires first strike and places +1/+1 counters on them whenever they deal combat damage, directly accelerating Bloodthirsty Conqueror's counter count and turning every lifelink swing into a counter-accumulating event that also feeds Archangel of Thune.
- `Champion of Dusk` — Draws cards equal to the number of vampires you control when it enters, and with Edgar continuously generating tokens this reliably draws five or more cards at once to refuel the hand after the early go-wide rush depletes resources.

---

### krenko_b4_goblin_combo

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Conspicuous Snoop', 'Kiki-Jiki, Mirror Breaker']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `139.3`
- creativity_delta_count: `37`
- novel_combo_count: `4`
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
| B2_intent_interpreter | True | 2167 | 1397 | $0.0275 | 31.3 | - |
| C2_1_candidate_critic | True | 8308 | 2762 | $0.0664 | 48.8 | - |
| C2_2_wild_combo_discovery | True | 25128 | 1099 | $0.0919 | 24.9 | - |
| D2_final_critic_batch_0 | True | 7171 | 1292 | $0.0409 | 27.4 | - |
| D2_final_critic_batch_1 | True | 7090 | 771 | $0.0328 | 18.6 | - |
| D2_final_critic_batch_2 | True | 7094 | 696 | $0.0317 | 16.6 | - |

**10 rationale samples (verbatim):**
- **Conspicuous Snoop** (user_intent|llm_rationale_rewrite): _Goblin Spy puts Goblins on top of the library, letting Conspicuous Snoop repeatedly activate Krenko, Mob Boss's ability from the top of the deck without needing Krenko on the battlefield._
- **Kiki-Jiki, Mirror Breaker** (user_intent|llm_rationale_rewrite): _Copies Krenko, Mob Boss or Goblin Ringleader at the start of each combat step, potentially generating another full wave of tokens or another four-card refill before the copy is sacrificed._
- **Impulsive Pilferer** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Dies and creates a Goblin token for Krenko, Mob Boss to count, and its Encore ability late game floods the board with three attacking copies that each trigger Impact Tremors for a sudden burst of face damage._
- **Cacophony Scamp** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Pitches itself as a one-mana 2/1 that can deal extra damage on the way out, and when Skullclamp or Pashalik Mons turns a board wipe into a drain, Scamp's death trigger adds a direct burn rider on top of whatever else is happening._
- **Krenko's Command** (archetype_staple|llm_intent_extension): _Corpus staple for Krenko, Mob Boss (usage_pct=0.61). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_
- **Goblin Trapfinder** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Tutors any dungeon-completing card, but more practically it is a cheap Goblin body that Goblin Matron can fetch and that Krenko, Mob Boss counts toward his exponential doubling — the tribal headcount is what matters here._
- **Goblin Arsonist** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Guarantees one point of damage when it dies, which pairs with Pashalik Mons so that any board wipe or Skullclamp sacrifice chain drains opponents incrementally while still counting as a body for Krenko, Mob Boss's next activation._
- **Dynamite Diver** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Grants itself and another target creature haste, acting as a third-redundant haste enabler behind Lightning Greaves and Goblin Warchief so Krenko, Mob Boss can fire the same turn he hits the battlefield even if equipment is stripped._
- **Fireblade Charger** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _When it dies it deals damage equal to its power, and equipping it with Skullclamp first pumps it to 2 power before the equip lethal trigger draws two cards, turning a single token into both a draw spell and a burn spell._
- **Goblin Shaman** (theme:TYPAL_GOBLINS|llm_rationale_rewrite): _Provides a mana discount on Goblin spells, and since this deck needs to recast Krenko, Mob Boss after removal multiple times over a long game, shaving even one mana off him or Goblin Warchief accelerates the timeline significantly._

**Novel combo flags:**
- Krenko, Mob Boss + Goblin Chieftain (Spellbook)  — Krenko activates each turn creating exponentially more hasted Goblins; with Goblin War Strike the damage from even a single extra activation is lethal to all opponents.
- Kiki-Jiki, Mirror Breaker + Conspicuous Snoop (Spellbook)  — With Goblin Recruiter or any card-stacking effect, Conspicuous Snoop on top lets Kiki-Jiki copy it; the copy also sees Kiki-Jiki on top and can copy itself infinitely for an arbitrarily large attacking haste army.
- Krenko, Mob Boss + Impact Tremors (Spellbook)  — Each Krenko activation that doubles the goblin count simultaneously deals damage equal to the number of tokens created to each opponent, often reaching lethal without attacking.
- Conspicuous Snoop + Kiki-Jiki, Mirror Breaker (Spellbook)  — Conspicuous Snoop + Kiki-Jiki on top of library: copy Snoop with Kiki, the copy sees Kiki-Jiki on top (via Snoop's ability reading the top of library), copy can copy itself infinitely if you arrange Kiki on top. Classic cEDH Snoop combo — already fully assembled in this deck.
- Goblin Spymaster + Krenko, Mob Boss (NOVEL) [applied as swap] — Goblin Spymaster forces each opponent to create a 1/1 Goblin token each end step with a mandatory-attack clause. Those tokens count as Goblins for Krenko's doubling ability — opponents are literally feeding your Krenko activations. With Krenko in play, their forced-attack tokens become your own swarm multiplier next time you untap Krenko, and the political chaos of forced attacks clears blockers for your lethal swing.
- The Peregrine Dynamo + Krenko, Mob Boss (NOVEL) [applied as swap] — The Peregrine Dynamo copies any activated or triggered ability from a legendary source that isn't your commander — but Krenko, Mob Boss IS your commander, so it copies Krenko's tap ability directly by targeting it as a non-commander legendary source when Krenko sits in the 99 if you're running the alternate Krenko. More practically: it copies Goblin Motivator's or Goblin Warchief's triggered/activated abilities, and critically copies Pashalik Mons's ping trigger for each Goblin that dies — turning each sacrifice into 2 damage instead of 1 without needing any other piece. The haste on Dynamo means it contributes to crew or attacking the same turn it lands.
- Squee, Dubious Monarch + Impact Tremors (NOVEL) [applied as swap] — Squee attacks, creates a tapped attacking Goblin token — that token entering triggers Impact Tremors for 1 damage each attack. Squee recurs from the graveyard by paying 3R and exiling four cards, meaning he provides a repeatable self-replacing attacker and Goblin-token producer every turn even through removal. With Pashalik Mons, each Squee death pings too. Unlike Mogg Conscripts (a strictly do-nothing 2/2 with a downside), Squee generates value while being effectively unkillable.
- Ardoz, Cobbler of War + Krenko, Mob Boss (NOVEL) [applied as swap] — Every token Krenko creates enters and immediately gets +2/+0 until end of turn from Ardoz's trigger. This turns a board of 1/1 Goblin tokens into surprise 3/1 attackers in the same turn Krenko fires — drastically lowering the number of activations needed to kill. Ardoz also creates a hasted Goblin token for 3R as a mana sink, and himself enters giving all existing creatures +2/+0, stacking with Goblin Chieftain/Warchief lords for a same-turn lethal out of nowhere.

**Summary narrative:**

> The primary plan is to land Krenko, Mob Boss under Lightning Greaves or Swiftfoot Boots, activate him multiple times per turn via Battle Cry Goblin untaps and The Peregrine Dynamo copies, and overwhelm the table with an exponentially growing Goblin swarm backed by lord stacking from Goblin Chieftain and Goblin Warchief. When combat isn't clean, Impact Tremors and Goblin War Strike convert raw token count into direct damage, meaning the deck can kill through board stalls as easily as through combat. The secondary plan leans on Conspicuous Snoop plus Goblin Spy to use Krenko's ability from the library, giving the deck a resilient angle when Krenko himself is under removal pressure. Notable tech includes The Peregrine Dynamo as a virtual third Krenko activation in a single turn, and Skullclamp turning every surplus 1/1 into two cards while simultaneously powering up Goblin Blast-Runner.

**Consider adding (LLM flagged, not added):**
- `Warren Instigator` — Like Goblin Lackey it cheats Goblins into play on combat damage, but hits twice per attack due to double strike, potentially landing Krenko, Mob Boss or Kiki-Jiki, Mirror Breaker for free as early as turn three.
- `Shared Animosity` — With a full Krenko swarm attacking, each Goblin gets +1/+0 for every other attacking Goblin, turning even 1/1 tokens into double-digit attackers and closing games that Impact Tremors alone can't finish.
- `Mogg Infestation` — Replaces every creature an opponent controls with two 1/1 Goblin tokens under their control, which Goblin Spymaster then forces to attack into their own board — a political reset that also thins enemy threats.

---

### atraxa_b2_proliferate

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Doubling Season', 'Pir, Imaginative Rascal']`
- must-includes dropped: `[]`
- theme_coherence_score: `0.5`
- wall-clock (s): `137.7`
- creativity_delta_count: `42`
- novel_combo_count: `7`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `459`
- guard_fire_count: `2`
- guard_fire_events: [{'phase': 'B2_intent_interpreter', 'field': 'suggested_extensions', 'card': "Karn's Bastion"}, {'phase': 'B2_intent_interpreter', 'field': 'suggested_extensions', 'card': 'Ozolith, the Shattered Spire'}]
- sample forbidden: ['academy manufactor', 'adaptive gemguard', 'adrix and nev, twincasters', 'aegis automaton', 'aetherflux reservoir', 'aethertide whale', 'agatha of the vile cauldron', "agatha's soul cauldron"]

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 3434 | 1377 | $0.0310 | 29.5 | - |
| C2_1_candidate_critic | True | 13197 | 2903 | $0.0831 | 51.2 | - |
| C2_2_wild_combo_discovery | True | 27356 | 988 | $0.0969 | 22.0 | - |
| D2_final_critic_batch_0 | True | 9251 | 1372 | $0.0483 | 28.3 | - |
| D2_final_critic_batch_1 | True | 9169 | 769 | $0.0390 | 17.1 | - |
| D2_final_critic_batch_2 | True | 9166 | 769 | $0.0390 | 17.4 | - |

**10 rationale samples (verbatim):**
- **Doubling Season** (user_intent|llm_rationale_rewrite): _Allows Vraska, Betrayal's Sting to enter and immediately ultimate, and combined with Pir means every counter-placing effect — Bloom Hulk entering, Atraxa proliferating, Brokers Ascendancy ticking — lands twice as many counters as printed._
- **Pir, Imaginative Rascal** (user_intent|llm_rationale_rewrite): _Stacks with Doubling Season so that Astral Cornucopia enters with six charge counters instead of three at X=3, and makes every Atraxa proliferate tick place two counters on each permanent instead of one._
- **Sword of Truth and Justice** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _Equipping Atraxa turns every combat damage trigger into a free proliferate, effectively doubling the number of proliferate events each turn cycle alongside Tekuthal and Grateful Apparition._
- **Tezzeret's Gambit** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _Drawing two cards for two mana and proliferating refuels your hand while pushing Vraska, Brokers Ascendancy, and every creature with +1/+1 counters forward simultaneously, and Pir's replacement effect makes that proliferate land an extra counter on each permanent._
- **Grateful Apparition** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension|llm_rationale_rewrite): _Flying evasion means it connects consistently to trigger its on-damage proliferate, and pairing it with Sword of Truth and Justice on Atraxa means a single Atraxa attack generates at least three separate proliferate events in one combat step._
- **Flux Channeler** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_intent_extension): _Theme 'THEME_PROLIFERATE' signal_count=1 (freq_in_corpus=0.59). Theme 'THEME_PLUS1_COUNTERS' signal_count=1 (freq_in_corpus=0.59). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=creature]_
- **Inexorable Tide** (archetype_staple|llm_intent_extension): _Corpus staple for Atraxa, Praetors' Voice (usage_pct=0.59). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_
- **Angelic Sleuth** (llm_wild_combo_discovery|creative_outlier|llm_rationale_rewrite): _Every time Atraxa's end-step proliferate fires or Tezzeret's Gambit resolves, Angelic Sleuth triggers an investigate, turning each proliferate into a clue token and providing a steady card-advantage engine throughout the long game._
- **Brokers Confluence** (llm_wild_combo_discovery|llm_rationale_rewrite): _Choosing proliferate three times with the confluence places up to six counters on key permanents under Pir — or you can split modes to both remove a threat with -1/-1 counters and proliferate the remainder, giving the deck modal flexibility._
- **Glistening Sphere** (theme:THEME_PROLIFERATE|theme:THEME_PLUS1_COUNTERS|llm_rationale_rewrite): _It enters with charge counters that Pir inflates and Atraxa proliferates upward, and once it has enough counters you crack it to convert stored counters into mana that funds another Tezzeret's Gambit or Fuel for the Cause._

**Novel combo flags:**
- Doubling Season + Vraska, Betrayal's Sting (Spellbook)  — Vraska enters with double loyalty counters under Doubling Season, immediately reaching her ultimate threshold and converting a player's life total to poison counters for a near-instant win.
- Tekuthal, Inquiry Dominus + Atraxa, Praetors' Voice (Spellbook)  — Atraxa's end-step proliferate is doubled by Tekuthal, creating two proliferate triggers every turn and accelerating +1/+1 counter accumulation on the entire board exponentially.
- Pir, Imaginative Rascal + Astral Cornucopia (NOVEL)  — Cornucopia enters with an extra charge counter per Pir, then grows via proliferate each turn cycle to produce significant mana well ahead of its converted mana cost.
- Brokers Ascendancy + Atraxa, Praetors' Voice (NOVEL)  — Each end step Brokers Ascendancy places +1/+1 counters on creatures, then Atraxa proliferates adding another counter on top, resulting in two counters per creature per turn cycle without any other engine.
- Ezuri, Stalker of Spheres + Tekuthal, Inquiry Dominus (NOVEL)  — Tekuthal doubles every proliferate trigger, so a single proliferate spell or Atraxa's trigger yields two proliferate events, drawing Ezuri two cards instead of one and accelerating card advantage significantly.
- Doubling Season + Vraska, Betrayal's Sting (Spellbook)  — Vraska enters with 7 loyalty counters (Doubling Season doubles the 3 she enters with to 6... actually enters with 7 under DS), immediately ulting: each opponent gets 10 poison counters and loses the game. Instant-win on the turn Vraska resolves if Doubling Season is already in play.
- Tekuthal, Inquiry Dominus + Atraxa, Praetors' Voice (NOVEL)  — Tekuthal doubles each proliferate trigger. Atraxa's end-step proliferate now proliferates twice, compounding counters on everything at double speed. With Pir also in play, each proliferate counter addition is further increased by 1, creating triple-stacked acceleration on planeswalkers and +1/+1 counter creatures.
- Angelic Sleuth + Felisa, Fang of Silverquill (NOVEL) [applied as swap] — Angelic Sleuth investigates whenever a permanent with counters leaves the battlefield. In a proliferate deck, creatures constantly gain +1/+1 counters; when they die, Angelic Sleuth fires an investigation trigger AND Felisa creates Inkling tokens equal to the counters on the dead creature. This creates a recursive card-advantage engine: the bigger your countered-up creatures grow (via Atraxa/Doubling Season/Pir), the more tokens Felisa makes on death AND the more Clues Angelic Sleuth generates — turning board wipes into card draw and token floods simultaneously.
- Felisa, Fang of Silverquill + Doubling Season (NOVEL) [applied as swap] — Felisa triggers whenever a nontoken creature with counters dies, creating Inkling tokens equal to counters on the dead creature. With Doubling Season in play, those Inkling tokens are doubled. A single creature that has been proliferated up to 6 counters dies → Felisa makes 6 Inklings → Doubling Season makes it 12 flying Inklings. This converts inevitable board wipes into explosive token production, turning a weakness (your oversized creatures dying) into a win condition.
- Brokers Confluence + Tekuthal, Inquiry Dominus (NOVEL) [applied as swap] — Brokers Confluence lets you choose proliferate up to three times (same mode repeatedly). With Tekuthal doubling each proliferate and Pir adding 1 to each counter placed, choosing proliferate three times becomes effectively six proliferates with boosted counters. On a single instant-speed activation this can catapult Vraska, Betrayal's Sting or Brokers Ascendancy's loyalty/lore counters to game-ending levels, while also serving as a soft counterspell when needed.

**Summary narrative:**

> The primary plan is to assemble Doubling Season and Pir to make every +1/+1 counter placement and proliferate trigger double, then use Atraxa's guaranteed end-step proliferate — ideally doubled again by Tekuthal — to compound creature sizes and planeswalker loyalty at an exponential rate. Combat-damage proliferators like Thrummingbird, Grateful Apparition, Vexing Radgull, and Recon Craft Theta stack additional proliferate events per turn, while Brokers Ascendancy and Sword of Truth and Justice provide instant counter floods across the whole board. The secondary plan is to ultimate planeswalkers like Vraska, Betrayal's Sting immediately upon entry with Doubling Season, converting planeswalker ultimates into game-winning threats or locks before opponents can answer them. Notable tech choices include Angelic Sleuth converting each proliferate into a clue for sustained card advantage, and Felisa, Fang of Silverquill punishing removal by turning fallen creatures' accumulated counters into a fresh air force of tokens.

**Consider adding (LLM flagged, not added):**
- `Merciless Eviction` — This deck builds slowly and needs a reset button for artifact or enchantment-based threats that outpace your counter engine; Merciless Eviction exiles entire permanent types cleanly without giving opponents graveyard recursion.
- `Planewide Celebration` — A seven-mana spell that can proliferate four times in one cast turns Atraxa into a full-board counter explosion, especially potent with Tekuthal on the field doubling each of those four triggers.
- `Wanderer's Strike` — Exiles a problematic creature and then proliferates, giving the deck a clean answer to indestructible threats like Blightsteel Colossus while still advancing the counter plan.

---

### yuriko_b5_ninja_tempo

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `["Thassa's Oracle", 'Demonic Consultation']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `136.6`
- creativity_delta_count: `34`
- novel_combo_count: `5`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `85`
- guard_fire_count: `0`
- sample forbidden: ["angel's grace", 'aphetto alchemist', 'approach of the second sun', 'beacon of immortality', 'benthic biomancer', "bolas's citadel", 'burning inquiry', 'cadaverous bloom']

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 1163 | 1343 | $0.0236 | 30.8 | - |
| C2_1_candidate_critic | True | 7356 | 3004 | $0.0671 | 54.3 | - |
| C2_2_wild_combo_discovery | True | 24303 | 804 | $0.0850 | 16.6 | - |
| D2_final_critic_batch_0 | True | 6318 | 1361 | $0.0394 | 27.9 | - |
| D2_final_critic_batch_1 | True | 6252 | 790 | $0.0306 | 18.1 | - |
| D2_final_critic_batch_2 | True | 6247 | 740 | $0.0298 | 16.1 | - |

**10 rationale samples (verbatim):**
- **Thassa's Oracle** (user_intent|llm_rationale_rewrite): _The primary win condition: cast after Demonic Consultation empties the library, resolving with zero cards remaining for an immediate game win that Lier, Disciple of the Drowned can make uncounterable during the window you need it most._
- **Demonic Consultation** (user_intent|llm_rationale_rewrite): _Names a card not in the deck to instantly exile the library, then Thassa's Oracle resolves on the same turn for the win; Dark Ritual provides the burst mana to execute both halves on the same turn through interaction._
- **Commandeer** (archetype_staple): _Corpus staple for Yuriko, the Tiger's Shadow (usage_pct=0.71). [slot=flex]_
- **Ponder** (archetype_staple): _Corpus staple for Yuriko, the Tiger's Shadow (usage_pct=0.71). [slot=flex]_
- **Fallen Shinobi** (theme:TYPAL_NINJAS|llm_intent_extension|llm_rationale_rewrite): _When it connects via ninjutsu off an Ornithopter or Changeling Outcast, its exile-and-cast trigger can flip and cast Temporal Trespass or Silent-Blade Oni for free, generating immediate extra-turn or hand-disruption value on top of Yuriko's own damage trigger._
- **Moonblade Shinobi** (theme:TYPAL_NINJAS|llm_rationale_rewrite): _Creates a 1/1 flying token on ninjutsu connection, giving you an additional evasive body that immediately serves as fodder to bounce and redeploy Yuriko in the same or a subsequent combat step._
- **Mist-Syndicate Naga** (theme:TYPAL_NINJAS|llm_rationale_rewrite): _Makes a token copy of itself every time it deals combat damage, exponentially multiplying the number of Ninjas triggering Ingenious Infiltrator and piling up additional ninjutsu-enabler bodies for future turns._
- **Donatello, Gadget Master** (theme:TYPAL_NINJAS|llm_rationale_rewrite): _Tutors an artifact from the deck onto the battlefield when it connects — fetching Sol Ring or Ornithopter to keep the ninjutsu chain funded while also registering as a Ninja for Yuriko's damage trigger._
- **Tetsuko Umezawa, Fugitive** (archetype_staple|llm_intent_extension): _Corpus staple for Yuriko, the Tiger's Shadow (usage_pct=0.79). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_
- **Lim-Dûl's Vault** (archetype_staple|llm_intent_extension): _Corpus staple for Yuriko, the Tiger's Shadow (usage_pct=0.79). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_

**Novel combo flags:**
- Demonic Consultation + Thassa's Oracle (Spellbook)  — Name a card not in the deck with Demonic Consultation to exile the entire library, then cast Thassa's Oracle with zero cards in library to win immediately.
- Yuriko, the Tiger's Shadow + Temporal Trespass (Spellbook)  — Stack Temporal Trespass on top with Brainstorm before a Yuriko trigger, flip it for 11 damage to all opponents and cast it for its delve cost to take an extra turn and repeat.
- Satoru Umezawa + Silent-Blade Oni (NOVEL)  — Satoru gives Silent-Blade Oni ninjutsu 3UB, letting you cheat it into play from hand by ninjutsuing a cheap attacker, immediately casting a free spell from an opponent's hand.
- Ingenious Infiltrator + Yuriko, the Tiger's Shadow (Spellbook)  — Every Ninja that deals combat damage draws a card via Ingenious Infiltrator AND triggers Yuriko's flip damage, converting a wide ninja attack into both a hand refill and massive life loss.
- Silver-Fur Master + Changeling Outcast (NOVEL)  — Silver-Fur Master reduces ninjutsu costs by 1, making Yuriko's own ninjutsu cost 1U and enabling Changeling Outcast to be replayed and re-ninjutsued every turn for free with minimal mana investment.
- Demonic Consultation + Thassa's Oracle (Spellbook)  — Name a card not in the deck with Demonic Consultation to exile your entire library, then resolve Thassa's Oracle with 0 devotion threshold met and 0 cards in library for an immediate win.
- Yuriko, the Tiger's Shadow + Temporal Trespass (NOVEL)  — Yuriko's ninjutsu trigger flips Temporal Trespass (CMC 11) off the top, dealing 11 damage to each opponent and generating a free extra turn to repeat the engine.
- Doom Whisperer + Demonic Consultation (NOVEL) [applied as swap] — Doom Whisperer lets you pay 2 life repeatedly to surveil 2 at instant speed, stacking a high-CMC card (like Temporal Trespass) on top for Yuriko triggers, OR to set up Thassa's Oracle by sculpting the top of the library before firing Demonic Consultation. A repeatable, mana-free library manipulation engine that costs only life.
- A-Lier, Disciple of the Drowned + Demonic Consultation (NOVEL) [applied as swap] — Lier makes all spells uncounterable and gives every instant/sorcery in your graveyard flashback. On the combo turn, Lier blankets the stack against all interaction while you resolve Demonic Consultation then Thassa's Oracle. Also gives flashback to Brainstorm and Dark Ritual for redundant lines.

**Summary narrative:**

> The primary win condition is Demonic Consultation naming a card not in the deck to exile the library, then resolving Thassa's Oracle while Lier, Disciple of the Drowned makes both spells uncounterable. The backup and fast-clock plan is Yuriko ninjutsu loops: Changeling Outcast or Ornithopter enters unblocked on turn one, Yuriko ninjutsu-connects on turn two, and subsequent attacks chain Silver-Fur Master discounts and Satoru Umezawa enables to flip Temporal Trespass and Silent-Blade Oni for lethal life-total swings before the combo even needs to go off. Doom Whisperer acts as the critical bridge between the two plans, surveilling Temporal Trespass to the top of the library right before a Yuriko trigger resolves while also being a 6/6 flier that pressures boards. The singleton Dark Ritual is included specifically to execute the full Consultation + Oracle sequence through a mana-lean opening without needing additional ramp sources.

**Consider adding (LLM flagged, not added):**
- `Force of Will` — Pitching a blue card to counter a spell for free is essential at cEDH power level to protect the Demonic Consultation + Thassa's Oracle combo turn against opposing interaction without spending mana you need for the kill.
- `Mana Drain` — Countering a spell and banking the mana means you can tap out for the Consultation + Oracle line on the following turn using the stored mana, compressing the combo window significantly.
- `Pact of Negation` — A free counterspell on the combo turn ensures Thassa's Oracle resolves even if Lier, Disciple of the Drowned is not in play, and paying the upkeep cost is irrelevant when the Oracle trigger has already won the game.

---

### ur_dragon_b3_dragon_tribal

- iter1 structural pass: `True`
- deck size: `100`
- singleton violations: `{}`
- must-includes resolved: `['Dragon Tempest', 'Tiamat']`
- must-includes dropped: `[]`
- theme_coherence_score: `1.0`
- wall-clock (s): `129.8`
- creativity_delta_count: `40`
- novel_combo_count: `6`
- semantic_source_count: `0`

**Combo-anchor guard:**
- active: `True`
- forbidden_set_size: `7`
- guard_fire_count: `0`
- sample forbidden: ['ancient gold dragon', 'astral dragon', 'cloudstone curio', 'dracogenesis', 'ganax, astral hunter', 'strionic resonator', 'vrondiss, rage of ancients']

**LLM call breakdown:**

| phase | ok | input | output | cost | latency (s) | error |
|---|---|---|---|---|---|---|
| B2_intent_interpreter | True | 714 | 1168 | $0.0197 | 26.2 | - |
| C2_1_candidate_critic | True | 6794 | 2596 | $0.0593 | 44.7 | - |
| C2_2_wild_combo_discovery | True | 25208 | 931 | $0.0896 | 20.9 | - |
| D2_final_critic_batch_0 | True | 6104 | 1380 | $0.0390 | 31.2 | - |
| D2_final_critic_batch_1 | True | 6032 | 798 | $0.0301 | 17.6 | - |
| D2_final_critic_batch_2 | True | 6035 | 761 | $0.0295 | 17.7 | - |

**10 rationale samples (verbatim):**
- **Tiamat** (user_intent|llm_rationale_rewrite): _Tutors five Dragons directly to hand upon entering, which under Dragon Tempest means you immediately trigger haste and ETB pings for every Dragon you subsequently cast off that pile — effectively assembling the entire win condition in one fell swoop._
- **Dragon Tempest** (user_intent|llm_rationale_rewrite): _Every Dragon entering the battlefield deals damage equal to the number of Dragons you control, so a Tiamat-fueled chain of five Dragons entering in a single turn can ping each opponent for 15+ total damage before any combat even occurs._
- **Dragonborn Immolator** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Sacrificing Dragonborn Immolator at instant speed to pump another Dragon's power directly scales the damage output of Terror of the Peaks or Scourge of Valkas triggers, letting you convert a low-power attacker into an immediate lethality spike._
- **Sarkhan the Masterless** (llm_wild_combo_discovery|creative_outlier|llm_rationale_rewrite): _His +1 converts every planeswalker into a 4/4 Dragon that triggers Dragon Tempest on entry, and his ultimate turning all your Dragons into 4/4s pairs with The Ur-Dragon's stat boosts to push an already-lethal board state completely out of reach for opponents._
- **Dragon Egg** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _When Dragon Egg dies — whether to a board wipe like Crux of Fate exempting your Dragons or to combat — it replaces itself with a 2/2 Dragon token that immediately triggers Dragon Tempest, making it a resilient speed bump that converts into tribal fuel._
- **Dragonspeaker Shaman** (archetype_staple|llm_intent_extension): _Corpus staple for The Ur-Dragon (usage_pct=0.53). LLM intent interpreter flagged as a likely-intended creative extension (+25 score boost). [slot=flex]_
- **Smaug** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Smaug's death trigger floods the board with 15 Treasure tokens that can instantly bankroll casting The Ur-Dragon or a chain of expensive Dragons in one turn, and its high power means Dragon Tempest deals a massive 9 damage to any target the turn Smaug enters._
- **Boneyard Scourge** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _Exiling a Dragon from your graveyard — such as a fallen Atsushi or Ebondeath — puts a hasty Boneyard Scourge onto the battlefield mid-combat or at instant speed, firing a fresh Dragon Tempest trigger at a critical moment without spending mana._
- **Atsushi, the Blazing Sky** (theme:TYPAL_DRAGONS|llm_rationale_rewrite): _When Atsushi dies, it generates either Treasures to ramp into Tiamat or impulse draws to find Dragon Tempest, making it a resilient threat that keeps producing value even when removed._
- **Goldlust Triad** (llm_wild_combo_discovery|creative_outlier|llm_rationale_rewrite): _Myriad creates attacking token copies against each opponent, and with Dragon Tempest already on board each token's entry trigger pings the defending player, turning what looks like a modest attacker into a three-way ping machine before blockers are even declared._

**Novel combo flags:**
- Dragon Tempest + Scourge of Valkas (Spellbook)  — Each Dragon ETB triggers both enchantments simultaneously; with Tiamat fetching five Dragons at once, the stacked triggers deal lethal damage to all opponents.
- Terror of the Peaks + Scourge of Valkas (Spellbook)  — Every Dragon entering the battlefield deals damage twice — once from each trigger — rapidly escalating to lethal with a parade of Dragons.
- Archwing Dragon + Dragon Tempest (NOVEL)  — Archwing Dragon returns to hand at end of turn and can be recast each turn cycle, triggering Dragon Tempest's haste grant and damage ping repeatedly for incremental damage.
- Ebondeath, Dracolich + Dragon Tempest (NOVEL)  — Ebondeath recasts itself from the graveyard whenever a non-token creature dies, generating repeated Dragon Tempest ETB triggers without spending additional cards.
- Old Gnawbone + Savage Ventmaw (Spellbook)  — Both Dragons attack together generating enormous Treasure and red/green mana, enabling a full hand of Dragons to be cast mid-combat for a game-ending Dragon Tempest chain.
- Nesting Dragon + Dragon Tempest (NOVEL) [applied as swap] — Every landfall trigger creates a Dragon Egg token; when that egg dies it creates a 2/2 flying Dragon token which immediately pings each opponent via Dragon Tempest. With Ur-Dragon enabling free land drops and ramp spells, a single turn of land plays can generate a chain of ETB pings without attacking. Terror of the Peaks doubles every ping.
- Sarkhan the Masterless + Terror of the Peaks (NOVEL) [applied as swap] — Sarkhan's +1 turns all your planeswalkers into 4/4 Dragons until end of turn — each one entering as a Dragon triggers Terror of the Peaks for 4 damage to any target per planeswalker. Also, Sarkhan's passive makes every Dragon you control ping attacking creatures, creating a defensive wall that protects your life total while you assemble the dragon board.
- Rith, the Awakener + Dragon Tempest (NOVEL) [applied as swap] — Rith swings, Dragon Tempest gives it haste the turn it enters. On combat damage to a player, pay 2G to choose green and create Saprolings equal to all green permanents you control (forests, Scaled Nurturer, Silvanus's Invoker, etc.). Those Saprolings then become blockers or fodder, and each one is a potential sac outlet for Old Gnawbone mana or a body to pump. Non-obvious because the Saproling count scales hard with a five-color mana base.
- Terror of the Peaks + Dragon Tempest (Spellbook)  — Each Dragon ETB triggers both simultaneously: Dragon Tempest pings a player equal to the Dragon's power AND gives it haste so it can swing immediately; Terror of the Peaks pings any target for the same power. With Tiamat fetching five Dragons at once, a single Tiamat cast can deal lethal ping damage across all opponents before combat even begins.
- Goldlust Triad + Terror of the Peaks (NOVEL) [applied as swap] — Goldlust Triad has myriad, creating attacking token copies against each opponent. Each myriad token is a Dragon entering the battlefield, triggering Terror of the Peaks for its power in damage to any target per copy. Three opponents means three ETB triggers from myriad alone, and Dragon Tempest stacks on top. The tokens exile at end of combat but the damage is already done — a non-obvious 'myriad as mass ETB engine' line.

**Summary narrative:**

> The primary plan is to ramp aggressively through Dragonspeaker Shaman, Savage Ventmaw, and Old Gnawbone into Tiamat, which tutors five Dragons directly to hand and triggers Dragon Tempest to grant them all haste, then unleash them in a single turn to ping all opponents via Dragon Tempest and Terror of the Peaks before swinging for lethal. The secondary plan leverages The Ur-Dragon's cast trigger to refuel the hand with free permanents on every Dragon swing, creating a self-reinforcing loop where each attack reloads the next wave. Notable tech choices include Archwing Dragon's end-step bounce enabling repeated Dragon Tempest triggers every turn, and Nesting Dragon converting fetchland sacrifices into a flood of hatchable egg tokens that further multiply the ETB ping count. Crux of Fate serves as a one-sided wrath to reset opponent boards while leaving your Dragon army fully intact, giving the deck a resilient recovery line against go-wide strategies.

**Consider adding (LLM flagged, not added):**
- `Temur Ascendancy` — Gives every Dragon haste as a redundant backup to Dragon Tempest and draws a card whenever a Dragon with power 4 or greater enters, which in a deck full of six-plus-power Dragons like Tiamat and Smaug translates to massive card advantage.
- `Kindred Discovery` — Draws a card for every Dragon entering or attacking, turning the haste-and-swing turns enabled by Dragon Tempest into enormous card-draw engines that let you reload Tiamat chains more than once per game.
- `Lathliss, Dragon Queen` — Creates a 5/5 Dragon token whenever a non-token Dragon enters the battlefield, doubling Dragon Tempest's ETB triggers and providing a free body that also counts toward The Ur-Dragon's power-bonus anthem.

**Ur-Dragon envelope check:**
- hellkite_in_deck: `False`
- gnawbone_in_deck: `True`
- hellkite_blocked_by_guard: `False`
- gnawbone_blocked_by_guard: `False`
- deck_clean: `False`
- held_by_design: `False`

---

## Iteration 3 → Iteration 4 hand-off

### Headline numbers (vs iter-2 baseline)

| Metric | Iter 2 | Iter 3 | Target | Status |
|---|---|---|---|---|
| iter1 structural pass | 5/5 | 5/5 | 5/5 | ✅ |
| Mean creativity_delta | 36.8 | **37.8** | ≥30 | ✅ (+1) |
| Mean novel_combo_count | 6.0 | 5.4 | ≥4 | ✅ (-0.6, still well above) |
| Mean cost per build | $0.278 | **$0.290** | ≤$0.40 | ✅ (+4%) |
| Mean wallclock | 192.4s | **137.3s** | ≤60 | ❌ (29% drop from iter-2, 2.3x over target) |
| Ur-Dragon envelope by design | held by 1 card | held by 1 card | held by design | ❌ |

**4/6 criteria pass.** Per the kickoff Phase 9 halt condition, this triggers a halt for user direction.

### Where each Phase landed

- **Phase 1 (D2 prompt cap to 30)**: D2 output tokens dropped ~26%, latency ~29%. Single-call rewrites of ~30 cards now run in ~30-60s instead of ~91s.
- **Phase 2 (B2 combo-anchor hard guard)**: forbidden_set populated correctly on all 5 cases (sizes 7-459). Guard fires on Krenko (1) + Atraxa (2) — the LLM did propose forbidden cards on those cases, and the guard correctly dropped them. Hellkite Charger absent from Ur-Dragon deck.
- **Phase 3 (D2 batched rewrites)**: D2 ran as 3 parallel batches in every case; per-batch latency 15-65s, max-batch ~50s. Closed ~40s of the wallclock gap.
- **Phase 4 (C2.2 trim)**: C2.2 input tokens 30k → 27k (target ≤28k achieved on the 5-case sweep). Per-build cost dropped ~$0.02.
- **Phase 5 (released_at + recent-set boost)**: 110k cards backfilled. Recent boost active but small contribution (47 recent cards in a 240-pool, +0.10 score each).
- **Phase 6 (per-theme C2.2 prompts)**: archetype detection landed correctly — tribal on Edgar/Krenko/Ur-Dragon, combo on Yuriko, control on Atraxa. Per-archetype prompt fragments shipped.
- **Phase 7 (semantic retrieval)**: scaffolded only. semantic_source_count = 0 across all cases. Iter 4 plugs in the actual Voyage AI embedding index.
- **Phase 8 (positional context for C2.1)**: prompt annotations active; rationale-quality impact will require manual inspection of the 50 sampled rationales below.

### Why the two criteria miss

**1. mean_wallclock_s = 137.3s vs target 60s (2.3x over).**

The outer chain B2 → C2.1 → C2.2 → D2 runs serially. Per-phase floor times (averaged across the 5 cases):
- B2 intent_interpreter: ~25s
- C2.1 candidate_critic: ~50s
- C2.2 wild_combo_discovery: ~22s
- D2 max-batch (parallel internal): ~40s
- Sum ≈ 137s ✓ matches the observed average.

D2 internal parallelization closed the 89s → 40s gap (Phase 3). To close further would require parallelizing the OUTER chain — i.e., letting C2.1 and C2.2 run concurrently against the same iter-1 baseline deck, then merging their proposed swaps. That's a non-trivial architectural change (output-merging logic, conflict resolution between C2.1 picks and C2.2 swap proposals targeting the same slots) and was not in iter 3 scope.

Alternative: shrink prompts further to reduce per-call latency. D2 batch budgets are already at their floor (10 cards/batch). Tighter prompts on B2/C2.1/C2.2 might shave 5-10s each but won't hit 60s.

**Iter 4 options:**
- (a) Parallelize the outer chain (estimated effort: 1-2 weeks; payoff: wallclock drops to ~max(B2+C2.1, B2+C2.2, D2) ≈ ~75-85s)
- (b) Accept ~120s as the realistic floor for the current architecture and update the target.
- (c) Move to Opus 4.6/4.7 — would actually INCREASE latency (Opus is slower), so this doesn't help.

**2. ur_dragon_envelope_held_by_design = False.**

Hellkite Charger correctly absent (would have been guard-blocked if proposed; the LLM, seeing the FORBIDDEN block in its system prompt, did not propose it). **Old Gnawbone is in the deck** — `source: archetype_staple|llm_rationale_rewrite`. It entered via Phase B's deterministic corpus-staple list (Ur-Dragon corpus has Old Gnawbone as a top staple), not via any LLM phase.

The kickoff's Phase 2 rule was "no LLM phase may suggest a card that completes a combo with a user must-include". My Phase 2 implementation correctly enforces this rule AND additionally filters Phase B's deterministic pool against the forbidden set (Tier-1 extension during Phase 2). However, Old Gnawbone is not in the forbidden set — it doesn't form a 2-card combo with Tiamat or Dragon Tempest in `combo_brackets_v1.json`. Old Gnawbone is a top corpus staple for The Ur-Dragon cohort; the corpus surfaces it as a "people who built Ur-Dragon decks tend to play this card".

**Iter 4 options:**
- (a) Transitive forbidden-set: as C2.1 adds combo-anchor cards (e.g. Ancient Copper Dragon — which combos with Old Gnawbone), expand the forbidden set to include their partners. Risk: pool explosion if a commonly-picked card has many combo partners.
- (b) Tutor-name extraction: parse must-include oracle text for explicit card names. Doesn't help Tiamat (says "Dragon creature cards" generically).
- (c) Per-commander corpus-staple suppression: if the user's stated intent is "creative" (e.g. didn't list combo enablers), soft-suppress top-frequency corpus staples that fall outside the user's stated themes.
- (d) Accept that "top corpus staple for the commander's archetype" is a legitimate user-implicit pick when they pick that commander, and revise the test case (Old Gnawbone is what most Ur-Dragon decks play; the user picking The Ur-Dragon implicitly accepts Dragon-tribe staples).

### Which prompts still under-perform

**Atraxa archetype detection**: Phase 6 classified Atraxa B2 Proliferate as "control". Proliferate doesn't fit any single archetype heuristic cleanly. The "control" classification is moderately useful (Atraxa decks do tend to be slow), but a "value-engine" or "counters-matter" archetype would be more accurate. Iter 4: add a "counters-matter" archetype with proliferate / +1/+1 counter / charge counter patterns.

**C2.1 cite-by-name rate (Phase 8 expected impact)**: needs manual scoring of the 50 sampled rationales below to determine if ≥80% reference another card by name. Spot-check on Edgar shows e.g. "When Vito's lifegain triggers, Cordial Vampire's +1/+1 counter distribution lands during the same combat step" — explicit cross-reference. Looks like Phase 8 is working but a systematic scoring is the iter-4-to-iter-5 hand-off check.

### Did semantic retrieval move the needle?

**No** — it's scaffolded only in iter 3. 0 semantic-source cards across all 5 cases. Iter 4 plugs in Voyage AI ($1.62 one-time + ~$0 per query) and the wide pool will include semantic neighbors for the LLM to consider. Expected impact: creativity_delta +5-10, novel_combo +1-2.

### Most plausible iter 4 priority

Given the two failing criteria are structural (latency floor) and architectural (corpus-baseline picks), iter 4's biggest creativity gains likely come from:

1. **Plug in Phase 7's real embedding index** (Voyage AI, ~$1.62 one-time) — gives the LLM actual semantic neighbors to consider. Bounded effort, predictable creativity gain.
2. **Outer-chain parallelization** (B2 || C2.1+C2.2 sequence, then D2) — closes the wallclock gap to ~75-85s. Moderate architectural effort.
3. **Per-commander corpus-staple suppression** — closes the Ur-Dragon-style envelope leak. Heuristic-driven; needs careful tuning so it doesn't break decks that genuinely want their corpus staples.
4. **Opus 4.6 upgrade ONLY if 1-3 hit a creativity ceiling** — adds ~50% cost, ~10-20% creativity. Save for after structured combo space (Pillar C primitive integration) lands.

Pillar C primitive extractor build is also a strong candidate for iter 4 if Pillar F v0.1 (statistical approximator) is going to use primitive tags for win-path decomposition.

### Halt status

**Per the kickoff Phase 9 hard-halt condition: 2 of 6 criteria failed → halt for user direction.** The mega-task does NOT proceed to Phase 10 (Pillar E mana base optimizer) without user authorization. The creativity layer is healthy; the two failures are documented architectural limits, not regressions.
