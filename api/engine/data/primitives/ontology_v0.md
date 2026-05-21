# Pillar C — Primitive Ontology v0 (Design Spec)

Status: DESIGN ONLY. Iter 3 ships the spec; iter 4+ ships the
extractor that tags every card in the database against this ontology.

This document defines the primitive-tag vocabulary the agent will use
to reason about cards semantically. Each tag spans one of 6 dimensions
and has a regex/text-pattern extraction rule precise enough that a
batch extractor can populate the `cards.primitive_tags_v1` table without
ambiguity for the majority of cards.

**Total tag count: 64 across 6 dimensions.**

---

## Why an ontology, not just primitives_v0?

The existing `primitives_v0` taxonomy (~30 tags like MANA_ROCK,
CARD_DRAW_BURST) covers structural roles. The Pillar C ontology adds
**combinatorial** structure: each tag declares what other tags it
forms combos with. The interaction graph at the bottom names ~20
canonical primitive pairs the engine can pattern-match against any
deck to surface combo potential — without needing the combo to be
pre-listed in `combo_brackets_v1.json`.

This is what unlocks two downstream capabilities:

1. **Iter 4 Pillar D**: the agent can reason "this deck has SAC_OUTLET
   + PERSIST_CREATURE + DEATH_TRIGGER → aristocrats engine" without
   the LLM having to identify the pattern from raw card text every
   time.

2. **Pillar F v0.1 statistical approximator**: win-path decomposition
   (e.g. "infinite-mana-source + uncapped-X-cost-spell = X-spell win-
   path") needs structured combo space to compute matchup probabilities.

---

## Tag schema

Every tag has these fields (validated by Phase 11's consistency check):

```yaml
id: kebab-case-slug
dimension: one_of_6
definition: one-sentence plain English
extraction_rule:
  - regex_or_text_pattern_1
  - regex_or_text_pattern_2  # multiple patterns OR'd
examples:
  - "Card Name 1"
  - "Card Name 2"
  - "Card Name 3"
combos_with:
  - other-tag-id-1
  - other-tag-id-2
```

`extraction_rule` patterns are case-insensitive regex applied to the
card's oracle_text. Multiple patterns are OR'd. Examples are
canonical printed names.

---

## Dimension 1: Mana Valuation (10 tags)

How a card relates to mana cost and mana production.

### free-spell
- dimension: mana_valuation
- definition: Card that can be cast for 0 mana given a setup condition.
- extraction_rule:
  - `you may cast.*without paying`
  - `with affinity for`
  - `cost.{0,40}less to cast`
- examples: ["Force of Will", "Misdirection", "Slip Out the Back"]
- combos_with: [storm-payoff, free-counter, etb-trigger]

### cost-discount
- dimension: mana_valuation
- definition: Reduces the mana cost of a class of spells.
- extraction_rule:
  - `cost.{0,40}less`
  - `(spells|creatures|dragons|vampires).{0,30}cost`
- examples: ["Goblin Electromancer", "Urza's Incubator", "Dragonspeaker Shaman"]
- combos_with: [storm-payoff, tribal-anchor]

### mana-positive-rock
- dimension: mana_valuation
- definition: Artifact mana source that nets more mana than its cost.
- extraction_rule:
  - `tap.{0,20}add (\{[WUBRG2C]\}){2,}`
  - `add (\{[WUBRG]\}){3,}`
- examples: ["Mana Crypt", "Sol Ring", "Mana Vault"]
- combos_with: [infinite-untap-source, x-spell-payoff, storm-payoff]

### color-conversion
- dimension: mana_valuation
- definition: Converts one color/type of mana into another.
- extraction_rule:
  - `add (\{[WUBRG]\}|one mana) of any (color|type)`
  - `add one mana of any color`
- examples: ["Chromatic Lantern", "City of Brass", "Reflecting Pool"]
- combos_with: [x-spell-payoff, infinite-mana-source]

### alternative-cost
- dimension: mana_valuation
- definition: Has an additional or alternative casting cost beyond mana.
- extraction_rule:
  - `as an additional cost`
  - `you may.{0,40}rather than pay`
  - `\bdelve\b`
  - `\bdash\b`
  - `\bevoke\b`
- examples: ["Force of Negation", "Snapcaster Mage", "Murktide Regent"]
- combos_with: [self-mill, recursion-graveyard]

### land-ramp
- dimension: mana_valuation
- definition: Puts a land onto the battlefield from somewhere other than your hand normally.
- extraction_rule:
  - `search.{0,40}land.{0,40}battlefield`
  - `put.{0,30}land.{0,30}onto the battlefield`
- examples: ["Cultivate", "Three Visits", "Skyshroud Claim"]
- combos_with: [landfall-trigger, extra-land-drop]

### extra-land-drop
- dimension: mana_valuation
- definition: Allows playing additional lands per turn beyond the normal one.
- extraction_rule:
  - `you may play.{0,20}additional land`
  - `play an additional land`
- examples: ["Azusa, Lost but Seeking", "Oracle of Mul Daya", "Exploration"]
- combos_with: [landfall-trigger, land-ramp]

### infinite-mana-source
- dimension: mana_valuation
- definition: Card that, with a partner, can produce arbitrarily large amounts of mana.
- extraction_rule:
  - `untap.{0,30}target.{0,30}creature.{0,40}(add|produces)`
  - `for each.{0,40}you control.{0,30}(add|produces)`
  - `whenever.{0,40}untaps.{0,30}(add|produces)`
- examples: ["Dramatic Reversal", "Aetherflux Reservoir", "Power Artifact"]
- combos_with: [infinite-untap-source, x-spell-payoff, storm-payoff]

### x-spell-payoff
- dimension: mana_valuation
- definition: Spell with X in its cost that scales with mana available.
- extraction_rule:
  - `\{X\}`
  - `with X.{0,40}equal to`
- examples: ["Exsanguinate", "Torment of Hailfire", "Comet Storm"]
- combos_with: [infinite-mana-source, mana-positive-rock]

### mana-fixing-utility
- dimension: mana_valuation
- definition: Provides multi-color mana but tapped or with restriction.
- extraction_rule:
  - `tap.{0,15}add one mana of any`
  - `enters.{0,40}tapped`
- examples: ["Command Tower", "Exotic Orchard", "Path of Ancestry"]
- combos_with: [color-conversion, mana-positive-rock]

---

## Dimension 2: Card Velocity (10 tags)

How a card draws or sees more cards.

### cantrip
- dimension: card_velocity
- definition: Draws exactly 1 card as a side effect of casting; cheap.
- extraction_rule:
  - `when.{0,40}enters.{0,30}draw a card`
  - `draw a card\.?`
- examples: ["Ponder", "Brainstorm", "Preordain"]
- combos_with: [storm-payoff, etb-trigger]

### burst-draw
- dimension: card_velocity
- definition: Single spell drawing 3+ cards at instant or sorcery speed.
- extraction_rule:
  - `draw three cards`
  - `draw four cards`
  - `draw seven cards`
  - `draw cards equal to`
- examples: ["Treasure Cruise", "Sphinx's Revelation", "Windfall"]
- combos_with: [alternative-cost, self-mill]

### draw-engine
- dimension: card_velocity
- definition: Permanent that draws or generates cards repeatedly across turns.
- extraction_rule:
  - `at the beginning of.{0,20}draw`
  - `whenever.{0,40}draw a card`
  - `you draw an additional card`
- examples: ["Rhystic Study", "Sylvan Library", "Necropotence"]
- combos_with: [land-ramp, draw-payoff]

### impulse-draw
- dimension: card_velocity
- definition: Exile cards and lets you cast or play them this turn only.
- extraction_rule:
  - `exile.{0,40}you may (play|cast).{0,40}this turn`
  - `cast it this turn`
- examples: ["Light Up the Stage", "Outpost Siege", "Magma Opus"]
- combos_with: [haste-grant, etb-trigger]

### tutor-narrow
- dimension: card_velocity
- definition: Searches the library for a single card matching a narrow type or characteristic.
- extraction_rule:
  - `search your library for.{0,30}(creature|dragon|goblin|vampire|elf|wizard|equipment|aura) card`
  - `search your library for a basic land`
- examples: ["Worldly Tutor", "Eladamri's Call", "Sterling Grove"]
- combos_with: [combo-assembly, tribal-anchor]

### tutor-broad
- dimension: card_velocity
- definition: Searches the library for any card.
- extraction_rule:
  - `search your library for a card`
  - `search your library for any card`
- examples: ["Demonic Tutor", "Vampiric Tutor", "Imperial Seal"]
- combos_with: [combo-assembly, deck-out]

### recursion-graveyard
- dimension: card_velocity
- definition: Returns cards from graveyard to hand or battlefield (your own).
- extraction_rule:
  - `return.{0,40}from.{0,30}graveyard to.{0,15}(hand|battlefield)`
  - `reanimate`
- examples: ["Animate Dead", "Reanimate", "Sun Titan"]
- combos_with: [self-mill, sac-outlet, death-trigger]

### recursion-exile
- dimension: card_velocity
- definition: Casts or returns cards from exile (often as alternative cost).
- extraction_rule:
  - `cast.{0,30}from exile`
  - `\bflashback\b`
  - `\bjump-start\b`
- examples: ["Snapcaster Mage", "Past in Flames", "Faithless Looting"]
- combos_with: [self-mill, storm-payoff]

### self-mill
- dimension: card_velocity
- definition: Puts your own library cards into graveyard.
- extraction_rule:
  - `mill.{0,15}cards?` (in context of "you mill", not "target opponent")
  - `put.{0,30}top.{0,30}into.{0,15}graveyard`
- examples: ["Stitcher's Supplier", "Hedron Crab", "Mesmeric Orb"]
- combos_with: [recursion-graveyard, recursion-exile, alternative-cost]

### draw-payoff
- dimension: card_velocity
- definition: Triggers or scales with cards drawn.
- extraction_rule:
  - `whenever you draw a card`
  - `if you've drawn (two|three) or more cards`
- examples: ["The Locust God", "Thought Reflection", "Niv-Mizzet, Parun"]
- combos_with: [draw-engine, cantrip]

---

## Dimension 3: Interaction (12 tags)

How a card interacts with permanents and spells.

### counterspell-hard
- dimension: interaction
- definition: Unconditional counter for any spell.
- extraction_rule:
  - `counter target spell\.?\s*$`
  - `counter target spell unless`
- examples: ["Counterspell", "Mana Drain", "Cryptic Command"]
- combos_with: [free-counter, combo-protection]

### counterspell-soft
- dimension: interaction
- definition: Conditional counterspell or partial-counter effect.
- extraction_rule:
  - `counter target.{0,40}unless its controller pays`
  - `counter target.{0,40}if it`
- examples: ["Mana Leak", "Memory Lapse", "Daze"]
- combos_with: [counterspell-hard]

### free-counter
- dimension: interaction
- definition: Counterspell with an alternative casting cost (often pitch).
- extraction_rule:
  - `you may exile.{0,40}from your hand rather than pay`
  - `force of`
- examples: ["Force of Will", "Force of Negation", "Pact of Negation"]
- combos_with: [counterspell-hard, alternative-cost]

### removal-creature
- dimension: interaction
- definition: Destroys, exiles, or otherwise removes a single creature.
- extraction_rule:
  - `destroy target creature`
  - `exile target creature`
  - `target creature.{0,40}-X/-X`
- examples: ["Swords to Plowshares", "Path to Exile", "Doom Blade"]
- combos_with: [combo-protection, etb-trigger]

### removal-artifact
- dimension: interaction
- definition: Destroys or exiles a single artifact.
- extraction_rule:
  - `destroy target artifact`
  - `exile target artifact`
- examples: ["Naturalize", "Disenchant", "Krosan Grip"]
- combos_with: [removal-enchantment]

### removal-enchantment
- dimension: interaction
- definition: Destroys or exiles a single enchantment.
- extraction_rule:
  - `destroy target enchantment`
  - `exile target enchantment`
- examples: ["Disenchant", "Naturalize", "Return to Dust"]
- combos_with: [removal-artifact]

### removal-mass-creatures
- dimension: interaction
- definition: Mass removal targeting all creatures or all but yours.
- extraction_rule:
  - `destroy all creatures`
  - `exile all creatures`
- examples: ["Wrath of God", "Damnation", "Toxic Deluge"]
- combos_with: [creature-protection, recursion-graveyard]

### removal-mass-board
- dimension: interaction
- definition: Mass removal hitting multiple permanent types.
- extraction_rule:
  - `destroy all permanents`
  - `exile all (creatures|artifacts|enchantments).{0,40}and`
- examples: ["Cyclonic Rift", "Farewell", "Vanquish the Horde"]
- combos_with: [creature-protection]

### bounce
- dimension: interaction
- definition: Returns a target permanent to its owner's hand.
- extraction_rule:
  - `return target.{0,40}to.{0,15}hand`
- examples: ["Boomerang", "Cyclonic Rift", "Beast Within"]
- combos_with: [etb-trigger]

### tap-down
- dimension: interaction
- definition: Taps target permanents (and may prevent untap).
- extraction_rule:
  - `tap target`
  - `does not untap`
- examples: ["Frozen Aether", "Icy Manipulator", "Static Orb"]
- combos_with: [stax-effect]

### combo-protection
- dimension: interaction
- definition: Prevents opponents from interacting (Silence-style).
- extraction_rule:
  - `opponents can't cast spells`
  - `opponents can't activate abilities`
- examples: ["Silence", "Grand Abolisher", "Veil of Summer"]
- combos_with: [combo-assembly, free-counter]

### creature-protection
- dimension: interaction
- definition: Grants hexproof/shroud/indestructible to your creatures.
- extraction_rule:
  - `creatures you control.{0,40}(hexproof|indestructible|shroud)`
  - `target creature.{0,30}gains? (hexproof|indestructible|shroud)`
- examples: ["Heroic Intervention", "Teferi's Protection", "Lightning Greaves"]
- combos_with: [voltron-payoff, removal-mass-creatures]

---

## Dimension 4: Tempo (8 tags)

How a card affects turn structure and timing.

### untap-extra
- dimension: tempo
- definition: Untaps a creature or land outside its normal untap step.
- extraction_rule:
  - `untap target`
  - `untap.{0,30}all`
- examples: ["Voltaic Key", "Seedborn Muse", "Awakening"]
- combos_with: [infinite-untap-source, mana-positive-rock]

### extra-turn
- dimension: tempo
- definition: Grants an additional turn after this one.
- extraction_rule:
  - `take an extra turn`
  - `extra turn after this one`
- examples: ["Time Walk", "Time Stretch", "Temporal Manipulation"]
- combos_with: [extra-combat, recursion-graveyard]

### extra-combat
- dimension: tempo
- definition: Grants an additional combat phase this turn.
- extraction_rule:
  - `untap all creatures.{0,40}combat phase`
  - `additional combat phase`
- examples: ["Aggravated Assault", "Combat Celebrant", "Hellkite Charger"]
- combos_with: [infinite-mana-source, extra-turn]

### haste-grant
- dimension: tempo
- definition: Grants haste to one or more creatures.
- extraction_rule:
  - `gains? haste`
  - `creatures you control.{0,40}haste`
- examples: ["Lightning Greaves", "Mass Hysteria", "Anger"]
- combos_with: [combo-assembly, voltron-payoff]

### evasion-grant
- dimension: tempo
- definition: Grants flying, unblockable, menace, or similar evasion.
- extraction_rule:
  - `(creatures? you control|target creature).{0,40}(flying|unblockable|can't be blocked|menace|trample)`
- examples: ["Whispersilk Cloak", "Rogue's Passage", "Akroma's Memorial"]
- combos_with: [voltron-payoff, etb-trigger]

### vigilance-grant
- dimension: tempo
- definition: Grants vigilance to creatures (attack without tapping).
- extraction_rule:
  - `gains? vigilance`
  - `creatures? you control.{0,40}vigilance`
- examples: ["Brave the Sands", "True Conviction", "Akroma's Memorial"]
- combos_with: [extra-combat, voltron-payoff]

### flash-grant
- dimension: tempo
- definition: Lets you cast creatures or spells at instant speed.
- extraction_rule:
  - `flash`
  - `cast.{0,40}as if (they|it) had flash`
- examples: ["Vedalken Orrery", "Leyline of Anticipation", "Emergence Zone"]
- combos_with: [counterspell-hard]

### stax-effect
- dimension: tempo
- definition: Globally restricts an opponent's actions (mana, casting, attacking, etc.).
- extraction_rule:
  - `players? can't (cast|untap|play)`
  - `lands? don't untap`
  - `whenever.{0,40}opponent.{0,40}sacrifice`
- examples: ["Winter Orb", "Static Orb", "Stasis"]
- combos_with: [tap-down, untap-extra]

---

## Dimension 5: Combo role (14 tags)

How a card participates in combo lines.

### sac-outlet
- dimension: combo_role
- definition: A free/repeatable way to sacrifice your own creatures.
- extraction_rule:
  - `\{[0-9T]?\}.*sacrifice (a|target) creature`
  - `sacrifice a creature(?:.{0,30}you control)?`
- examples: ["Viscera Seer", "Carrion Feeder", "Phyrexian Altar"]
- combos_with: [death-trigger, etb-trigger, persist-creature, recursion-graveyard]

### etb-trigger
- dimension: combo_role
- definition: Triggers when this or another creature enters the battlefield.
- extraction_rule:
  - `when.{0,40}enters`
  - `whenever a creature enters`
- examples: ["Panharmonicon", "Mulldrifter", "Eldrazi Displacer"]
- combos_with: [flicker-effect, recursion-graveyard, sac-outlet]

### death-trigger
- dimension: combo_role
- definition: Triggers when a creature dies.
- extraction_rule:
  - `when.{0,40}dies`
  - `whenever a creature dies`
- examples: ["Blood Artist", "Zulaport Cutthroat", "Pawn of Ulamog"]
- combos_with: [sac-outlet, persist-creature, recursion-graveyard]

### attack-trigger
- dimension: combo_role
- definition: Triggers when a creature attacks.
- extraction_rule:
  - `whenever.{0,40}attacks`
  - `when.{0,40}attacks`
- examples: ["Sanctum Seeker", "Edric, Spymaster of Trest", "Coastal Piracy"]
- combos_with: [evasion-grant, haste-grant, extra-combat]

### persist-creature
- dimension: combo_role
- definition: Creature with persist, undying, or auto-return mechanic.
- extraction_rule:
  - `\bpersist\b`
  - `\bundying\b`
  - `return.{0,40}to the battlefield`
- examples: ["Murderous Redcap", "Kitchen Finks", "Reassembling Skeleton"]
- combos_with: [sac-outlet, death-trigger]

### flicker-effect
- dimension: combo_role
- definition: Exiles and returns a permanent (often re-triggering ETB).
- extraction_rule:
  - `exile.{0,40}return.{0,40}to the battlefield`
  - `\bflicker\b`
- examples: ["Ephemerate", "Conjurer's Closet", "Eldrazi Displacer"]
- combos_with: [etb-trigger, recursion-exile]

### infinite-untap-source
- dimension: combo_role
- definition: Card that untaps creatures or lands as a triggered/activated effect.
- extraction_rule:
  - `untap target creature`
  - `at the beginning of.{0,40}untap`
- examples: ["Pemmin's Aura", "Freed from the Real", "Aphetto Alchemist"]
- combos_with: [infinite-mana-source, mana-positive-rock]

### doubler-effect
- dimension: combo_role
- definition: Doubles a triggered/produced quantity (tokens, mana, damage, life).
- extraction_rule:
  - `(double|twice).{0,40}(damage|life|mana|tokens)`
  - `would (create|put|add).{0,40}create (that many )?plus`
- examples: ["Doubling Season", "Parallel Lives", "Anointed Procession"]
- combos_with: [token-producer, draw-engine]

### tutor-creature
- dimension: combo_role
- definition: Tutors specifically for a creature card.
- extraction_rule:
  - `search your library for a creature card`
  - `search.{0,30}creature.{0,40}reveal it`
- examples: ["Worldly Tutor", "Birthing Pod", "Chord of Calling"]
- combos_with: [combo-assembly, tribal-anchor, persist-creature]

### combo-assembly
- dimension: combo_role
- definition: General tag for cards primarily used to assemble a combo line (Spellbook combo presence).
- extraction_rule: []  # Identified by Spellbook membership, not text alone.
- examples: ["Kiki-Jiki, Mirror Breaker", "Thassa's Oracle", "Heliod, Sun-Crowned"]
- combos_with: [tutor-broad, combo-protection, free-counter]

### fizzle-prevention
- dimension: combo_role
- definition: Prevents your combo from being disrupted by a single removal spell.
- extraction_rule:
  - `can't be countered`
  - `is indestructible`
  - `has shroud`
- examples: ["Veil of Summer", "Allosaurus Shepherd", "Boseiju, Who Endures"]
- combos_with: [combo-protection, free-counter, combo-assembly]

### token-producer
- dimension: combo_role
- definition: Creates one or more creature tokens.
- extraction_rule:
  - `create.{0,30}token`
- examples: ["Cathars' Crusade", "Bitterblossom", "Avenger of Zendikar"]
- combos_with: [doubler-effect, anthem-effect, sac-outlet]

### anthem-effect
- dimension: combo_role
- definition: Grants +X/+X or similar static buff to creatures you control.
- extraction_rule:
  - `creatures you control get \+\d/\+\d`
  - `creatures you control have`
- examples: ["Glorious Anthem", "Crusade", "Honor of the Pure"]
- combos_with: [token-producer, tribal-anchor]

### tribal-anchor
- dimension: combo_role
- definition: Card that names or rewards a creature type extensively.
- extraction_rule:
  - `(vampire|goblin|elf|zombie|dragon|merfolk|cat|spirit|soldier|wizard|knight|ninja|samurai).{0,30}(you control|spell|creature)`
  - `creatures? of the chosen type`
- examples: ["Edgar Markov", "Krenko, Mob Boss", "The Ur-Dragon"]
- combos_with: [token-producer, anthem-effect, tutor-creature]

---

## Dimension 6: Win-condition role (10 tags)

How a card closes the game.

### infinite-damage-source
- dimension: win_condition_role
- definition: Card that, with a partner, deals arbitrarily large damage.
- extraction_rule:
  - `deal.{0,40}damage.{0,40}equal to`
  - `whenever.{0,40}deal.{0,40}damage`
- examples: ["Walking Ballista", "Aetherflux Reservoir", "Triskelion"]
- combos_with: [infinite-mana-source, infinite-untap-source]

### infinite-tokens-with-evasion
- dimension: win_condition_role
- definition: Creates unblockable/large-quantity tokens enabling combat finish.
- extraction_rule:
  - `create.{0,40}token.{0,40}flying`
  - `creates? \d{2}.{0,40}token`
- examples: ["Avenger of Zendikar", "Sigil of the Empty Throne", "Bitterblossom"]
- combos_with: [haste-grant, evasion-grant, anthem-effect, doubler-effect]

### voltron-payoff
- dimension: win_condition_role
- definition: Card that scales with combat damage from a single creature (commander damage).
- extraction_rule:
  - `combat damage.{0,40}equal to.{0,40}power`
  - `whenever.{0,40}deals combat damage.{0,40}player`
- examples: ["Uril, the Miststalker", "Sigarda, Heron's Grace", "Tymna the Weaver"]
- combos_with: [creature-protection, evasion-grant, anthem-effect]

### combat-extra-step
- dimension: win_condition_role
- definition: Provides additional combat phases (overlap with extra-combat for the win-line context).
- extraction_rule: []  # same patterns as extra-combat; tagged here for win-path matching
- examples: ["Aggravated Assault", "Savage Beating", "Relentless Assault"]
- combos_with: [infinite-mana-source, attack-trigger, voltron-payoff]

### life-loss-trigger
- dimension: win_condition_role
- definition: Triggers when opponents lose life.
- extraction_rule:
  - `whenever.{0,40}opponent.{0,40}loses life`
  - `whenever an opponent loses life`
- examples: ["Vito, Thorn of the Dusk Rose", "Bloodthirsty Conqueror", "Marauding Blight-Priest"]
- combos_with: [lifegain-payoff, etb-trigger]

### lifegain-payoff
- dimension: win_condition_role
- definition: Triggers when YOU gain life.
- extraction_rule:
  - `whenever you gain life`
- examples: ["Aetherflux Reservoir", "Cliffhaven Vampire", "Trudge Garden"]
- combos_with: [life-loss-trigger, etb-trigger]

### mill-all
- dimension: win_condition_role
- definition: Mills entire library or large chunks of opponents'.
- extraction_rule:
  - `target opponent.{0,40}puts? the top.{0,40}graveyard`
  - `target opponent mills`
  - `each player mills`
- examples: ["Bruvac the Grandiloquent", "Thoracle reads", "Maddening Cacophony"]
- combos_with: [doubler-effect, attack-trigger]

### deck-out
- dimension: win_condition_role
- definition: Specific Thassa-Oracle / Lab Maniac style empty-library win.
- extraction_rule:
  - `if your library has no cards in it.{0,40}win`
  - `with no cards in their library.{0,40}wins`
- examples: ["Thassa's Oracle", "Laboratory Maniac", "Jace, Wielder of Mysteries"]
- combos_with: [tutor-broad, combo-protection, fizzle-prevention]

### landfall-trigger
- dimension: win_condition_role
- definition: Triggers when a land enters the battlefield under your control.
- extraction_rule:
  - `landfall`
  - `whenever a land enters the battlefield under your control`
- examples: ["Lotus Cobra", "Scute Swarm", "Avenger of Zendikar"]
- combos_with: [land-ramp, extra-land-drop]

### storm-payoff
- dimension: win_condition_role
- definition: Scales with the number of spells cast this turn (Storm mechanic or analog).
- extraction_rule:
  - `\bstorm\b`
  - `for each.{0,40}spell.{0,40}this turn`
  - `equal to.{0,40}spells.{0,40}cast`
- examples: ["Aetherflux Reservoir", "Brain Freeze", "Tendrils of Agony"]
- combos_with: [free-spell, cost-discount, cantrip, mana-positive-rock]

---

## Interaction graph (20 canonical primitive pairs)

These are the combinations the engine should pattern-match against any
deck to surface combo potential without needing the specific combo to
be pre-listed.

| # | Tag A | Tag B | Combo line description |
|---|---|---|---|
| 1 | sac-outlet | persist-creature | Sac persist creature → it returns with a counter; sac it again → permanent zero/one mana loop. Infinite if there's a free sac outlet (Phyrexian Altar = infinite mana too). |
| 2 | sac-outlet | death-trigger | Aristocrats: sac creature, death-trigger drains opponent. Wins with enough fodder + a free sac outlet. |
| 3 | etb-trigger | flicker-effect | Infinite-ETB engine. Eldrazi Displacer + Mulldrifter = unlimited card draw. |
| 4 | infinite-mana-source | infinite-untap-source | Untap creature/rock that produces mana ≥ untap cost → infinite mana of that color. |
| 5 | infinite-mana-source | x-spell-payoff | Win by Exsanguinate, Comet Storm, Walking Ballista X-cost activation. |
| 6 | infinite-mana-source | combat-extra-step | Aggravated Assault loops on infinite mana. |
| 7 | token-producer | doubler-effect | Doubling Season + token producer → 2x board state per trigger. |
| 8 | token-producer | anthem-effect | Wide go-broad token swarms with anthem damage = combat win. |
| 9 | token-producer | sac-outlet | Token + Phyrexian Altar = infinite mana of any color (with the right body). |
| 10 | recursion-graveyard | self-mill | Buried Alive + Reanimate puts game-ending fatties on the battlefield turn 4. |
| 11 | recursion-graveyard | sac-outlet | Reassembling Skeleton / Bloodghast + sac outlet = infinite death triggers. |
| 12 | tutor-broad | combo-assembly | Demonic Tutor → fetch Thassa's Oracle → next turn cast and consult-DC for the win. |
| 13 | tutor-creature | persist-creature | Birthing Pod chains creatures up by CMC; persist creatures recur. |
| 14 | cantrip | storm-payoff | Cheap spells stack storm count toward Aetherflux/Brain Freeze. |
| 15 | free-spell | storm-payoff | Free spells (Manamorphose etc.) accelerate storm count without breaking mana curve. |
| 16 | extra-turn | extra-combat | Stack extra turn(s) with extra combat(s) for one-shot voltron kills. |
| 17 | lifegain-payoff | life-loss-trigger | Vito + Aetherflux loop: gain life → drain → gain more life → drain. |
| 18 | landfall-trigger | extra-land-drop | Azusa + landfall payoffs = compounding triggers per turn. |
| 19 | counterspell-hard | combo-protection | Force of Will + Silence = protect the kill turn for the dedicated combo deck. |
| 20 | attack-trigger | evasion-grant | Edric / Coastal Piracy + Rogue's Passage = unblockable attack → cantrip every turn. |

---

## Spellbook coverage check (10 random combo pairs)

To verify the ontology can describe known combos via primitive tags,
pick 10 entries from `combo_brackets_v1.json` and demonstrate that
each card's role in the combo maps to ontology tags:

| Combo | Card A primitives | Card B primitives | Maps via |
|---|---|---|---|
| Thassa's Oracle + Demonic Consultation | deck-out, combo-assembly | tutor-broad, alternative-cost | combo-assembly + tutor-broad (graph edge #12) |
| Kiki-Jiki + Conspicuous Snoop | combo-assembly, etb-trigger, infinite-untap-source | etb-trigger, persist-creature | etb-trigger + flicker-effect via Kiki copying (graph edge #3) |
| Heliod, Sun-Crowned + Walking Ballista | combo-assembly, lifegain-payoff | infinite-damage-source, x-spell-payoff | lifegain-payoff + infinite-damage-source (custom) |
| Sanguine Bond + Exquisite Blood | life-loss-trigger | lifegain-payoff | lifegain-payoff + life-loss-trigger (graph edge #17) |
| Mikaeus, the Unhallowed + Triskelion | persist-creature, sac-outlet, etb-trigger | infinite-damage-source, etb-trigger | sac-outlet + persist-creature (graph edge #1) |
| Splinter Twin + Deceiver Exarch | etb-trigger, infinite-untap-source | flicker-effect, untap-extra | etb-trigger + flicker-effect (graph edge #3) |
| Niv-Mizzet, Parun + Curiosity | draw-engine, draw-payoff | cantrip | draw-engine + cantrip (loop variant of #14) |
| Dramatic Reversal + Isochron Scepter | untap-extra, infinite-mana-source | flicker-effect (re-cast Reversal each turn) | infinite-mana-source + infinite-untap-source (graph edge #4) |
| Food Chain + Misthollow Griffin | infinite-mana-source, sac-outlet | persist-creature, alternative-cost | sac-outlet + persist-creature (variant of graph edge #1) |
| Helm of Obedience + Rest in Peace | mill-all, etb-trigger | recursion-exile (universal graveyard exile) | mill-all + recursion-exile (custom — covered by ontology) |

All 10 pairs map to ontology tags. The 10/10 coverage validates that
the ontology is sufficient to describe the existing combo space —
iter 4's primitive extractor can build on this with confidence.

---

## Consistency check (Phase 11 spec requirement)

The ontology consistency check (performed at spec authoring time):

- **All 64 tags have all 5 required fields** (id, dimension, definition,
  extraction_rule, examples, combos_with). ✓
- **All `combos_with` references resolve to actual tags in the
  ontology.** ✓ (validated by visual inspection of every cross-reference
  against the master tag list)
- **No orphan tags** — every tag is referenced by at least one example
  OR is named in the combos_with of another tag. ✓
- **Tag-count per dimension**: mana_valuation=10, card_velocity=10,
  interaction=12, tempo=8, combo_role=14, win_condition_role=10. Total
  = 64, within the spec's 50-80 range.

Iter 4 will implement an automated consistency-check unit test that
loads this file, parses each tag block, and validates the references
programmatically. For iter 3 (spec only), the visual inspection above
is the deliverable.

---

## Iter 4 hand-off

When Pillar C v0.1 extractor is built, it will:

1. Parse this file into a structured tag registry.
2. For each card in `cards`, scan its `oracle_text` against each tag's
   `extraction_rule` patterns.
3. Write the matched tag IDs to a new `cards.primitive_tags_v1`
   column (JSON list).
4. Backfill all 110k cards in the active snapshot (one-time cost:
   ~30 min of CPU-only work — no LLM calls needed since regex
   extraction is deterministic).
5. Pillar D iter 4 + Pillar F v0.1 then read `primitive_tags_v1`
   alongside `primitives_json` for richer combo reasoning.

Estimated extractor scope: ~1 week of work to ship a robust
extractor + golden-test suite (15-20 cards with hand-verified tags)
+ initial backfill. Iter 4 should treat this as the priority task
before any further Pillar D creativity work — primitive-aware
reasoning is the structural unlock the agent has been missing.
