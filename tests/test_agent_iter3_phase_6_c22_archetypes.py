"""Iter 3 Phase 6 tests — per-theme C2.2 prompts.

Tests `detect_archetype` against representative intent_analysis +
theme_hints combinations matching the 5 iter-2 canonical cases plus a
handful of additional archetypes (storm, aristocrats, control, blink,
reanimator, landfall, group_hug, tokens). Also verifies that the
`prompt_fragment_for` mapping covers every entry in ARCHETYPES.
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_c22_prompt_templates_v1 import (
    ARCHETYPES,
    detect_archetype,
    prompt_fragment_for,
)


class DetectArchetypeTests(unittest.TestCase):
    def test_default_when_no_input(self) -> None:
        self.assertEqual(detect_archetype(intent_analysis=None), "default")

    def test_edgar_vampires_detects_tribal(self) -> None:
        ia = {
            "likely_win_condition": "Flood the board with vampires via Edgar's eminence and drain via Vito",
            "implicit_themes": ["lifegain payoffs", "wide vampire swarm", "aristocrats light"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["TYPAL_VAMPIRES"],
            commander="Edgar Markov",
        )
        self.assertEqual(result, "tribal")

    def test_krenko_goblin_combo_detects_tribal(self) -> None:
        # Krenko + Kiki-Jiki + Conspicuous Snoop — combo signals would
        # also fire, but tribal scores higher because of the goblin
        # theme + commander name + must_include_analysis text.
        ia = {
            "likely_win_condition": "Goblin tribal swarm with Kiki/Snoop infinite combo as backup",
            "implicit_themes": ["goblin tribal", "infinite combo"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["TYPAL_GOBLINS"],
            commander="Krenko, Mob Boss",
        )
        # Both tribal and combo will score; "tribal" comes earlier in
        # ARCHETYPES tuple so it wins the tiebreaker — but here it
        # genuinely has more matches.
        self.assertEqual(result, "tribal")

    def test_yuriko_thoracle_detects_combo(self) -> None:
        ia = {
            "likely_win_condition": "Thassa's Oracle + Demonic Consultation kill in the early game",
            "implicit_themes": ["ninja tempo", "instant-speed combo"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["TYPAL_NINJAS"],
            commander="Yuriko, the Tiger's Shadow",
        )
        # Thassa's Oracle in win condition should make combo dominant.
        self.assertEqual(result, "combo")

    def test_atraxa_proliferate_detects_default_or_control(self) -> None:
        # Proliferate doesn't fit any tribal archetype; "default" is the
        # honest answer here.
        ia = {
            "likely_win_condition": "Proliferate +1/+1 counters until creatures lethal",
            "implicit_themes": ["+1/+1 counters", "proliferate value"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["THEME_PROLIFERATE"],
            commander="Atraxa, Praetors' Voice",
        )
        # Acceptable: "default" if no archetype scores. Tokens is also
        # plausible if "create tokens" is in win condition; here it isn't.
        self.assertIn(result, ARCHETYPES)
        self.assertNotEqual(result, "tribal")  # critical: not tribal

    def test_ur_dragon_dragon_tribal_detects_tribal(self) -> None:
        ia = {
            "likely_win_condition": "Tutor Dragons with Tiamat and swing through Dragon Tempest",
            "implicit_themes": ["Dragon tribal", "ramp into expensive Dragons"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["TYPAL_DRAGONS"],
            commander="The Ur-Dragon",
        )
        self.assertEqual(result, "tribal")

    def test_storm_detection(self) -> None:
        ia = {
            "likely_win_condition": "Storm count chain via ritual chains and free spells, win with Brain Freeze",
            "implicit_themes": ["storm enablers", "cost reduction"],
        }
        result = detect_archetype(intent_analysis=ia, commander="Kess, Dissident Mage")
        self.assertEqual(result, "storm")

    def test_aristocrats_detection(self) -> None:
        ia = {
            "likely_win_condition": "Sacrifice creatures via Viscera Seer triggering Blood Artist for incremental drain",
            "implicit_themes": ["sacrifice outlet", "death triggers"],
        }
        result = detect_archetype(intent_analysis=ia)
        self.assertEqual(result, "aristocrats")

    def test_control_detection(self) -> None:
        ia = {
            "likely_win_condition": "Control the board with counterspells and removal, win slowly with planeswalkers",
            "implicit_themes": ["counter magic", "board wipes"],
        }
        result = detect_archetype(intent_analysis=ia)
        self.assertEqual(result, "control")

    def test_blink_detection(self) -> None:
        ia = {
            "likely_win_condition": "ETB trigger abuse via Ephemerate and Conjurer's Closet flicker chains",
            "implicit_themes": ["ETB abuse", "flicker effects"],
        }
        result = detect_archetype(intent_analysis=ia, commander="Brago, King Eternal")
        self.assertEqual(result, "blink")

    def test_reanimator_detection(self) -> None:
        ia = {
            "likely_win_condition": "Buried Alive + Reanimate Razaketh to tutor combo",
            "implicit_themes": ["reanimation targets", "self-mill"],
        }
        result = detect_archetype(intent_analysis=ia)
        self.assertEqual(result, "reanimator")

    def test_landfall_detection(self) -> None:
        ia = {
            "likely_win_condition": "Landfall triggers from Azusa's extra land drops scale Scute Swarm to lethal",
            "implicit_themes": ["landfall payoffs", "extra land drops"],
        }
        result = detect_archetype(intent_analysis=ia)
        self.assertEqual(result, "landfall")

    def test_group_hug_detection(self) -> None:
        ia = {
            "likely_win_condition": "Group hug politics — every opponent draws cards while we win slow with Approach",
            "implicit_themes": ["politics", "symmetric draw"],
        }
        result = detect_archetype(intent_analysis=ia)
        self.assertEqual(result, "group_hug")

    def test_tokens_detection(self) -> None:
        ia = {
            "likely_win_condition": "Token swarm go wide with Shared Animosity for lethal trampler swing",
            "implicit_themes": ["token doublers", "anthem effects"],
        }
        result = detect_archetype(intent_analysis=ia)
        self.assertEqual(result, "tokens")

    # Iter 4 Phase 2: counters-matter archetype detection.

    def test_atraxa_proliferate_detects_counters_matter(self) -> None:
        ia = {
            "likely_win_condition": (
                "Proliferate +1/+1 counters and planeswalker loyalty counters "
                "until creatures and planeswalkers reach lethal thresholds"
            ),
            "implicit_themes": ["+1/+1 counters", "proliferate value engine",
                                "charge counters on artifacts"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["THEME_PROLIFERATE", "THEME_PLUS1_COUNTERS"],
            commander="Atraxa, Praetors' Voice",
        )
        self.assertEqual(result, "counters_matter")

    def test_roalesk_apex_hybrid_detects_counters_matter(self) -> None:
        ia = {
            "likely_win_condition": (
                "Roalesk death trigger proliferates all permanents repeatedly "
                "for compounding +1/+1 counter swarms"
            ),
            "implicit_themes": ["+1/+1 counters", "proliferate triggers"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["THEME_PROLIFERATE"],
            commander="Roalesk, Apex Hybrid",
        )
        self.assertEqual(result, "counters_matter")

    def test_pir_toothy_detects_counters_matter(self) -> None:
        ia = {
            "likely_win_condition": (
                "Pir doubles every +1/+1 counter placement on Toothy until "
                "Toothy draws the deck"
            ),
            "implicit_themes": ["+1/+1 counters", "counter-doubling"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["THEME_PLUS1_COUNTERS"],
            commander="Pir, Imaginative Rascal",
        )
        self.assertEqual(result, "counters_matter")

    def test_edgar_markov_does_not_false_positive_counters_matter(self) -> None:
        # Edgar Markov vampire tribal has SOME +1/+1 counter interactions
        # (Cordial Vampire, Bloodthirsty Conqueror), but the dominant
        # archetype signal is tribal. counters_matter must NOT win here.
        ia = {
            "likely_win_condition": (
                "Flood with vampires via Edgar's eminence; Bloodthirsty "
                "Conqueror scales with +1/+1 counters but the primary plan "
                "is wide vampire combat"
            ),
            "implicit_themes": ["vampire tribal", "lifegain payoffs",
                                "aristocrats light"],
        }
        result = detect_archetype(
            intent_analysis=ia,
            theme_hints=["TYPAL_VAMPIRES"],
            commander="Edgar Markov",
        )
        self.assertEqual(result, "tribal")

    def test_voltron_detection(self) -> None:
        ia = {
            "likely_win_condition": "Equip a hexproof commander with voltron auras for one-shot commander damage",
            "implicit_themes": ["equipment", "commander damage"],
        }
        result = detect_archetype(intent_analysis=ia, commander="Uril, the Miststalker")
        self.assertEqual(result, "voltron")


class PromptFragmentTests(unittest.TestCase):
    def test_every_archetype_has_a_fragment(self) -> None:
        for a in ARCHETYPES:
            frag = prompt_fragment_for(a)
            self.assertTrue(frag, f"No fragment for archetype {a}")
            self.assertIn(a.upper() if a != "default" else "GENERAL", frag.upper())

    def test_fragments_are_distinct(self) -> None:
        # Each non-default fragment should be unique (otherwise the
        # per-archetype customization adds no value).
        seen = set()
        for a in ARCHETYPES:
            if a == "default":
                continue
            frag = prompt_fragment_for(a)
            self.assertNotIn(frag, seen, f"Duplicate fragment for {a}")
            seen.add(frag)

    def test_unknown_archetype_falls_back_to_default(self) -> None:
        frag = prompt_fragment_for("nonexistent_archetype_xyz")
        # Same as the default fragment.
        self.assertEqual(frag, prompt_fragment_for("default"))


if __name__ == "__main__":
    unittest.main()
