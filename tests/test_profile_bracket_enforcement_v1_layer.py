from __future__ import annotations

import unittest
from unittest.mock import patch

import api.engine.layers.profile_bracket_enforcement_v1 as enforcement_layer
from api.engine.constants import GAME_CHANGERS_SET, GAME_CHANGERS_VERSION
from api.engine.layers.profile_bracket_enforcement_v1 import (
    PROFILE_BRACKET_ENFORCEMENT_V1_VERSION,
    run_profile_bracket_enforcement_v1,
)


_PAYLOAD_KEYS = [
    "version",
    "profile_id",
    "bracket_id",
    "profile_definition_version",
    "bracket_definition_version",
    "game_changers_version",
    "gc_limits_version",
    "bracket_rules_version",
    "limits",
    "category_results",
    "status",
    "counts",
    "violations",
    "unknowns",
]
_COUNT_KEYS = ["deck_size_total", "game_changers_in_deck"]
_LIMIT_KEYS = ["min", "max"]
_CATEGORY_RESULT_KEYS = ["mass_land_denial", "extra_turn_chains", "two_card_combos"]
_CATEGORY_RESULT_ENTRY_KEYS = ["policy", "count", "supported"]
_VIOLATION_KEYS = ["code", "message", "category", "card", "limit", "actual"]
_UNKNOWN_KEYS = ["code", "message"]


class ProfileBracketEnforcementV1LayerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if GAME_CHANGERS_VERSION == "gc_missing":
            raise unittest.SkipTest("Local Game Changers list is missing in this environment.")
        cls.gc_names = sorted([name for name in GAME_CHANGERS_SET if isinstance(name, str)])
        if len(cls.gc_names) < 10:
            raise unittest.SkipTest("Need at least 10 Game Changers for bracket range test coverage.")

    def _gc_cards(self, count: int) -> list[str]:
        return list(self.gc_names[:count])

    def _run(
        self,
        *,
        deck_cards: list[str],
        commander: str = "__NON_GC_COMMANDER__",
        profile_id: str = "focused",
        bracket_id: str = "B2",
        game_changers_version: str = GAME_CHANGERS_VERSION,
        bracket_definition_version: str = "bracket_v0",
        profile_definition_version: str = "profile_defaults_v1_10",
        category_support: dict | None = None,
        primitive_index_by_slot: dict[str, list[str]] | None = None,
        deck_slot_ids_playable: list[str] | None = None,
        oracle_id_by_name: dict[str, str] | None = None,
    ) -> dict:
        kwargs = {
            "deck_cards": deck_cards,
            "commander": commander,
            "profile_id": profile_id,
            "bracket_id": bracket_id,
            "game_changers_version": game_changers_version,
            "bracket_definition_version": bracket_definition_version,
            "profile_definition_version": profile_definition_version,
            "primitive_index_by_slot": primitive_index_by_slot,
            "deck_slot_ids_playable": deck_slot_ids_playable,
            "oracle_id_by_name": oracle_id_by_name,
        }
        if category_support is None:
            return run_profile_bracket_enforcement_v1(**kwargs)

        with patch.object(
            enforcement_layer,
            "_resolve_category_support_counts",
            return_value=category_support,
        ):
            return run_profile_bracket_enforcement_v1(**kwargs)

    def _supported_counts(
        self,
        *,
        mass_land_denial: int = 0,
        extra_turn_chains: int = 0,
        two_card_combos: int = 0,
    ) -> dict:
        return {
            "mass_land_denial": {
                "supported": True,
                "count": mass_land_denial,
            },
            "extra_turn_chains": {
                "supported": True,
                "count": extra_turn_chains,
            },
            "two_card_combos": {
                "supported": True,
                "count": two_card_combos,
            },
        }

    def _assert_base_shape(self, payload: dict) -> None:
        self.assertEqual(list(payload.keys()), _PAYLOAD_KEYS)
        self.assertEqual(payload.get("version"), PROFILE_BRACKET_ENFORCEMENT_V1_VERSION)
        self.assertEqual(payload.get("gc_limits_version"), "gc_limits_v1")
        self.assertEqual(payload.get("bracket_rules_version"), "bracket_rules_v2")

        limits = payload.get("limits") if isinstance(payload.get("limits"), dict) else {}
        self.assertEqual(list(limits.keys()), _LIMIT_KEYS)

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        self.assertEqual(list(category_results.keys()), _CATEGORY_RESULT_KEYS)
        for category in _CATEGORY_RESULT_KEYS:
            entry = category_results.get(category) if isinstance(category_results.get(category), dict) else {}
            self.assertEqual(list(entry.keys()), _CATEGORY_RESULT_ENTRY_KEYS)

        counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        self.assertEqual(list(counts.keys()), _COUNT_KEYS)

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        for violation in violations:
            self.assertEqual(list(violation.keys()), _VIOLATION_KEYS)

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        for unknown in unknowns:
            self.assertEqual(list(unknown.keys()), _UNKNOWN_KEYS)

    def test_b1_with_1_gc_is_error_max_exceeded(self) -> None:
        payload = self._run(
            deck_cards=self._gc_cards(1),
            bracket_id="B1",
            category_support=self._supported_counts(),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("limits"), {"min": 0, "max": 0})

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].get("code"), "GAME_CHANGER_MAX_EXCEEDED")
        self.assertEqual(violations[0].get("category"), "game_changers")
        self.assertEqual(violations[0].get("limit"), 0)
        self.assertEqual(violations[0].get("actual"), 1)

    def test_b1_with_0_gc_is_ok(self) -> None:
        payload = self._run(
            deck_cards=[],
            bracket_id="B1",
            category_support=self._supported_counts(),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("limits"), {"min": 0, "max": 0})
        self.assertEqual(payload.get("violations"), [])
        self.assertEqual(payload.get("unknowns"), [])

    def test_b3_with_0_gc_is_error_min_not_met(self) -> None:
        payload = self._run(
            deck_cards=[],
            bracket_id="B3",
            category_support=self._supported_counts(),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("limits"), {"min": 1, "max": 3})

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].get("code"), "GAME_CHANGER_MIN_NOT_MET")
        self.assertEqual(violations[0].get("category"), "game_changers")
        self.assertEqual(violations[0].get("limit"), 1)
        self.assertEqual(violations[0].get("actual"), 0)

    def test_b3_two_card_combos_track_only_with_supported_count_has_no_violation(self) -> None:
        payload = self._run(
            deck_cards=self._gc_cards(1),
            bracket_id="B3",
            category_support=self._supported_counts(two_card_combos=2),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "OK")

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        two_card = category_results.get("two_card_combos") if isinstance(category_results.get("two_card_combos"), dict) else {}
        self.assertEqual(two_card.get("policy"), "TRACK_ONLY")
        self.assertEqual(two_card.get("supported"), True)
        self.assertEqual(two_card.get("count"), 2)

        self.assertEqual(payload.get("violations"), [])
        self.assertEqual(payload.get("unknowns"), [])

    def test_b1_two_card_combos_disallow_with_supported_count_is_violation(self) -> None:
        payload = self._run(
            deck_cards=[],
            bracket_id="B1",
            category_support=self._supported_counts(two_card_combos=1),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "ERROR")

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].get("code"), "TWO_CARD_COMBOS_DISALLOWED")
        self.assertEqual(violations[0].get("category"), "two_card_combos")

    def test_mass_land_denial_runtime_support_count_and_disallow_violation(self) -> None:
        for bracket_id in ("B1", "B2", "B3"):
            with self.subTest(bracket_id=bracket_id):
                if bracket_id == "B3":
                    deck_cards = [self.gc_names[0], "Mass LD Card"]
                    primitive_index_by_slot = {
                        "S0": [],
                        "S1": ["mass_land_denial"],
                    }
                    slot_ids_playable = ["S0", "S1"]
                else:
                    deck_cards = ["Mass LD Card"]
                    primitive_index_by_slot = {
                        "S0": ["mass_land_denial"],
                    }
                    slot_ids_playable = ["S0"]

                payload = self._run(
                    deck_cards=deck_cards,
                    bracket_id=bracket_id,
                    primitive_index_by_slot=primitive_index_by_slot,
                    deck_slot_ids_playable=slot_ids_playable,
                )
                self._assert_base_shape(payload)
                self.assertEqual(payload.get("status"), "ERROR")

                category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
                mass_land_denial = (
                    category_results.get("mass_land_denial")
                    if isinstance(category_results.get("mass_land_denial"), dict)
                    else {}
                )
                self.assertEqual(mass_land_denial.get("policy"), "DISALLOW")
                self.assertTrue(mass_land_denial.get("supported"))
                self.assertEqual(mass_land_denial.get("count"), 1)

                unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
                unknown_codes = [entry.get("code") for entry in unknowns if isinstance(entry, dict)]
                self.assertNotIn("MISSING_SUPPORT_MASS_LAND_DENIAL", unknown_codes)

                violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
                disallow_matches = [
                    entry
                    for entry in violations
                    if isinstance(entry, dict)
                    and entry.get("code") == "MASS_LAND_DENIAL_DISALLOWED"
                    and entry.get("category") == "mass_land_denial"
                ]
                self.assertGreaterEqual(len(disallow_matches), 1)

    def test_extra_turn_runtime_support_count_and_disallow_violation(self) -> None:
        for bracket_id in ("B1", "B2", "B3"):
            with self.subTest(bracket_id=bracket_id):
                if bracket_id == "B3":
                    deck_cards = [self.gc_names[0], "Extra Turn Card"]
                    primitive_index_by_slot = {
                        "S0": [],
                        "S1": ["extra_turn"],
                    }
                    slot_ids_playable = ["S0", "S1"]
                else:
                    deck_cards = ["Extra Turn Card"]
                    primitive_index_by_slot = {
                        "S0": ["extra_turn"],
                    }
                    slot_ids_playable = ["S0"]

                payload = self._run(
                    deck_cards=deck_cards,
                    bracket_id=bracket_id,
                    primitive_index_by_slot=primitive_index_by_slot,
                    deck_slot_ids_playable=slot_ids_playable,
                )
                self._assert_base_shape(payload)
                self.assertEqual(payload.get("status"), "ERROR")

                category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
                extra_turn_chains = (
                    category_results.get("extra_turn_chains")
                    if isinstance(category_results.get("extra_turn_chains"), dict)
                    else {}
                )
                self.assertEqual(extra_turn_chains.get("policy"), "DISALLOW")
                self.assertTrue(extra_turn_chains.get("supported"))
                self.assertEqual(extra_turn_chains.get("count"), 1)

                unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
                unknown_codes = [entry.get("code") for entry in unknowns if isinstance(entry, dict)]
                self.assertNotIn("MISSING_SUPPORT_EXTRA_TURN_CHAINS", unknown_codes)

                violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
                disallow_matches = [
                    entry
                    for entry in violations
                    if isinstance(entry, dict)
                    and entry.get("code") == "EXTRA_TURN_CHAINS_DISALLOWED"
                    and entry.get("category") == "extra_turn_chains"
                ]
                self.assertGreaterEqual(len(disallow_matches), 1)

    # Canonical real-data pair used by the runtime-support tests below.
    # The on-disk v2 pack (post-2026-05-06 bulk-dump ingest) contains this pair.
    _THORACLE_NAME = "Thassa's Oracle"
    _CONSULTATION_NAME = "Demonic Consultation"
    _THORACLE_ORACLE = "1de1b591-a73f-4974-b507-8c63e07a0868"
    _CONSULTATION_ORACLE = "9a1412db-45ad-46ea-8f12-a85d203113d8"

    def _thoracle_consult_mapping(self) -> dict[str, str]:
        return {
            self._THORACLE_NAME.lower(): self._THORACLE_ORACLE,
            self._CONSULTATION_NAME.lower(): self._CONSULTATION_ORACLE,
        }

    def test_two_card_combo_runtime_support_count_and_b1_disallow_violation(self) -> None:
        # Rewritten 2026-05-06 (Priority A close-out): now uses real card names
        # + oracle_id_by_name. Original version hardcoded "Combo Piece A/B"
        # placeholder strings that lived in two_card_combos_v2.json before the
        # bulk-dump ingest replaced them with real Spellbook UUIDs.
        payload = self._run(
            deck_cards=[self._THORACLE_NAME, self._CONSULTATION_NAME],
            bracket_id="B1",
            primitive_index_by_slot={
                "S0": [],
                "S1": [],
            },
            deck_slot_ids_playable=["S0", "S1"],
            oracle_id_by_name=self._thoracle_consult_mapping(),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "ERROR")

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        two_card_combos = (
            category_results.get("two_card_combos")
            if isinstance(category_results.get("two_card_combos"), dict)
            else {}
        )
        self.assertEqual(two_card_combos.get("policy"), "DISALLOW")
        self.assertTrue(two_card_combos.get("supported"))
        self.assertEqual(two_card_combos.get("count"), 1)

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        disallow_matches = [
            entry
            for entry in violations
            if isinstance(entry, dict)
            and entry.get("code") == "TWO_CARD_COMBOS_DISALLOWED"
            and entry.get("category") == "two_card_combos"
        ]
        self.assertGreaterEqual(len(disallow_matches), 1)

    def test_two_card_combo_runtime_support_count_and_b3_track_only_has_no_violation(self) -> None:
        # Rewritten 2026-05-06: real card names + oracle_id_by_name (see B1 sibling).
        payload = self._run(
            deck_cards=[self.gc_names[0], self._THORACLE_NAME, self._CONSULTATION_NAME],
            bracket_id="B3",
            primitive_index_by_slot={
                "S0": [],
                "S1": [],
                "S2": [],
            },
            deck_slot_ids_playable=["S0", "S1", "S2"],
            oracle_id_by_name=self._thoracle_consult_mapping(),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "OK")

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        two_card_combos = (
            category_results.get("two_card_combos")
            if isinstance(category_results.get("two_card_combos"), dict)
            else {}
        )
        self.assertEqual(two_card_combos.get("policy"), "TRACK_ONLY")
        self.assertTrue(two_card_combos.get("supported"))
        self.assertEqual(two_card_combos.get("count"), 1)

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        disallow_matches = [
            entry
            for entry in violations
            if isinstance(entry, dict)
            and entry.get("code") == "TWO_CARD_COMBOS_DISALLOWED"
            and entry.get("category") == "two_card_combos"
        ]
        self.assertEqual(len(disallow_matches), 0)

    def test_two_card_combo_runtime_support_count_and_b4_allow_has_no_violation(self) -> None:
        # Rewritten 2026-05-06: real card names + oracle_id_by_name (see B1 sibling).
        payload = self._run(
            deck_cards=[self._THORACLE_NAME, self._CONSULTATION_NAME],
            bracket_id="B4",
            primitive_index_by_slot={
                "S0": [],
                "S1": [],
            },
            deck_slot_ids_playable=["S0", "S1"],
            oracle_id_by_name=self._thoracle_consult_mapping(),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "OK")

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        two_card_combos = (
            category_results.get("two_card_combos")
            if isinstance(category_results.get("two_card_combos"), dict)
            else {}
        )
        self.assertEqual(two_card_combos.get("policy"), "ALLOW")
        self.assertTrue(two_card_combos.get("supported"))
        self.assertEqual(two_card_combos.get("count"), 1)
        self.assertEqual(payload.get("violations"), [])

    def test_two_card_combo_runtime_missing_support_emits_unknown_without_crashing(self) -> None:
        with patch.object(enforcement_layer, "detect_two_card_combos", side_effect=RuntimeError("TWO_CARD_COMBOS_V2_MISSING: test")):
            payload = self._run(
                deck_cards=["Combo Piece A", "Combo Piece B"],
                bracket_id="B4",
                primitive_index_by_slot={
                    "S0": [],
                    "S1": [],
                },
                deck_slot_ids_playable=["S0", "S1"],
            )

        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "WARN")

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        two_card_combos = (
            category_results.get("two_card_combos")
            if isinstance(category_results.get("two_card_combos"), dict)
            else {}
        )
        self.assertEqual(two_card_combos.get("policy"), "ALLOW")
        self.assertFalse(two_card_combos.get("supported"))
        self.assertIsNone(two_card_combos.get("count"))

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        unknown_codes = [entry.get("code") for entry in unknowns if isinstance(entry, dict)]
        self.assertIn("MISSING_SUPPORT_TWO_CARD_COMBOS", unknown_codes)

    def test_b2_two_card_combos_disallow_with_supported_count_is_violation(self) -> None:
        payload = self._run(
            deck_cards=[],
            bracket_id="B2",
            category_support=self._supported_counts(two_card_combos=1),
        )
        self._assert_base_shape(payload)

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].get("code"), "TWO_CARD_COMBOS_DISALLOWED")
        self.assertEqual(violations[0].get("category"), "two_card_combos")

    def test_unknown_bracket_warn_with_null_policies(self) -> None:
        payload = self._run(
            deck_cards=[],
            bracket_id="B9",
            category_support=self._supported_counts(),
        )
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("limits"), {"min": None, "max": None})

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        for category in _CATEGORY_RESULT_KEYS:
            entry = category_results.get(category) if isinstance(category_results.get(category), dict) else {}
            self.assertIsNone(entry.get("policy"))

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        unknown_codes = [entry.get("code") for entry in unknowns if isinstance(entry, dict)]
        self.assertIn("UNKNOWN_BRACKET", unknown_codes)

    def test_missing_support_emits_unknowns_and_warn(self) -> None:
        payload = self._run(deck_cards=self._gc_cards(1), bracket_id="B3")
        self._assert_base_shape(payload)
        self.assertEqual(payload.get("status"), "WARN")

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        unknown_codes = [entry.get("code") for entry in unknowns if isinstance(entry, dict)]
        self.assertIn("MISSING_SUPPORT_MASS_LAND_DENIAL", unknown_codes)
        self.assertIn("MISSING_SUPPORT_EXTRA_TURN_CHAINS", unknown_codes)
        self.assertNotIn("MISSING_SUPPORT_TWO_CARD_COMBOS", unknown_codes)

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        for category in ("mass_land_denial", "extra_turn_chains"):
            entry = category_results.get(category) if isinstance(category_results.get(category), dict) else {}
            self.assertFalse(entry.get("supported"))
            self.assertIsNone(entry.get("count"))

        two_card_combos = (
            category_results.get("two_card_combos")
            if isinstance(category_results.get("two_card_combos"), dict)
            else {}
        )
        self.assertTrue(two_card_combos.get("supported"))
        self.assertEqual(two_card_combos.get("count"), 0)

    def test_deterministic_for_same_input(self) -> None:
        kwargs = {
            "deck_cards": self._gc_cards(3),
            "commander": "__NON_GC_COMMANDER__",
            "profile_id": "focused",
            "bracket_id": "B3",
            "game_changers_version": GAME_CHANGERS_VERSION,
            "bracket_definition_version": "bracket_v0",
            "profile_definition_version": "profile_defaults_v1_10",
        }

        support = self._supported_counts()
        with patch.object(enforcement_layer, "_resolve_category_support_counts", return_value=support):
            first = run_profile_bracket_enforcement_v1(**kwargs)
            second = run_profile_bracket_enforcement_v1(**kwargs)

        self.assertEqual(first, second)

    def test_oracle_id_by_name_resolves_combo_against_uuid_keyed_pack(self) -> None:
        # Regression for the 2026-05-05 pre-ingest audit's HIGH-risk finding: once the
        # Spellbook ingest lands, pair.a/.b in two_card_combos_v2.json are lowercase
        # oracle UUIDs. The bracket-enforcement layer is fed deck card NAMES by the
        # orchestrator. Without an oracle_id_by_name mapping, the detector's lowercased
        # name strings never match the UUIDs and combo detection silently returns 0.
        # This test exercises the kwarg path end-to-end: mock the v2 loader with a
        # UUID-keyed payload, supply oracle_id_by_name for the deck's names, and
        # assert the combo is registered (and B1 raises the disallow violation).
        import api.engine.combos.two_card_combos_v2 as combos_v2

        thoracle_oracle = "1de1b591-a73f-4974-b507-8c63e07a0868"
        consultation_oracle = "9a1412db-45ad-46ea-8f12-a85d203113d8"

        # pair.a < pair.b is enforced by the loader; '1de1b591' < '9a1412db' lexicographically.
        uuid_keyed_payload = {
            "version": "two_card_combos_v2",
            "pairs": [
                {
                    "a": thoracle_oracle,
                    "b": consultation_oracle,
                    "variant_ids": ["TEST-THORACLE-CONSULT"],
                }
            ],
        }

        oracle_id_by_name = {
            "thassa's oracle": thoracle_oracle,
            "demonic consultation": consultation_oracle,
        }

        with patch.object(
            combos_v2,
            "load_two_card_combos_prefer_v2",
            return_value=uuid_keyed_payload,
        ):
            payload = run_profile_bracket_enforcement_v1(
                deck_cards=["Thassa's Oracle", "Demonic Consultation"],
                commander="__NON_GC_COMMANDER__",
                profile_id="focused",
                bracket_id="B1",
                game_changers_version=GAME_CHANGERS_VERSION,
                bracket_definition_version="bracket_v0",
                profile_definition_version="profile_defaults_v1_10",
                oracle_id_by_name=oracle_id_by_name,
            )

        category_results = payload.get("category_results") if isinstance(payload.get("category_results"), dict) else {}
        two_card = (
            category_results.get("two_card_combos")
            if isinstance(category_results.get("two_card_combos"), dict)
            else {}
        )
        self.assertTrue(two_card.get("supported"))
        self.assertEqual(two_card.get("count"), 1)
        self.assertEqual(two_card.get("policy"), "DISALLOW")

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        disallow_codes = [v.get("code") for v in violations if isinstance(v, dict)]
        self.assertIn("TWO_CARD_COMBOS_DISALLOWED", disallow_codes)


if __name__ == "__main__":
    unittest.main()
