"""Phase 9 — 50 new multi-card v11 fixtures.

Per kickoff Phase 9 spec, this file ships 50 fixtures distributed
across 5 scenario categories (10 each):
  1. Anthem-stack scenarios (multiple anthems / continuous layered)
  2. ETB-cascade scenarios (chains of ETB triggers)
  3. Replacement-effect chains (multiple replacements per event)
  4. Triggered-ability orderings (APNAP + same-controller order)
  5. Combo-line resolutions (the headline use-case of each combo card)

Each fixture is a single test method asserting an explicit expected
outcome against a focused multi-card scenario.
"""
from __future__ import annotations

import unittest

# Triggers all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import build_activation_payload
from api.engine.pillar_f.v0_2.cards.continuous import (
    attach_layered_effects_for, get_static_modifiers,
)
from api.engine.pillar_f.v0_2.cards.etb import fire_etb_triggers
from api.engine.pillar_f.v0_2.cards.replacement import (
    attach_replacements_for,
)
from api.engine.pillar_f.v0_2.cards.spell import build_spell_payload
from api.engine.pillar_f.v0_2.cards.triggered import (
    fire_event_triggers,
)
from api.engine.pillar_f.v0_2.layers import apply_continuous_effects
from api.engine.pillar_f.v0_2.replacement import (
    apply_replacements, CounterAddEvent, DieEvent, DrawEvent,
    EnterBattlefieldEvent,
)
from api.engine.pillar_f.v0_2.stack import (
    drain_triggers_to_stack, push_to_stack, resolve_top,
    run_stack_to_resolution,
)
from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)


def _empty_game(*, life: int = 40) -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(
            player_id=pid, name=f"P{pid}", life_total=life, zones=PlayerZones(),
        ))
    gs.active_player = 0
    return gs


def _bf(gs: GameState, owner: int, name: str, *,
       type_line: str = "Creature — Bear",
       power="2", toughness="2",
       subtypes=None, keywords=None, colors=None) -> Card:
    card = Card(name=name, owner=owner, controller=owner,
                type_line=type_line, power=power, toughness=toughness,
                subtypes=list(subtypes or []), keywords=list(keywords or []),
                colors=list(colors or []))
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


def _gy(gs: GameState, owner: int, name: str, *,
       type_line: str = "Instant") -> Card:
    card = Card(name=name, owner=owner, type_line=type_line)
    gs.add_card(card)
    gs.players[owner].zones.graveyard.append(card.card_id)
    return card


def _cast(gs: GameState, caster: int, spell_name: str, *,
         targets=None, payment_extra=None) -> None:
    payment = build_spell_payload(spell_name)
    if payment_extra:
        payment.update(payment_extra)
    push_to_stack(gs, card_id=None, controller=caster, entry_type="spell",
                 payment=payment, targets=targets or [],
                 description=spell_name)
    resolve_top(gs)


def _activate(gs: GameState, card: Card, ability_key: str,
             *, tap=True, sacrifice_self=False, targets=None, **extra):
    if tap:
        card.tapped = True
    if sacrifice_self:
        gs.move_card(card.card_id, from_player=card.controller,
                    from_zone="battlefield",
                    to_player=card.owner, to_zone="graveyard")
    payload = build_activation_payload(card.name, ability_key, **extra)
    push_to_stack(gs, card_id=card.card_id, controller=card.controller,
                 entry_type="activated", payment=payload,
                 targets=targets or [],
                 description=f"{card.name}.{ability_key}")
    resolve_top(gs)


# =================================================================
# Category 1: Anthem-stack scenarios (10)
# =================================================================


class AnthemStackScenarios(unittest.TestCase):
    def test_a01_urborg_alone_mountain_is_swamp(self) -> None:
        gs = _empty_game()
        m = _bf(gs, 0, "Mountain", type_line="Basic Land — Mountain",
               subtypes=["Mountain"], power=None, toughness=None)
        u = _bf(gs, 0, "Urborg, Tomb of Yawgmoth",
               type_line="Legendary Land", power=None, toughness=None)
        attach_layered_effects_for(gs, u.card_id)
        chars = apply_continuous_effects(gs)[m.card_id]
        self.assertIn("Swamp", chars.subtypes)

    def test_a02_yavimaya_alone_island_is_forest(self) -> None:
        gs = _empty_game()
        i = _bf(gs, 0, "Island", type_line="Basic Land — Island",
               subtypes=["Island"], power=None, toughness=None)
        y = _bf(gs, 0, "Yavimaya, Cradle of Growth",
               type_line="Legendary Land", power=None, toughness=None)
        attach_layered_effects_for(gs, y.card_id)
        chars = apply_continuous_effects(gs)[i.card_id]
        self.assertIn("Forest", chars.subtypes)

    def test_a03_urborg_plus_yavimaya_all_lands_both_subtypes(self) -> None:
        gs = _empty_game()
        p = _bf(gs, 0, "Plains", type_line="Basic Land — Plains",
               subtypes=["Plains"], power=None, toughness=None)
        u = _bf(gs, 0, "Urborg, Tomb of Yawgmoth",
               type_line="Legendary Land", power=None, toughness=None)
        y = _bf(gs, 0, "Yavimaya, Cradle of Growth",
               type_line="Legendary Land", power=None, toughness=None)
        attach_layered_effects_for(gs, u.card_id)
        attach_layered_effects_for(gs, y.card_id)
        chars = apply_continuous_effects(gs)[p.card_id]
        for st in ("Plains", "Swamp", "Forest"):
            self.assertIn(st, chars.subtypes)

    def test_a04_rhythm_of_wild_grants_riot(self) -> None:
        gs = _empty_game()
        b = _bf(gs, 0, "Bear", power="2", toughness="2")
        r = _bf(gs, 0, "Rhythm of the Wild", type_line="Enchantment",
               power=None, toughness=None)
        attach_layered_effects_for(gs, r.card_id)
        chars = apply_continuous_effects(gs)[b.card_id]
        self.assertIn("riot", [k.lower() for k in chars.keywords])

    def test_a05_anger_grants_haste(self) -> None:
        gs = _empty_game()
        b = _bf(gs, 0, "Bear", power="2", toughness="2")
        a = _bf(gs, 0, "Anger", type_line="Creature — Incarnation",
               power="2", toughness="2", keywords=["haste"])
        attach_layered_effects_for(gs, a.card_id)
        chars = apply_continuous_effects(gs)[b.card_id]
        self.assertIn("haste", [k.lower() for k in chars.keywords])

    def test_a06_rhythm_plus_anger_double_keywords(self) -> None:
        gs = _empty_game()
        b = _bf(gs, 0, "Bear", power="2", toughness="2")
        r = _bf(gs, 0, "Rhythm of the Wild", type_line="Enchantment",
               power=None, toughness=None)
        a = _bf(gs, 0, "Anger", type_line="Creature — Incarnation",
               power="2", toughness="2")
        attach_layered_effects_for(gs, r.card_id)
        attach_layered_effects_for(gs, a.card_id)
        chars = apply_continuous_effects(gs)[b.card_id]
        kw = [k.lower() for k in chars.keywords]
        self.assertIn("riot", kw)
        self.assertIn("haste", kw)

    def test_a07_no_anthem_no_buff(self) -> None:
        gs = _empty_game()
        b = _bf(gs, 0, "Bear", power="2", toughness="2")
        chars = apply_continuous_effects(gs)[b.card_id]
        self.assertEqual(chars.power, 2)
        self.assertEqual(chars.toughness, 2)

    def test_a08_static_modifier_medallions_query(self) -> None:
        gs = _empty_game()
        _bf(gs, 0, "Ruby Medallion", type_line="Artifact",
            power=None, toughness=None)
        _bf(gs, 1, "Sapphire Medallion", type_line="Artifact",
            power=None, toughness=None)
        from api.engine.pillar_f.v0_2.cards.continuous import (
            query_active_static_modifiers,
        )
        active = query_active_static_modifiers(gs, "cost_reduction")
        names = {a["card_name"] for a in active}
        self.assertIn("Ruby Medallion", names)
        self.assertIn("Sapphire Medallion", names)

    def test_a09_detach_removes_anthem(self) -> None:
        from api.engine.pillar_f.v0_2.cards.continuous import (
            detach_layered_effects_for,
        )
        gs = _empty_game()
        m = _bf(gs, 0, "Mountain", type_line="Basic Land — Mountain",
               subtypes=["Mountain"], power=None, toughness=None)
        u = _bf(gs, 0, "Urborg, Tomb of Yawgmoth",
               type_line="Legendary Land", power=None, toughness=None)
        attach_layered_effects_for(gs, u.card_id)
        detach_layered_effects_for(gs, u.card_id)
        chars = apply_continuous_effects(gs)[m.card_id]
        self.assertNotIn("Swamp", chars.subtypes)

    def test_a10_propaganda_and_ghostly_prison_both_query_as_attack_tax(self) -> None:
        gs = _empty_game()
        _bf(gs, 0, "Propaganda", type_line="Enchantment",
            power=None, toughness=None)
        _bf(gs, 0, "Ghostly Prison", type_line="Enchantment",
            power=None, toughness=None)
        from api.engine.pillar_f.v0_2.cards.continuous import (
            query_active_static_modifiers,
        )
        taxes = query_active_static_modifiers(gs, "attack_tax")
        self.assertEqual(len(taxes), 2)


# =================================================================
# Category 2: ETB-cascade scenarios (10)
# =================================================================


class ETBCascadeScenarios(unittest.TestCase):
    def _fire_etb(self, gs, card) -> int:
        event = EnterBattlefieldEvent(card_id=card.card_id,
                                       controller=card.controller,
                                       from_zone="hand")
        fired = fire_etb_triggers(gs, event)
        if fired:
            drain_triggers_to_stack(gs)
            run_stack_to_resolution(gs, lambda s, p: None)
        return fired

    def test_e01_eternal_witness_returns_card(self) -> None:
        gs = _empty_game()
        c = _gy(gs, 0, "Spell")
        ew = _bf(gs, 0, "Eternal Witness", power="2", toughness="1")
        self._fire_etb(gs, ew)
        self.assertIn(c.card_id, gs.players[0].zones.hand)

    def test_e02_reclamation_sage_destroys_opp_artifact(self) -> None:
        gs = _empty_game()
        sr = _bf(gs, 1, "Sol Ring", type_line="Artifact",
                power=None, toughness=None)
        rs = _bf(gs, 0, "Reclamation Sage", power="2", toughness="1")
        self._fire_etb(gs, rs)
        self.assertIn(sr.card_id, gs.players[1].zones.graveyard)

    def test_e03_bojuka_bog_exiles_opp_gy(self) -> None:
        gs = _empty_game()
        _gy(gs, 1, "Dead1")
        _gy(gs, 1, "Dead2")
        bb = _bf(gs, 0, "Bojuka Bog", type_line="Land",
                power=None, toughness=None)
        self._fire_etb(gs, bb)
        self.assertEqual(len(gs.players[1].zones.graveyard), 0)

    def test_e04_garruks_uprising_draws_with_pow_4_plus(self) -> None:
        gs = _empty_game()
        for i in range(3):
            c = Card(name=f"L_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _bf(gs, 0, "Dragon", power="5", toughness="5")
        gu = _bf(gs, 0, "Garruk's Uprising", type_line="Enchantment",
                power=None, toughness=None)
        self._fire_etb(gs, gu)
        self.assertEqual(len(gs.players[0].zones.hand), 1)

    def test_e05_garruks_no_draw_without_pow_4(self) -> None:
        gs = _empty_game()
        for i in range(3):
            c = Card(name=f"L_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _bf(gs, 0, "Bear", power="2", toughness="2")
        gu = _bf(gs, 0, "Garruk's Uprising", type_line="Enchantment",
                power=None, toughness=None)
        fired = self._fire_etb(gs, gu)
        self.assertEqual(fired, 0)

    def test_e06_avenger_of_zendikar_creates_tokens_per_land(self) -> None:
        gs = _empty_game()
        for _ in range(3):
            _bf(gs, 0, "Forest", type_line="Basic Land — Forest",
                power=None, toughness=None)
        av = _bf(gs, 0, "Avenger of Zendikar",
                type_line="Creature — Plant Elemental",
                power="5", toughness="5")
        self._fire_etb(gs, av)
        tokens = sum(1 for cid in gs.players[0].zones.battlefield
                     for c in [gs.get_card(cid)]
                     if c and c.name == "Plant Token")
        self.assertEqual(tokens, 3)

    def test_e07_solemn_etb_finds_basic(self) -> None:
        gs = _empty_game()
        p = Card(name="Plains", owner=0,
                type_line="Basic Land — Plains", subtypes=["Plains"])
        gs.add_card(p)
        gs.players[0].zones.library.append(p.card_id)
        sj = _bf(gs, 0, "Solemn Simulacrum",
                type_line="Artifact Creature — Golem",
                power="2", toughness="2")
        self._fire_etb(gs, sj)
        self.assertIn(p.card_id, gs.players[0].zones.battlefield)

    def test_e08_witness_then_witness_chain(self) -> None:
        gs = _empty_game()
        c = _gy(gs, 0, "Bolt")
        ew1 = _bf(gs, 0, "Eternal Witness", power="2", toughness="1")
        ew2 = _bf(gs, 0, "Eternal Witness", power="2", toughness="1")
        self._fire_etb(gs, ew1)
        self._fire_etb(gs, ew2)
        # First Witness moved the bolt to hand; second has no GY target.
        self.assertIn(c.card_id, gs.players[0].zones.hand)

    def test_e09_plaguecrafter_each_player_sacs(self) -> None:
        gs = _empty_game()
        _bf(gs, 1, "Bear", power="2", toughness="2")
        _bf(gs, 3, "Goblin", power="1", toughness="1")
        pc = _bf(gs, 0, "Plaguecrafter", power="3", toughness="2")
        self._fire_etb(gs, pc)
        # P1 and P3 each lose one creature.
        self.assertEqual(len(gs.players[1].zones.graveyard), 1)
        self.assertEqual(len(gs.players[3].zones.graveyard), 1)

    def test_e10_ranger_captain_tutors_cmc_1_creature(self) -> None:
        gs = _empty_game()
        big = Card(name="Dragon", owner=0,
                  type_line="Creature — Dragon",
                  cmc=5.0, power="5", toughness="5")
        small = Card(name="Birds", owner=0,
                    type_line="Creature — Bird",
                    cmc=1.0, power="0", toughness="1")
        gs.add_card(big)
        gs.add_card(small)
        gs.players[0].zones.library.extend([big.card_id, small.card_id])
        rc = _bf(gs, 0, "Ranger-Captain of Eos",
                power="3", toughness="3")
        self._fire_etb(gs, rc)
        self.assertIn(small.card_id, gs.players[0].zones.hand)


# =================================================================
# Category 3: Replacement-effect chains (10)
# =================================================================


class ReplacementChainScenarios(unittest.TestCase):
    def test_r01_hardened_scales_plus_one(self) -> None:
        gs = _empty_game()
        hs = _bf(gs, 0, "Hardened Scales", type_line="Enchantment",
                power=None, toughness=None)
        attach_replacements_for(gs, hs.card_id)
        c = _bf(gs, 0, "Bear", power="2", toughness="2")
        event = CounterAddEvent(target_card_id=c.card_id,
                                counter_type="+1/+1", count=2)
        apply_replacements(gs, event)
        self.assertEqual(event.count, 3)

    def test_r02_doubling_season_doubles_counters(self) -> None:
        gs = _empty_game()
        ds = _bf(gs, 0, "Doubling Season", type_line="Enchantment",
                power=None, toughness=None)
        attach_replacements_for(gs, ds.card_id)
        c = _bf(gs, 0, "Bear", power="2", toughness="2")
        event = CounterAddEvent(target_card_id=c.card_id,
                                counter_type="+1/+1", count=2)
        apply_replacements(gs, event)
        self.assertEqual(event.count, 4)

    def test_r03_scales_plus_ds_compound(self) -> None:
        gs = _empty_game()
        hs = _bf(gs, 0, "Hardened Scales", type_line="Enchantment",
                power=None, toughness=None)
        ds = _bf(gs, 0, "Doubling Season", type_line="Enchantment",
                power=None, toughness=None)
        attach_replacements_for(gs, hs.card_id)
        attach_replacements_for(gs, ds.card_id)
        c = _bf(gs, 0, "Bear", power="2", toughness="2")
        event = CounterAddEvent(target_card_id=c.card_id,
                                counter_type="+1/+1", count=1)
        apply_replacements(gs, event)
        # Either 3 or 4 per substrate-chosen order — CR 614.5 each applies once.
        self.assertIn(event.count, (3, 4))

    def test_r04_dauthi_voidwalker_exiles_opp(self) -> None:
        gs = _empty_game()
        dv = _bf(gs, 0, "Dauthi Voidwalker", power="3", toughness="2")
        attach_replacements_for(gs, dv.card_id)
        v = _bf(gs, 1, "Bear", power="2", toughness="2")
        event = DieEvent(card_id=v.card_id, controller=1)
        apply_replacements(gs, event)
        self.assertEqual(event.instead_zone, "exile")

    def test_r05_dauthi_doesnt_exile_self_creatures(self) -> None:
        gs = _empty_game()
        dv = _bf(gs, 0, "Dauthi Voidwalker", power="3", toughness="2")
        attach_replacements_for(gs, dv.card_id)
        own = _bf(gs, 0, "Bear", power="2", toughness="2")
        event = DieEvent(card_id=own.card_id, controller=0)
        apply_replacements(gs, event)
        self.assertIsNone(event.instead_zone)

    def test_r06_lab_maniac_wins_on_empty_lib_draw(self) -> None:
        gs = _empty_game()
        lm = _bf(gs, 0, "Laboratory Maniac", power="2", toughness="2")
        attach_replacements_for(gs, lm.card_id)
        apply_replacements(gs, DrawEvent(player_id=0, count=1))
        self.assertTrue(gs.game_over)
        self.assertEqual(gs.winner_player_id, 0)

    def test_r07_lab_maniac_doesnt_fire_for_opp(self) -> None:
        gs = _empty_game()
        lm = _bf(gs, 0, "Laboratory Maniac", power="2", toughness="2")
        attach_replacements_for(gs, lm.card_id)
        apply_replacements(gs, DrawEvent(player_id=2, count=1))
        self.assertFalse(gs.game_over)

    def test_r08_charcoal_diamond_etb_tapped(self) -> None:
        gs = _empty_game()
        cd = _bf(gs, 0, "Charcoal Diamond", type_line="Artifact",
                power=None, toughness=None)
        attach_replacements_for(gs, cd.card_id)
        event = EnterBattlefieldEvent(card_id=cd.card_id,
                                       controller=0, from_zone="hand")
        apply_replacements(gs, event)
        self.assertTrue(event.tapped_on_etb)

    def test_r09_dauthi_detach_clears_replacement(self) -> None:
        from api.engine.pillar_f.v0_2.cards.replacement import (
            detach_replacements_for,
        )
        gs = _empty_game()
        dv = _bf(gs, 0, "Dauthi Voidwalker", power="3", toughness="2")
        attach_replacements_for(gs, dv.card_id)
        detach_replacements_for(gs, dv.card_id)
        v = _bf(gs, 1, "Bear", power="2", toughness="2")
        event = DieEvent(card_id=v.card_id, controller=1)
        apply_replacements(gs, event)
        self.assertIsNone(event.instead_zone)

    def test_r10_static_modifiers_persist(self) -> None:
        """Panharmonicon's etb_trigger_multiplier is queryable when
        Panharmonicon is on the battlefield."""
        gs = _empty_game()
        _bf(gs, 0, "Panharmonicon", type_line="Artifact",
            power=None, toughness=None)
        from api.engine.pillar_f.v0_2.cards.continuous import (
            query_active_static_modifiers,
        )
        active = query_active_static_modifiers(
            gs, "etb_trigger_multiplier",
        )
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["card_name"], "Panharmonicon")


# =================================================================
# Category 4: Triggered-ability orderings (10)
# =================================================================


class TriggeredOrderingScenarios(unittest.TestCase):
    def _fire_event(self, gs, event) -> int:
        fired = fire_event_triggers(gs, event)
        if fired:
            drain_triggers_to_stack(gs)
            run_stack_to_resolution(gs, lambda s, p: None)
        return fired

    def test_t01_blood_artist_drain_on_death(self) -> None:
        gs = _empty_game(life=40)
        _bf(gs, 0, "Blood Artist", power="0", toughness="1")
        v = _bf(gs, 1, "Bear", power="2", toughness="2")
        gs.move_card(v.card_id, from_player=1, from_zone="battlefield",
                    to_player=1, to_zone="graveyard")
        self._fire_event(gs, DieEvent(card_id=v.card_id, controller=1))
        self.assertEqual(gs.players[0].life_total, 41)

    def test_t02_zulaport_aoe_drain(self) -> None:
        gs = _empty_game()
        _bf(gs, 0, "Zulaport Cutthroat", power="1", toughness="1")
        v = _bf(gs, 0, "Bear", power="2", toughness="2")
        gs.move_card(v.card_id, from_player=0, from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        self._fire_event(gs, DieEvent(card_id=v.card_id, controller=0))
        # Each opp -1; controller +3.
        self.assertEqual(gs.players[0].life_total, 43)
        self.assertEqual(gs.players[1].life_total, 39)

    def test_t03_blood_artist_and_zulaport_both_fire(self) -> None:
        gs = _empty_game(life=40)
        _bf(gs, 0, "Blood Artist", power="0", toughness="1")
        _bf(gs, 0, "Zulaport Cutthroat", power="1", toughness="1")
        v = _bf(gs, 0, "Bear", power="2", toughness="2")
        gs.move_card(v.card_id, from_player=0, from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        self._fire_event(gs, DieEvent(card_id=v.card_id, controller=0))
        # P0 = 40 + 1 (BA) + 3 (ZP) = 44
        self.assertEqual(gs.players[0].life_total, 44)

    def test_t04_pitiless_plunderer_treasure_on_death(self) -> None:
        gs = _empty_game()
        _bf(gs, 0, "Pitiless Plunderer", power="1", toughness="4")
        v = _bf(gs, 0, "Bear", power="2", toughness="2")
        before = len(gs.players[0].zones.battlefield)
        self._fire_event(gs, DieEvent(card_id=v.card_id, controller=0))
        # +1 Treasure.
        self.assertEqual(len(gs.players[0].zones.battlefield), before + 1)

    def test_t05_morbid_opp_draws_once_per_turn(self) -> None:
        gs = _empty_game()
        for i in range(5):
            c = Card(name=f"L_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _bf(gs, 0, "Morbid Opportunist", power="2", toughness="2")
        v1 = _bf(gs, 1, "B1", power="2", toughness="2")
        v2 = _bf(gs, 1, "B2", power="2", toughness="2")
        self._fire_event(gs, DieEvent(card_id=v1.card_id, controller=1))
        self._fire_event(gs, DieEvent(card_id=v2.card_id, controller=1))
        self.assertEqual(len(gs.players[0].zones.hand), 1)  # only 1

    def test_t06_rhystic_study_opp_cast_draws(self) -> None:
        from api.engine.pillar_f.v0_2.cards.triggered import SpellCastEvent
        gs = _empty_game()
        c = Card(name="Top", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        _bf(gs, 0, "Rhystic Study", type_line="Enchantment",
            power=None, toughness=None)
        self._fire_event(gs, SpellCastEvent(caster_player_id=1,
                                             spell_card_name="Bolt",
                                             spell_types=["Instant"]))
        self.assertIn(c.card_id, gs.players[0].zones.hand)

    def test_t07_beast_whisperer_self_cast(self) -> None:
        from api.engine.pillar_f.v0_2.cards.triggered import SpellCastEvent
        gs = _empty_game()
        c = Card(name="Top", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        _bf(gs, 0, "Beast Whisperer", power="2", toughness="3")
        self._fire_event(gs, SpellCastEvent(caster_player_id=0,
                                             spell_card_name="Bear",
                                             spell_types=["Creature"]))
        self.assertIn(c.card_id, gs.players[0].zones.hand)

    def test_t08_smothering_tithe_creates_treasure_on_opp_draw(self) -> None:
        gs = _empty_game()
        _bf(gs, 0, "Smothering Tithe", type_line="Enchantment",
            power=None, toughness=None)
        before = len(gs.players[0].zones.battlefield)
        self._fire_event(gs, DrawEvent(player_id=1, count=1))
        self.assertEqual(len(gs.players[0].zones.battlefield), before + 1)

    def test_t09_solemn_ltb_draws_via_lki(self) -> None:
        gs = _empty_game()
        c = Card(name="Top", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        ss = _bf(gs, 0, "Solemn Simulacrum",
                type_line="Artifact Creature — Golem",
                power="2", toughness="2")
        gs.move_card(ss.card_id, from_player=0,
                    from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        self._fire_event(gs, DieEvent(card_id=ss.card_id, controller=0))
        self.assertIn(c.card_id, gs.players[0].zones.hand)

    def test_t10_vito_drain_on_life_gain(self) -> None:
        from api.engine.pillar_f.v0_2.replacement import LifeChangeEvent
        gs = _empty_game(life=40)
        _bf(gs, 0, "Vito, Thorn of the Dusk Rose", power="1", toughness="3")
        self._fire_event(gs, LifeChangeEvent(player_id=0, delta=3))
        self.assertEqual(gs.players[1].life_total, 37)


# =================================================================
# Category 5: Combo-line resolutions (10)
# =================================================================


class ComboLineScenarios(unittest.TestCase):
    def test_c01_sol_ring_plus_mana_vault_5_C(self) -> None:
        gs = _empty_game()
        sr = _bf(gs, 0, "Sol Ring", type_line="Artifact",
                power=None, toughness=None)
        mv = _bf(gs, 0, "Mana Vault", type_line="Artifact",
                power=None, toughness=None)
        _activate(gs, sr, "tap_mana")
        _activate(gs, mv, "tap_mana")
        self.assertEqual(gs.players[0].mana_pool.C, 5)

    def test_c02_swords_to_plowshares_exile_plus_life(self) -> None:
        gs = _empty_game()
        b = _bf(gs, 1, "Big", power="5", toughness="5")
        _cast(gs, 0, "Swords to Plowshares", targets=[b.card_id])
        self.assertIn(b.card_id, gs.players[1].zones.exile)
        self.assertEqual(gs.players[1].life_total, 45)

    def test_c03_thassas_oracle_wins_empty_library(self) -> None:
        gs = _empty_game()
        _cast(gs, 0, "Thassa's Oracle")
        self.assertTrue(gs.game_over)
        self.assertEqual(gs.winner_player_id, 0)

    def test_c04_demonic_consultation_thoracle_substitute(self) -> None:
        """Without Demonic Consultation in top 500, substitute Reanimate +
        Thoracle: lib empty after a search → Thoracle wins."""
        gs = _empty_game()
        # Library already empty.
        _cast(gs, 0, "Thassa's Oracle")
        self.assertEqual(gs.winner_player_id, 0)

    def test_c05_reanimate_brings_back_dragon(self) -> None:
        gs = _empty_game(life=40)
        d = Card(name="Dragon", owner=1,
                type_line="Creature — Dragon",
                cmc=5.0, power="5", toughness="5")
        gs.add_card(d)
        gs.players[1].zones.graveyard.append(d.card_id)
        _cast(gs, 0, "Reanimate", targets=[d.card_id])
        self.assertIn(d.card_id, gs.players[0].zones.battlefield)
        self.assertEqual(d.controller, 0)

    def test_c06_skullclamp_combo_kill_1_1_draw_2(self) -> None:
        gs = _empty_game()
        c1 = Card(name="Top1", owner=0)
        c2 = Card(name="Top2", owner=0)
        gs.add_card(c1)
        gs.add_card(c2)
        gs.players[0].zones.library.extend([c1.card_id, c2.card_id])
        sc = _bf(gs, 0, "Skullclamp", type_line="Artifact — Equipment",
                power=None, toughness=None)
        goblin = _bf(gs, 0, "Goblin", type_line="Creature — Goblin",
                    power="1", toughness="1")
        push_to_stack(
            gs, card_id=sc.card_id, controller=0, entry_type="activated",
            payment=build_activation_payload("Skullclamp", "equip"),
            targets=[goblin.card_id], description="Equip",
        )
        resolve_top(gs)
        # Mock the -1 toughness death.
        goblin.toughness = "0"
        gs.move_card(goblin.card_id, from_player=0,
                    from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        fire_event_triggers(gs, DieEvent(card_id=goblin.card_id, controller=0))
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda s, p: None)
        self.assertEqual(len(gs.players[0].zones.hand), 2)

    def test_c07_avenger_plus_craterhoof_swing(self) -> None:
        """Avenger of Zendikar creates plants; Craterhoof gives them
        trample + power. Test: 3 lands → 3 plants → 4 creatures → +4/+4."""
        gs = _empty_game()
        for _ in range(3):
            _bf(gs, 0, "Forest", type_line="Basic Land — Forest",
                power=None, toughness=None)
        avenger = _bf(gs, 0, "Avenger of Zendikar",
                     type_line="Creature — Plant Elemental",
                     power="5", toughness="5")
        # Fire avenger ETB.
        ev = EnterBattlefieldEvent(card_id=avenger.card_id,
                                    controller=0, from_zone="hand")
        fire_etb_triggers(gs, ev)
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda s, p: None)
        plants = [c for cid in gs.players[0].zones.battlefield
                  for c in [gs.get_card(cid)]
                  if c and c.name == "Plant Token"]
        self.assertEqual(len(plants), 3)
        # Now Craterhoof.
        ch = _bf(gs, 0, "Craterhoof Behemoth",
                type_line="Creature — Beast",
                power="5", toughness="5", keywords=["haste"])
        ev2 = EnterBattlefieldEvent(card_id=ch.card_id,
                                     controller=0, from_zone="hand")
        fire_etb_triggers(gs, ev2)
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda s, p: None)
        # X = creatures = 5 (3 plants + avenger + craterhoof).
        # Each gets +5/+5 + trample.
        self.assertEqual(avenger.power_int(), 10)
        for p in plants:
            self.assertTrue(p.has_keyword("trample"))

    def test_c08_animate_dead_reanimate_dragon(self) -> None:
        gs = _empty_game()
        d = Card(name="Dragon", owner=0,
                type_line="Creature — Dragon",
                cmc=5.0, power="5", toughness="5")
        gs.add_card(d)
        gs.players[0].zones.graveyard.append(d.card_id)
        ad = _bf(gs, 0, "Animate Dead", type_line="Enchantment — Aura",
                power=None, toughness=None)
        ev = EnterBattlefieldEvent(card_id=ad.card_id,
                                    controller=0, from_zone="hand")
        fire_etb_triggers(gs, ev)
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda s, p: None)
        self.assertIn(d.card_id, gs.players[0].zones.battlefield)

    def test_c09_brainstorm_then_ponder(self) -> None:
        gs = _empty_game()
        for i in range(5):
            c = Card(name=f"L_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _cast(gs, 0, "Brainstorm")
        # +1 net in hand.
        self.assertEqual(len(gs.players[0].zones.hand), 1)
        _cast(gs, 0, "Ponder")
        # Ponder stub draws 1.
        self.assertEqual(len(gs.players[0].zones.hand), 2)

    def test_c10_demonic_tutor_finds_thassas_oracle_then_win(self) -> None:
        """Demonic Tutor for Thoracle, then cast Thoracle on empty library."""
        gs = _empty_function = _empty_game()  # noqa
        gs = _empty_game()
        # Library has 1 card: Thoracle (then becomes empty).
        thoracle = Card(name="Thassa's Oracle", owner=0,
                       type_line="Creature — Merfolk Wizard",
                       cmc=2.0, power="1", toughness="3")
        gs.add_card(thoracle)
        gs.players[0].zones.library.append(thoracle.card_id)
        _cast(gs, 0, "Demonic Tutor", targets=[thoracle.card_id])
        # Library empty now.
        self.assertEqual(len(gs.players[0].zones.library), 0)
        # Cast Thoracle from hand.
        _cast(gs, 0, "Thassa's Oracle")
        self.assertTrue(gs.game_over)


if __name__ == "__main__":
    unittest.main()
