"""Combat phase machinery per CR 506-511.

Phase 6 of mega-task v9.

Combat substep flow:
  beginning_of_combat
    → declare_attackers
    → declare_blockers
    → first_strike_damage   (only fires if any attacker/blocker has
                              first strike or double strike)
    → combat_damage         (normal pass; double strikers deal again)
    → end_of_combat

Attack declarations:
- Active player chooses creatures to attack. Must be untapped, not
  summoning sick (unless haste), not affected by "can't attack".
- Each attacker targets a player or planeswalker.
- Attack costs (Propaganda, Ghostly Prison) paid by attacking player —
  iter 10 stub: no costs.

Block declarations:
- Defending player(s) assign blockers per attacker. Multi-block legal.
- Active player declares damage assignment order on attacking creatures
  with multi-blockers (iter 10: assignment order = list order in
  block_assignments).

Combat damage:
- Two passes: first_strike pass (only first-strike + double-strike
  creatures), then normal pass (double-strike + non-first-strike).
- Damage assignment respects assignment order.
- Trample carries excess to defending player/planeswalker.
- Lifelink heals controller.
- Deathtouch: any damage from deathtouch is lethal (handled at SBA
  cascade, not here).
- Indestructible: damage doesn't kill (SBA in Phase 4 already handles).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from api.engine.pillar_f.v0_2.state import (
    Card, GameState,
)
from api.engine.pillar_f.v0_2.layers import (
    apply_continuous_effects, Characteristics,
)
from api.engine.pillar_f.v0_2.replacement import (
    DamageEvent, apply_replacements, run_sba_loop,
)


COMBAT_VERSION = "pillar_f_v0_2_combat_v1"


# ============================================================
# Combat state dataclasses
# ============================================================


@dataclass
class AttackerDeclaration:
    """A creature attacking. target is either a player_id (int) or a
    planeswalker card_id (str)."""
    attacker_card_id: str
    target: Any  # int (player_id) or str (planeswalker card_id)


@dataclass
class BlockerAssignment:
    """For each attacker, the ordered list of blocker card_ids assigned
    to it. Order matters for damage assignment (active player chooses)."""
    attacker_card_id: str
    blocker_card_ids: List[str] = field(default_factory=list)


@dataclass
class CombatState:
    """Per-combat-phase tracking. attached to the engine for the
    duration of one combat phase."""
    attackers: List[AttackerDeclaration] = field(default_factory=list)
    blocker_assignments: List[BlockerAssignment] = field(default_factory=list)
    damage_assignment_order: Dict[str, List[str]] = field(default_factory=dict)
    # ^ attacker_card_id → ordered list of blocker card_ids for damage assignment

    def blockers_for(self, attacker_card_id: str) -> List[str]:
        for ba in self.blocker_assignments:
            if ba.attacker_card_id == attacker_card_id:
                return ba.blocker_card_ids
        return []


# ============================================================
# Declare attackers + blockers
# ============================================================


def can_attack(card: Card, characteristics: Characteristics) -> bool:
    """Returns True if `card` is eligible to attack. Iter-10 minimal:
    must be untapped, not summoning-sick (unless haste), creature type.
    Defender keyword check supported."""
    if card.tapped:
        return False
    if not characteristics or "Creature" not in characteristics.types:
        return False
    if characteristics.has_keyword("defender"):
        return False
    if card.summoning_sick and not characteristics.has_keyword("haste"):
        return False
    return True


def declare_attackers(
    state: GameState,
    attackers: List[AttackerDeclaration],
    *,
    combat_state: Optional[CombatState] = None,
) -> CombatState:
    """Declare attackers per CR 508. Taps each attacker (unless vigilance).
    Returns the CombatState with attackers recorded.

    Validates each attacker is legal; raises ValueError on first illegal
    attacker (iter 10 strict; iter 11+ may downgrade to warnings)."""
    if combat_state is None:
        combat_state = CombatState()
    chars_table = apply_continuous_effects(state)
    for decl in attackers:
        card = state.get_card(decl.attacker_card_id)
        if card is None:
            raise ValueError(f"attacker {decl.attacker_card_id!r} not in cards_by_id")
        chars = chars_table.get(decl.attacker_card_id)
        if not can_attack(card, chars):
            raise ValueError(
                f"creature {card.name!r} cannot attack "
                f"(tapped={card.tapped}, summoning_sick={card.summoning_sick}, "
                f"keywords={chars.keywords if chars else []})"
            )
        # Tap unless vigilance.
        if not chars.has_keyword("vigilance"):
            card.tapped = True
        # Validate target.
        target = decl.target
        if isinstance(target, int):
            if not (0 <= target < len(state.players)):
                raise ValueError(f"attack target player_id {target} out of range")
        # Planeswalker target validation skipped iter 10 (just trust).
        combat_state.attackers.append(decl)
    return combat_state


def declare_blockers(
    state: GameState,
    block_assignments: List[BlockerAssignment],
    *,
    combat_state: CombatState,
) -> CombatState:
    """Declare blockers per CR 509. Each blocker must be untapped + a
    creature. Multi-block legal. Records assignments + initializes
    damage_assignment_order = blocker_card_ids per attacker (active
    player chooses; iter-10 default is block-declaration order)."""
    chars_table = apply_continuous_effects(state)
    for ba in block_assignments:
        for blocker_id in ba.blocker_card_ids:
            blocker_card = state.get_card(blocker_id)
            if blocker_card is None or blocker_card.tapped:
                raise ValueError(f"blocker {blocker_id!r} invalid or tapped")
            chars = chars_table.get(blocker_id)
            if not chars or "Creature" not in chars.types:
                raise ValueError(f"blocker {blocker_id!r} not a creature")
        combat_state.blocker_assignments.append(ba)
        # Iter 10: damage_assignment_order defaults to blocker order.
        combat_state.damage_assignment_order[ba.attacker_card_id] = list(
            ba.blocker_card_ids
        )
    return combat_state


# ============================================================
# Combat damage assignment + dealing
# ============================================================


def _is_first_strike_phase_card(chars: Characteristics) -> bool:
    """First-strike-phase participants: anyone with first strike OR
    double strike."""
    return chars.has_keyword("first strike") or chars.has_keyword("double strike")


def _is_normal_phase_card(chars: Characteristics) -> bool:
    """Normal-phase participants: anyone WITHOUT first strike (but WITH
    double strike). i.e., not-first-strike OR double strike."""
    return (not chars.has_keyword("first strike")) or chars.has_keyword("double strike")


def deal_combat_damage(
    state: GameState,
    combat_state: CombatState,
    *,
    is_first_strike: bool = False,
) -> List[Dict[str, Any]]:
    """Compute + assign combat damage for one pass (first-strike OR
    normal). Returns a list of action records {"attacker", "type",
    "target", "amount"} for diagnostics.

    For each attacker (filtered by participating in this pass):
      - If blocked: damage is assigned to blockers in order. Each
        blocker takes (toughness - already_marked) before next gets
        anything (lethal damage rule). Trample: excess to player.
      - If unblocked: all damage to original target (player or pw).

    Damage is routed through DamageEvent → apply_replacements, then
    applied as marked damage (for creatures) or life loss (for players).
    """
    chars_table = apply_continuous_effects(state)
    actions: List[Dict[str, Any]] = []

    for decl in combat_state.attackers:
        attacker = state.get_card(decl.attacker_card_id)
        if attacker is None:
            continue
        atk_chars = chars_table.get(decl.attacker_card_id)
        if atk_chars is None:
            continue
        # Determine whether attacker participates in this phase.
        # NOTE: even if the attacker doesn't deal damage this phase,
        # its blockers may still deal damage TO it (e.g., first-strike-
        # only attacker dealing damage in FS pass, then taking blocker
        # damage in normal pass when blocker has no FS). Blocker damage
        # is computed in a separate loop below the attacker loop.
        attacker_fires = (
            (is_first_strike and _is_first_strike_phase_card(atk_chars))
            or (not is_first_strike and _is_normal_phase_card(atk_chars))
        )
        damage = atk_chars.power if attacker_fires else 0
        blockers = combat_state.damage_assignment_order.get(decl.attacker_card_id, [])

        if not attacker_fires:
            # Skip the attacker damage section but still need to process
            # blocker damage TO this attacker (handled in the inner
            # blocker loop below).
            pass

        if not blockers and not attacker_fires:
            # No attacker damage + no blockers = no actions for this decl.
            continue

        if not blockers:
            # Unblocked → all damage to target.
            evt = DamageEvent(
                source_card_id=decl.attacker_card_id,
                source_controller=attacker.controller,
                target_kind="planeswalker" if isinstance(decl.target, str) else "player",
                target_id=decl.target,
                amount=damage,
                is_combat=True,
                is_first_strike=is_first_strike,
            )
            apply_replacements(state, evt)
            if not evt.prevent and evt.amount > 0:
                if isinstance(evt.target_id, int) and 0 <= evt.target_id < len(state.players):
                    target_player = state.players[evt.target_id]
                    target_player.life_total -= evt.amount
                    # Commander damage tracking.
                    if state.commander_card_ids.get(attacker.controller) == attacker.card_id:
                        # Attacker IS the commander.
                        oid = attacker.oracle_id or attacker.card_id
                        target_player.commander_damage_taken_from[oid] = (
                            target_player.commander_damage_taken_from.get(oid, 0)
                            + evt.amount
                        )
                # Lifelink: attacker's controller gains the damage as life.
                if atk_chars.has_keyword("lifelink"):
                    state.players[attacker.controller].life_total += evt.amount
            actions.append({
                "type": "unblocked_damage", "attacker": decl.attacker_card_id,
                "target": decl.target, "amount": evt.amount,
            })
        else:
            # Blocked. Per CR 510.1c + 702.2b:
            #   - SINGLE blocker: attacker assigns ALL its damage to that
            #     blocker (excess wasted unless trample).
            #   - MULTI blockers: active player declares an order; lethal
            #     damage must be assigned to each before any moves to the
            #     next. Deathtouch makes 1 damage lethal per blocker.
            remaining = damage
            is_single_block = len(blockers) == 1
            has_trample = atk_chars.has_keyword("trample")
            for idx, blocker_id in enumerate(blockers):
                if remaining <= 0:
                    break
                blocker = state.get_card(blocker_id)
                if blocker is None:
                    continue
                blocker_chars = chars_table.get(blocker_id)
                if blocker_chars is None:
                    continue
                is_last = (idx == len(blockers) - 1)
                if has_trample:
                    # Trample: assign lethal first to each blocker, then
                    # carry excess to defending player/planeswalker
                    # (handled in the trample block below).
                    lethal = max(1, blocker_chars.toughness - blocker.damage_marked)
                    if atk_chars.has_keyword("deathtouch"):
                        lethal = 1
                    assign = min(lethal, remaining)
                elif is_single_block:
                    # No trample, single block → dump all (excess wasted).
                    assign = remaining
                elif is_last:
                    # Last in multi-block → take all remaining damage.
                    assign = remaining
                else:
                    # Multi-block, not last: must assign lethal first.
                    lethal = max(1, blocker_chars.toughness - blocker.damage_marked)
                    if atk_chars.has_keyword("deathtouch"):
                        lethal = 1
                    assign = min(lethal, remaining)
                evt = DamageEvent(
                    source_card_id=decl.attacker_card_id,
                    source_controller=attacker.controller,
                    target_kind="creature",
                    target_id=blocker_id,
                    amount=assign,
                    is_combat=True,
                    is_first_strike=is_first_strike,
                )
                apply_replacements(state, evt)
                if not evt.prevent and evt.amount > 0:
                    blocker.damage_marked += evt.amount
                    # Deathtouch SBA approximation: any damage from a
                    # deathtouch source makes the target dead this SBA
                    # pass — force damage_marked >= toughness so SBA picks
                    # it up. (CR 702.2b: SBA flag tracked per-creature;
                    # iter-10 simplification.)
                    if atk_chars.has_keyword("deathtouch"):
                        blocker.damage_marked = max(
                            blocker.damage_marked, blocker_chars.toughness,
                        )
                    if atk_chars.has_keyword("lifelink"):
                        state.players[attacker.controller].life_total += evt.amount
                actions.append({
                    "type": "damage_to_blocker", "attacker": decl.attacker_card_id,
                    "blocker": blocker_id, "amount": evt.amount,
                })
                # For non-trample single block, all damage went to this
                # blocker → remaining=0. For trample or multi-block,
                # subtract assigned amount so trample excess flows through.
                if is_single_block and not has_trample:
                    remaining = 0
                else:
                    remaining -= assign
            # Trample: excess to original target.
            if remaining > 0 and atk_chars.has_keyword("trample"):
                evt = DamageEvent(
                    source_card_id=decl.attacker_card_id,
                    source_controller=attacker.controller,
                    target_kind="planeswalker" if isinstance(decl.target, str) else "player",
                    target_id=decl.target,
                    amount=remaining,
                    is_combat=True,
                    is_first_strike=is_first_strike,
                )
                apply_replacements(state, evt)
                if not evt.prevent and evt.amount > 0:
                    if isinstance(evt.target_id, int) and 0 <= evt.target_id < len(state.players):
                        target_player = state.players[evt.target_id]
                        target_player.life_total -= evt.amount
                        if state.commander_card_ids.get(attacker.controller) == attacker.card_id:
                            oid = attacker.oracle_id or attacker.card_id
                            target_player.commander_damage_taken_from[oid] = (
                                target_player.commander_damage_taken_from.get(oid, 0)
                                + evt.amount
                            )
                    if atk_chars.has_keyword("lifelink"):
                        state.players[attacker.controller].life_total += evt.amount
                actions.append({
                    "type": "trample_excess",
                    "attacker": decl.attacker_card_id,
                    "target": decl.target,
                    "amount": evt.amount,
                })
        # Blockers deal damage back to attacker (only blockers that
        # participate in this phase per first-strike rules).
        for blocker_id in blockers:
            blocker = state.get_card(blocker_id)
            if blocker is None:
                continue
            blocker_chars = chars_table.get(blocker_id)
            if blocker_chars is None:
                continue
            if is_first_strike and not _is_first_strike_phase_card(blocker_chars):
                continue
            if not is_first_strike and not _is_normal_phase_card(blocker_chars):
                continue
            b_dmg = blocker_chars.power
            if b_dmg <= 0:
                continue
            evt = DamageEvent(
                source_card_id=blocker_id,
                source_controller=blocker.controller,
                target_kind="creature",
                target_id=decl.attacker_card_id,
                amount=b_dmg,
                is_combat=True,
                is_first_strike=is_first_strike,
            )
            apply_replacements(state, evt)
            if not evt.prevent and evt.amount > 0:
                attacker.damage_marked += evt.amount
                if blocker_chars.has_keyword("lifelink"):
                    state.players[blocker.controller].life_total += evt.amount
            actions.append({
                "type": "blocker_damage_to_attacker",
                "blocker": blocker_id,
                "attacker": decl.attacker_card_id,
                "amount": evt.amount,
            })
    # State-based actions cascade after damage assignment (CR 510.4).
    run_sba_loop(state)
    return actions


def first_strike_phase_active(combat_state: CombatState,
                              state: GameState) -> bool:
    """Returns True iff any attacker or blocker has first or double
    strike — if so, the first_strike_damage substep happens before
    the normal combat_damage substep (CR 510.1)."""
    chars_table = apply_continuous_effects(state)
    for decl in combat_state.attackers:
        chars = chars_table.get(decl.attacker_card_id)
        if chars and _is_first_strike_phase_card(chars):
            return True
    for ba in combat_state.blocker_assignments:
        for bid in ba.blocker_card_ids:
            chars = chars_table.get(bid)
            if chars and _is_first_strike_phase_card(chars):
                return True
    return False


def run_combat_phase(
    state: GameState,
    attackers: List[AttackerDeclaration],
    block_assignments: List[BlockerAssignment],
) -> Tuple[CombatState, List[Dict[str, Any]]]:
    """End-to-end combat phase: declare attackers → declare blockers →
    first strike (if applicable) → normal damage → return action log.

    SBA loop runs after each damage pass. Combat ends; caller advances
    to end_of_combat step.
    """
    cs = declare_attackers(state, attackers)
    declare_blockers(state, block_assignments, combat_state=cs)
    actions: List[Dict[str, Any]] = []
    if first_strike_phase_active(cs, state):
        actions.extend(deal_combat_damage(state, cs, is_first_strike=True))
    actions.extend(deal_combat_damage(state, cs, is_first_strike=False))
    return cs, actions
