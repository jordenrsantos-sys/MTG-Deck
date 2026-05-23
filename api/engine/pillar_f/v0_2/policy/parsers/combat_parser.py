"""Combat-phase response parsers + validators.

Parses the LLM's JSON output for attackers + blockers prompts.
Validates the chosen indices against the eligible lists. Errors
return (parsed=None, error_message) for re-prompt.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from api.engine.pillar_f.v0_2.policy.parsers.action_parser import (
    _extract_first_json_object,
)


COMBAT_PARSER_VERSION = "pillar_f_v0_2_policy_combat_parser_v1"


@dataclass
class AttackerDeclarationParsed:
    """One LLM-chosen attacker → target mapping."""
    attacker_index: int
    target_index: int


@dataclass
class AttackersResponse:
    """Parsed attackers prompt response."""
    attackers: List[AttackerDeclarationParsed] = field(default_factory=list)
    rationale: str = ""


@dataclass
class BlockAssignmentParsed:
    """One LLM-chosen attacker → blockers mapping."""
    attacker_index: int
    blocker_indices: List[int] = field(default_factory=list)


@dataclass
class BlockersResponse:
    """Parsed blockers prompt response."""
    blocks: List[BlockAssignmentParsed] = field(default_factory=list)
    rationale: str = ""


def parse_attackers_response(
    raw_text: str,
    eligible_attackers: List[Dict[str, Any]],
    attack_targets: List[Dict[str, Any]],
) -> Tuple[Optional[AttackersResponse], Optional[str]]:
    """Parse the LLM's attackers JSON. Validates each entry's
    attacker_index + target_index are in-range and unique attackers."""
    if not raw_text or not raw_text.strip():
        return None, "Empty LLM response for attackers."
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```\s*$", "", text)
    obj_str = _extract_first_json_object(text)
    if obj_str is None:
        return None, f"No JSON object found in: {text[:200]!r}"
    try:
        parsed = json.loads(obj_str)
    except json.JSONDecodeError as exc:
        return None, f"JSON parse error: {exc}"
    if not isinstance(parsed, dict):
        return None, f"Expected JSON object, got {type(parsed).__name__}"

    atk_list = parsed.get("attackers")
    if atk_list is None:
        # Missing key = no attackers (legal).
        atk_list = []
    if not isinstance(atk_list, list):
        return None, f"'attackers' must be a list, got {type(atk_list).__name__}"

    seen_indices: set = set()
    out: List[AttackerDeclarationParsed] = []
    for i, entry in enumerate(atk_list):
        if not isinstance(entry, dict):
            return None, f"attackers[{i}] is not a dict"
        aidx = entry.get("attacker_index")
        tidx = entry.get("target_index")
        if aidx is None or tidx is None:
            return None, (
                f"attackers[{i}] missing attacker_index or target_index "
                f"(got keys: {sorted(entry.keys())})"
            )
        try:
            aidx_int = int(aidx)
            tidx_int = int(tidx)
        except (TypeError, ValueError):
            return None, f"attackers[{i}] indices must be ints"
        if aidx_int < 0 or aidx_int >= len(eligible_attackers):
            return None, (
                f"attackers[{i}].attacker_index {aidx_int} out of range "
                f"(eligible: [0, {len(eligible_attackers) - 1}])"
            )
        if tidx_int < 0 or tidx_int >= len(attack_targets):
            return None, (
                f"attackers[{i}].target_index {tidx_int} out of range "
                f"(targets: [0, {len(attack_targets) - 1}])"
            )
        if aidx_int in seen_indices:
            return None, f"attackers[{i}].attacker_index {aidx_int} duplicate"
        seen_indices.add(aidx_int)
        out.append(AttackerDeclarationParsed(
            attacker_index=aidx_int, target_index=tidx_int,
        ))
    rationale = str(parsed.get("rationale") or "")[:500]
    return AttackersResponse(attackers=out, rationale=rationale), None


def parse_blockers_response(
    raw_text: str,
    eligible_blockers: List[Dict[str, Any]],
    attackers_to_block: List[Dict[str, Any]],
) -> Tuple[Optional[BlockersResponse], Optional[str]]:
    """Parse the LLM's blockers JSON."""
    if not raw_text or not raw_text.strip():
        return None, "Empty LLM response for blockers."
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```\s*$", "", text)
    obj_str = _extract_first_json_object(text)
    if obj_str is None:
        return None, f"No JSON object found in: {text[:200]!r}"
    try:
        parsed = json.loads(obj_str)
    except json.JSONDecodeError as exc:
        return None, f"JSON parse error: {exc}"
    if not isinstance(parsed, dict):
        return None, f"Expected JSON object, got {type(parsed).__name__}"

    blocks_list = parsed.get("blocks")
    if blocks_list is None:
        blocks_list = []
    if not isinstance(blocks_list, list):
        return None, f"'blocks' must be a list, got {type(blocks_list).__name__}"

    seen_blocker_indices: set = set()
    seen_attacker_indices: set = set()
    out: List[BlockAssignmentParsed] = []
    for i, entry in enumerate(blocks_list):
        if not isinstance(entry, dict):
            return None, f"blocks[{i}] is not a dict"
        aidx = entry.get("attacker_index")
        b_idx_list = entry.get("blocker_indices")
        if aidx is None or b_idx_list is None:
            return None, (
                f"blocks[{i}] missing attacker_index or blocker_indices "
                f"(got keys: {sorted(entry.keys())})"
            )
        try:
            aidx_int = int(aidx)
        except (TypeError, ValueError):
            return None, f"blocks[{i}].attacker_index must be int"
        if aidx_int < 0 or aidx_int >= len(attackers_to_block):
            return None, (
                f"blocks[{i}].attacker_index {aidx_int} out of range"
            )
        if aidx_int in seen_attacker_indices:
            return None, (
                f"blocks[{i}].attacker_index {aidx_int} duplicate "
                f"(each attacker may only appear once in blocks)"
            )
        seen_attacker_indices.add(aidx_int)
        if not isinstance(b_idx_list, list):
            return None, f"blocks[{i}].blocker_indices must be a list"
        b_indices: List[int] = []
        for j, bidx in enumerate(b_idx_list):
            try:
                bidx_int = int(bidx)
            except (TypeError, ValueError):
                return None, f"blocks[{i}].blocker_indices[{j}] must be int"
            if bidx_int < 0 or bidx_int >= len(eligible_blockers):
                return None, (
                    f"blocks[{i}].blocker_indices[{j}] = {bidx_int} "
                    f"out of range"
                )
            if bidx_int in seen_blocker_indices:
                return None, (
                    f"blocker_index {bidx_int} appears in multiple "
                    f"block assignments (each blocker may only block "
                    f"one attacker)"
                )
            seen_blocker_indices.add(bidx_int)
            b_indices.append(bidx_int)
        out.append(BlockAssignmentParsed(
            attacker_index=aidx_int, blocker_indices=b_indices,
        ))
    rationale = str(parsed.get("rationale") or "")[:500]
    return BlockersResponse(blocks=out, rationale=rationale), None
