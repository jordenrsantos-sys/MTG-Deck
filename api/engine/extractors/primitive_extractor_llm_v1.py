"""
primitive_extractor_llm_v1 — LLM-extractor supplement for the
Pillar C ontology v1 regex extractor (iter 5 Phase 3).

For cards where the regex extractor in `primitive_extractor_v2`
returns <2 tags (likely an ambiguous card — short oracle text,
unusual phrasing, modern wording that doesn't match the legacy regex
patterns), this module calls Claude with the card text + the ontology
spec and asks for additional primitive tags.

Cost: ~$0.001 per call at Sonnet 4.6 pricing (~3k input + ~200 output
tokens per card). Restricted to ambiguous cards (~10k of 110k corpus
expected) by the gating threshold, capping the LLM-extractor spend at
~$10 for a full corpus pass.

Public API:
  - `is_ambiguous(regex_tags) -> bool` — gating heuristic
  - `llm_supplement(card, ontology, llm_client=None) -> set[str]`
  - `LLM_EXTRACTOR_VERSION` — version string
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set


LLM_EXTRACTOR_VERSION = "primitive_extractor_llm_v1.0"

_AMBIGUOUS_THRESHOLD = 2  # cards with <2 regex tags get LLM supplement
_LLM_INPUT_BUDGET = 4000
_LLM_OUTPUT_BUDGET = 400


def is_ambiguous(regex_tags: Set[str]) -> bool:
    """Gating heuristic: re-route cards to LLM extractor when regex
    pass produced <2 tags. Vanilla creatures + lands without abilities
    naturally have 0 tags; those still go to the LLM but the LLM will
    return empty too (no false-positives expected)."""
    return len(regex_tags) < _AMBIGUOUS_THRESHOLD


def _build_system_prompt(ontology_summary: str) -> str:
    return (
        "You are tagging an MTG card with Pillar C ontology v1 "
        "primitive tags. The ontology has 7 dimensions: mana_valuation, "
        "card_velocity, interaction, tempo, combo_role, "
        "win_condition_role, rules_modifiers. Each tag is a short "
        "kebab-case id (e.g. `sac-outlet`, `mandatory-trigger`).\n\n"
        f"Ontology summary:\n{ontology_summary}\n\n"
        "Output VALID JSON ONLY:\n"
        "{\n"
        '  "tags": ["tag-id-1", "tag-id-2", ...]\n'
        "}\n"
        "Return ONLY tags from the ontology summary above. Return an "
        "empty array if no tags apply (vanilla creatures, lands "
        "without abilities, joke cards, etc.). Do NOT invent new tags."
    )


def _build_user_prompt(card: Dict[str, Any], existing_tags: Set[str]) -> str:
    return (
        f"Card name: {card.get('name', '?')}\n"
        f"Type line: {card.get('type_line', '')}\n"
        f"Mana cost: {card.get('mana_cost', '')}\n"
        f"Oracle text:\n{card.get('oracle_text', '')}\n\n"
        f"Existing regex-extracted tags: {sorted(existing_tags)}\n\n"
        "Return any additional primitive tags from the ontology that "
        "apply to this card. Be conservative — only return tags whose "
        "definition clearly matches."
    )


def _build_ontology_summary(ontology: Dict[str, Any]) -> str:
    """Build a compact one-line-per-tag summary of the ontology to fit
    in the LLM prompt budget."""
    lines: List[str] = []
    by_dim: Dict[str, List[str]] = {}
    for tag_id, tag in ontology.items():
        by_dim.setdefault(tag.dimension, []).append(
            f"  - {tag_id}: {tag.definition}"
        )
    for dim, dim_lines in sorted(by_dim.items()):
        lines.append(f"[{dim}]")
        lines.extend(dim_lines)
    return "\n".join(lines)


def llm_supplement(
    card: Dict[str, Any],
    ontology: Dict[str, Any],
    existing_tags: Optional[Set[str]] = None,
    llm_client: Optional[Any] = None,
) -> Set[str]:
    """Call Claude to suggest additional primitive tags for an ambiguous
    card. Returns the LLM-proposed set, FILTERED to tag IDs actually
    present in the ontology (no hallucinations land in the DB).

    On any error / non-JSON response / empty array, returns set().
    Caller merges with regex tags.
    """
    existing_tags = existing_tags or set()
    if llm_client is None:
        from api.engine.layers.agent_llm_client_v1 import get_default_client
        llm_client = get_default_client()
    if not llm_client.is_available():
        return set()

    ontology_summary = _build_ontology_summary(ontology)
    system = _build_system_prompt(ontology_summary)
    user = _build_user_prompt(card, existing_tags)

    result = llm_client.call_with_budget(
        system=system, user=user,
        max_input_tokens=_LLM_INPUT_BUDGET,
        max_output_tokens=_LLM_OUTPUT_BUDGET,
    )
    if not result.ok or not isinstance(result.parsed_json, dict):
        return set()
    proposed = result.parsed_json.get("tags")
    if not isinstance(proposed, list):
        return set()
    valid_tag_ids = set(ontology.keys())
    return {
        t.strip() for t in proposed
        if isinstance(t, str) and t.strip() in valid_tag_ids
    }
