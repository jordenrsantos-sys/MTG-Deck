"""
primitive_extractor_v1 — Pillar C primitive tag extractor (iter 4 Phase 5).

Reads `repo/api/engine/data/primitives/ontology_v0.md`, parses each
tag block (id / dimension / extraction_rule / examples / combos_with),
compiles the `extraction_rule` regex patterns, and applies them to a
card's `(oracle_text, type_line, mana_cost)` to produce the set of
ontology tag IDs the card matches.

Two tags have empty `extraction_rule` in the ontology — they are
populated from external signals:

  - `combo-assembly`: card name appears in `combo_brackets_v1.json`.
  - `combat-extra-step`: aliased to `extra-combat` (same patterns).

Public API:
  - `extract_primitives(oracle_text, type_line, mana_cost,
                        card_name=None, combo_assembly_set=None) -> set[str]`
  - `load_ontology() -> dict[str, ParsedTag]`
  - `load_combo_assembly_names() -> set[str]`
  - `ParsedTag` — dataclass with id, dimension, patterns (compiled),
    raw_patterns, examples, combos_with.

The extractor is pure-Python + regex; no LLM calls. Runs at ~10k cards
per second on a single core, so the 110k-card backfill completes in
~10-15 seconds end-to-end.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Pattern, Set, Tuple


PRIMITIVE_EXTRACTOR_VERSION = "primitive_extractor_v1.0"


ONTOLOGY_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "primitives" / "ontology_v0.md"
)
COMBO_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "combos" / "combo_brackets_v1.json"
)


@dataclass
class ParsedTag:
    id: str
    dimension: str
    definition: str
    patterns: List[Pattern[str]] = field(default_factory=list)
    raw_patterns: List[str] = field(default_factory=list)
    examples: List[str] = field(default_factory=list)
    combos_with: List[str] = field(default_factory=list)


# ============================================================
# Ontology parsing.
# ============================================================


# Match a tag block starting with "### tag-id" and lasting until the
# next "### " line or "## " section header.
_TAG_HEAD_RE = re.compile(
    r"^###\s+(?P<id>[a-z][a-z0-9\-]*)\s*$",
    re.MULTILINE,
)

_DIMENSION_RE = re.compile(r"^-\s*dimension:\s*(?P<v>[\w_]+)\s*$", re.MULTILINE)
_DEFINITION_RE = re.compile(r"^-\s*definition:\s*(?P<v>.+)$", re.MULTILINE)


def _parse_extraction_rules(block: str) -> List[str]:
    """Extract the regex patterns from a tag block's extraction_rule
    list. Patterns are wrapped in backticks, one per bullet line under
    the `extraction_rule:` heading. Stops at the next top-level bullet
    (examples: / combos_with: / etc.)."""
    # Find the extraction_rule section.
    m = re.search(r"^-\s*extraction_rule:\s*$", block, re.MULTILINE)
    if not m:
        # Possibly inline: "- extraction_rule: []"
        if re.search(r"^-\s*extraction_rule:\s*\[\]\s*$", block, re.MULTILINE):
            return []
        return []
    start = m.end()
    # Read indented bullets until the next top-level bullet.
    patterns: List[str] = []
    for line in block[start:].splitlines():
        if not line.strip():
            continue
        # Top-level bullet (re-aligned with the extraction_rule level) ends
        # the section. We allow any depth of sub-bullet under it.
        if re.match(r"^-\s+\w+:", line):
            break
        # Pattern lines look like "  - `pattern`" or "  - `pattern`  # comment"
        # or "  - `pattern` (parenthetical note)". Accept anything after
        # the closing backtick — patterns are just the backtick contents.
        m2 = re.match(r"^\s+-\s+`(?P<p>[^`]*)`", line)
        if m2:
            patterns.append(m2.group("p"))
            continue
        # Unrecognized — skip (allow notes/comments).
    return patterns


def _parse_examples(block: str) -> List[str]:
    m = re.search(r"^-\s*examples:\s*(?P<v>.+)$", block, re.MULTILINE)
    if not m:
        return []
    raw = m.group("v").strip()
    # Examples are an inline JSON-array-like list: ["Name 1", "Name 2"].
    try:
        return [str(x) for x in json.loads(raw)]
    except json.JSONDecodeError:
        return []


def _parse_combos_with(block: str) -> List[str]:
    m = re.search(r"^-\s*combos_with:\s*(?P<v>.+)$", block, re.MULTILINE)
    if not m:
        return []
    raw = m.group("v").strip()
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        return [s.strip().strip('"').strip("'") for s in inner.split(",")]
    return []


def load_ontology(ontology_path: Optional[Path] = None) -> Dict[str, ParsedTag]:
    """Parse the ontology markdown into a dict of `tag_id -> ParsedTag`."""
    path = ontology_path or ONTOLOGY_PATH
    text = path.read_text(encoding="utf-8")
    # Find all tag headings.
    heads = list(_TAG_HEAD_RE.finditer(text))
    out: Dict[str, ParsedTag] = {}
    for i, h in enumerate(heads):
        tag_id = h.group("id")
        block_start = h.end()
        block_end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
        # Stop the block early if we hit a top-level "## " section
        # (e.g. "## Interaction graph" or "## Spellbook coverage check").
        section_m = re.search(r"^##\s+", text[block_start:block_end], re.MULTILINE)
        if section_m:
            block_end = block_start + section_m.start()
        block = text[block_start:block_end]

        dim_m = _DIMENSION_RE.search(block)
        def_m = _DEFINITION_RE.search(block)
        if not dim_m:
            continue   # malformed tag — skip
        dimension = dim_m.group("v").strip()
        definition = def_m.group("v").strip() if def_m else ""
        raw_patterns = _parse_extraction_rules(block)
        compiled: List[Pattern[str]] = []
        for p in raw_patterns:
            try:
                # IGNORECASE: card text matches case-insensitive.
                # MULTILINE: `$` and `^` match line boundaries so single-
                # line patterns like `counter target spell\.?\s*$` match
                # when the oracle text is composed of multiple lines.
                # We do NOT use DOTALL — patterns are line-bounded.
                compiled.append(re.compile(p, re.IGNORECASE | re.MULTILINE))
            except re.error:
                continue
        out[tag_id] = ParsedTag(
            id=tag_id,
            dimension=dimension,
            definition=definition,
            patterns=compiled,
            raw_patterns=raw_patterns,
            examples=_parse_examples(block),
            combos_with=_parse_combos_with(block),
        )
    return out


# ============================================================
# Combo-assembly set loading.
# ============================================================


def load_combo_assembly_names(
    registry_path: Optional[Path] = None,
) -> Set[str]:
    """Return the set of card names that appear in any combo variant
    in `combo_brackets_v1.json`. These are the cards primarily used to
    assemble combo lines — Pillar C tags them `combo-assembly` since
    that tag's extraction_rule is empty in the ontology.
    """
    path = registry_path or COMBO_REGISTRY_PATH
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()
    names: Set[str] = set()
    # The registry is a dict {variant_id -> variant} or {variants: [...]}
    # or a top-level list — handle all three shapes.
    if isinstance(data, list):
        variant_iter = iter(data)
    elif "by_variant_id" in data:
        variant_iter = iter(data["by_variant_id"].values())
    else:
        variant_iter = iter(data.get("variants") or [])
    for v in variant_iter:
        if not isinstance(v, dict):
            continue
        cards = v.get("card_names") or v.get("cards") or []
        for c in cards:
            if isinstance(c, str):
                names.add(c.strip().lower())
    return names


# ============================================================
# Extraction.
# ============================================================


def extract_primitives(
    oracle_text: str,
    type_line: str = "",
    mana_cost: str = "",
    card_name: Optional[str] = None,
    ontology: Optional[Dict[str, ParsedTag]] = None,
    combo_assembly_set: Optional[Set[str]] = None,
) -> Set[str]:
    """Apply the ontology's extraction_rule regex patterns to the card
    text and return the matching tag IDs.

    Args:
      oracle_text: card's printed oracle text (case-insensitive matching).
      type_line: card's printed type line.
      mana_cost: card's printed mana cost (e.g. "{2}{U}{U}").
      card_name: card's printed name (used for combo-assembly lookup).
      ontology: pre-loaded ParsedTag dict (loaded if None).
      combo_assembly_set: pre-loaded set of combo-assembly card names
        (loaded if None). Pass `set()` to skip combo-assembly tagging.

    Returns: set[str] of tag IDs.
    """
    if ontology is None:
        ontology = load_ontology()
    haystack = "\n".join([s for s in (oracle_text, type_line, mana_cost) if s])
    if not haystack:
        haystack = ""
    # Normalize modern mana-symbol notation so the ontology's plain-
    # English patterns ("tap.{0,20}add ...") still match cards whose
    # printed text uses "{T}: Add {C}{C}." style instead.
    haystack_norm = (
        haystack.replace("{T}", "tap")
        .replace("{t}", "tap")
    )

    matched: Set[str] = set()
    for tag_id, tag in ontology.items():
        for pat in tag.patterns:
            if pat.search(haystack_norm):
                matched.add(tag_id)
                break

    # combat-extra-step has no extraction_rule of its own — alias to
    # extra-combat (per ontology's note "same patterns as extra-combat").
    if "extra-combat" in matched:
        matched.add("combat-extra-step")

    # combo-assembly: tag if the card name appears in the combo registry.
    if card_name:
        if combo_assembly_set is None:
            combo_assembly_set = load_combo_assembly_names()
        if card_name.strip().lower() in combo_assembly_set:
            matched.add("combo-assembly")

    return matched
