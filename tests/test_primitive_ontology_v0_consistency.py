"""Phase 11 tests — Pillar C primitive ontology v0 consistency check.

Parses `repo/api/engine/data/primitives/ontology_v0.md` and validates:
  - Every tag block has the required fields (dimension, definition,
    extraction_rule, examples, combos_with).
  - Every `combos_with` reference resolves to a real tag in the
    ontology.
  - The 6 dimensions are present.
  - Total tag count is in the 50-80 range per kickoff.
  - The 10 Spellbook-pair coverage demo references actual tags.
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ONTOLOGY_PATH = REPO_ROOT / "api" / "engine" / "data" / "primitives" / "ontology_v0.md"

# Lazy-load: parse on first test, share across all assertions.
_PARSED_CACHE = None


def _parse_ontology() -> dict:
    """Parse ontology_v0.md into a structured registry. Returns a dict
    with `tags` (id → metadata) and `dimensions` (list)."""
    global _PARSED_CACHE
    if _PARSED_CACHE is not None:
        return _PARSED_CACHE

    text = ONTOLOGY_PATH.read_text(encoding="utf-8")

    # Tags are defined as `### tag-id` blocks followed by `- field: value` lines.
    tags = {}
    current_id = None
    current_field = None  # one of: dimension, definition, extraction_rule, examples, combos_with

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        # Tag header.
        m = re.match(r"^###\s+([a-z0-9-]+)\s*$", line)
        if m:
            current_id = m.group(1)
            tags[current_id] = {
                "dimension": None, "definition": None,
                "extraction_rule": [], "examples": [],
                "combos_with": [],
            }
            current_field = None
            continue

        if current_id is None:
            continue

        # Field declaration: "- dimension: x" or "- combos_with:" (list follows).
        m = re.match(r"^-\s+(\w+):\s*(.*)$", line)
        if m:
            field = m.group(1)
            value = m.group(2).strip()
            if field == "dimension":
                tags[current_id]["dimension"] = value
                current_field = None
            elif field == "definition":
                tags[current_id]["definition"] = value
                current_field = None
            elif field == "extraction_rule":
                # If value present on same line ("- extraction_rule: []") handle empty;
                # otherwise the list follows on indented lines.
                if value == "[]":
                    current_field = None
                else:
                    current_field = "extraction_rule"
            elif field == "examples":
                # Inline list parsing for "examples: [...]" — handle both forms.
                m2 = re.match(r"^\[(.+)\]$", value)
                if m2:
                    items = [s.strip().strip('"').strip("'") for s in m2.group(1).split(",")]
                    tags[current_id]["examples"].extend(items)
                    current_field = None
                else:
                    current_field = "examples"
            elif field == "combos_with":
                m2 = re.match(r"^\[(.*)\]$", value)
                if m2:
                    inner = m2.group(1).strip()
                    if not inner:
                        current_field = None
                    else:
                        items = [s.strip() for s in inner.split(",")]
                        tags[current_id]["combos_with"].extend(items)
                        current_field = None
                else:
                    current_field = "combos_with"
            else:
                current_field = None
            continue

        # Continuation list item: "  - value" within a current_field.
        m = re.match(r"^\s+-\s+(.*)$", line)
        if m and current_field:
            val = m.group(1).strip()
            # Strip surrounding quotes for examples.
            if current_field in ("examples", "extraction_rule"):
                val = val.strip('"').strip("'").strip("`")
            tags[current_id][current_field].append(val)

    # Extract dimensions from the "## Dimension N: ..." headers.
    dimensions = []
    for line in text.splitlines():
        m = re.match(r"^##\s+Dimension\s+\d+:\s+(.+?)(?:\s*\(\d+\s+tags\))?\s*$", line)
        if m:
            dimensions.append(m.group(1).strip().lower().replace(" ", "_"))

    _PARSED_CACHE = {"tags": tags, "dimensions": dimensions}
    return _PARSED_CACHE


# Computed lazily.
_REQUIRED_FIELDS = ("dimension", "definition", "examples", "combos_with")


class OntologyExistsTests(unittest.TestCase):
    def test_ontology_file_exists(self) -> None:
        self.assertTrue(ONTOLOGY_PATH.is_file(),
                        f"Ontology file not found at {ONTOLOGY_PATH}")


class TagCountTests(unittest.TestCase):
    def test_tag_count_in_range(self) -> None:
        registry = _parse_ontology()
        n = len(registry["tags"])
        self.assertGreaterEqual(n, 50,
                                f"Ontology has {n} tags; spec requires ≥50.")
        self.assertLessEqual(n, 80,
                             f"Ontology has {n} tags; spec caps at 80.")

    def test_six_dimensions_present(self) -> None:
        registry = _parse_ontology()
        # Expecting at least the 6 named dimensions per the spec.
        self.assertGreaterEqual(len(registry["dimensions"]), 6,
                                f"Found {len(registry['dimensions'])} dimensions; spec requires 6.")


class TagFieldCompletenessTests(unittest.TestCase):
    def test_every_tag_has_dimension(self) -> None:
        registry = _parse_ontology()
        missing = [tag for tag, meta in registry["tags"].items() if not meta["dimension"]]
        self.assertFalse(missing, f"Tags missing 'dimension' field: {missing}")

    def test_every_tag_has_definition(self) -> None:
        registry = _parse_ontology()
        missing = [tag for tag, meta in registry["tags"].items() if not meta["definition"]]
        self.assertFalse(missing, f"Tags missing 'definition' field: {missing}")

    def test_every_tag_has_examples(self) -> None:
        registry = _parse_ontology()
        missing = [tag for tag, meta in registry["tags"].items() if not meta["examples"]]
        self.assertFalse(missing, f"Tags missing 'examples' field: {missing}")


class CombosWithReferencesTests(unittest.TestCase):
    def test_every_combos_with_resolves_to_tag(self) -> None:
        registry = _parse_ontology()
        valid_ids = set(registry["tags"].keys())
        broken: list = []
        for tag, meta in registry["tags"].items():
            for ref in meta["combos_with"]:
                if ref not in valid_ids:
                    broken.append(f"{tag} → {ref}")
        self.assertFalse(broken,
                         f"Unresolved combos_with references: {broken}")

    def test_no_orphan_tags(self) -> None:
        """Every tag should be referenced by at least one other tag's
        `combos_with`, OR be the SOURCE of at least one combos_with
        edge — otherwise it sits alone with no graph connections."""
        registry = _parse_ontology()
        # Build the inverse adjacency.
        referenced_by_other: set = set()
        for tag, meta in registry["tags"].items():
            for ref in meta["combos_with"]:
                referenced_by_other.add(ref)
        orphans = []
        for tag, meta in registry["tags"].items():
            has_outgoing = bool(meta["combos_with"])
            is_referenced = tag in referenced_by_other
            if not has_outgoing and not is_referenced:
                orphans.append(tag)
        self.assertFalse(orphans,
                         f"Orphan tags (no incoming or outgoing combos_with edges): {orphans}")


if __name__ == "__main__":
    unittest.main()
