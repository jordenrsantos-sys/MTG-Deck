"""
new_combo_discovery_v1 — Mega-task v3 Phase 4.

Discovers candidate new combo pairs by traversing the Pillar C
primitive interaction graph: for each new card, find existing cards
whose primitive tags form a known `combos_with` edge with the new
card's primitives.

Discovered pairs are written to `combo_brackets_v1_set_appended.json`
(additive — never modifies the base registry `combo_brackets_v1.json`
per the v3 kickoff rule).

Public API:
  - discover_new_combo_pairs(new_cards, existing_cards=None,
                              db_path=None, snapshot_id=None) -> list[dict]
  - append_discovered_pairs(pairs, path=None) -> int

Confidence scoring:
  - 1.0 — primitive A on new card ↔ primitive B on partner card where
          B is in A's `combos_with` cross-references (and vice versa).
  - 0.7 — A and B share a primitive cluster (one of the 20 canonical
          interaction-graph pairs).
  - 0.5 — single-primitive overlap (catch-all weak signal).
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


NEW_COMBO_DISCOVERY_VERSION = "new_combo_discovery_v1.0"

APPENDED_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1]
    / "data" / "combos" / "combo_brackets_v1_set_appended.json"
)

# Canonical interaction-graph pairs from `ontology_v0.md` section
# "Interaction graph (20 canonical primitive pairs)". Bidirectional —
# (a, b) IS (b, a). We keep them as a set of frozensets so order
# doesn't matter at lookup time.
_CANONICAL_PAIRS: List[frozenset] = [
    frozenset(["sac-outlet", "persist-creature"]),
    frozenset(["sac-outlet", "death-trigger"]),
    frozenset(["etb-trigger", "flicker-effect"]),
    frozenset(["infinite-mana-source", "infinite-untap-source"]),
    frozenset(["infinite-mana-source", "x-spell-payoff"]),
    frozenset(["infinite-mana-source", "combat-extra-step"]),
    frozenset(["token-producer", "doubler-effect"]),
    frozenset(["token-producer", "anthem-effect"]),
    frozenset(["token-producer", "sac-outlet"]),
    frozenset(["recursion-graveyard", "self-mill"]),
    frozenset(["recursion-graveyard", "sac-outlet"]),
    frozenset(["tutor-broad", "combo-assembly"]),
    frozenset(["tutor-creature", "persist-creature"]),
    frozenset(["cantrip", "storm-payoff"]),
    frozenset(["free-spell", "storm-payoff"]),
    frozenset(["extra-turn", "extra-combat"]),
    frozenset(["lifegain-payoff", "life-loss-trigger"]),
    frozenset(["landfall-trigger", "extra-land-drop"]),
    frozenset(["counterspell-hard", "combo-protection"]),
    frozenset(["attack-trigger", "evasion-grant"]),
]


@dataclass
class DiscoveredPair:
    new_card: str
    paired_with: str
    combo_pattern: str       # the matched ontology edge or shared-primitive
    confidence: float        # 0.5 / 0.7 / 1.0
    via_primitives: Tuple[str, str]   # (new_card's tag, partner's tag)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["via_primitives"] = list(d["via_primitives"])
        return d


# ============================================================
# Ontology loader (lazy + cached) for combos_with edges.
# ============================================================


_ONTOLOGY_CACHE: Dict[str, Any] = {}


def _ontology_combos_with() -> Dict[str, List[str]]:
    """Return `tag_id -> [partner_tag_id, ...]` from the Pillar C
    ontology's `combos_with` cross-references. Cached after first load.
    """
    if "combos_with" in _ONTOLOGY_CACHE:
        return _ONTOLOGY_CACHE["combos_with"]
    from api.engine.extractors.primitive_extractor_v1 import load_ontology
    ontology = load_ontology()
    out: Dict[str, List[str]] = {}
    for tag_id, tag in ontology.items():
        out[tag_id] = list(tag.combos_with)
    _ONTOLOGY_CACHE["combos_with"] = out
    return out


# ============================================================
# Existing-cards lookup (DB-backed).
# ============================================================


def _load_existing_cards_with_primitives(
    db_path: Path, snapshot_id: str,
    exclude_names: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Load every card in the snapshot that has at least one v1
    primitive tag. Returns `[{name, primitives}]`.
    """
    exclude_names = {n.strip().lower() for n in (exclude_names or set())}
    con = sqlite3.connect(str(db_path))
    try:
        rows = con.execute(
            "SELECT name, primitives_v1_json FROM cards "
            "WHERE snapshot_id=? AND primitives_v1_json IS NOT NULL "
            "AND primitives_v1_json != '[]' AND primitives_v1_json != ''",
            (snapshot_id,),
        ).fetchall()
    finally:
        con.close()
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for name, raw in rows:
        if not name:
            continue
        key = name.strip().lower()
        if key in exclude_names or key in seen:
            continue
        seen.add(key)
        try:
            tags = json.loads(raw)
            if isinstance(tags, list):
                out.append({"name": name, "primitives": [t for t in tags if isinstance(t, str)]})
        except json.JSONDecodeError:
            continue
    return out


# ============================================================
# Discovery.
# ============================================================


def _confidence_for_pair(
    a_tag: str, b_tag: str,
    combos_with: Dict[str, List[str]],
) -> Optional[Tuple[float, str]]:
    """Return (confidence, combo_pattern) or None if not a meaningful pair."""
    # Tier 1: ontology-declared cross-reference.
    if b_tag in combos_with.get(a_tag, []):
        return 1.0, f"ontology_edge:{a_tag}<->{b_tag}"
    if a_tag in combos_with.get(b_tag, []):
        return 1.0, f"ontology_edge:{b_tag}<->{a_tag}"
    # Tier 2: canonical interaction-graph pair.
    pair = frozenset([a_tag, b_tag])
    if pair in _CANONICAL_PAIRS and a_tag != b_tag:
        return 0.7, f"canonical_pair:{'+'.join(sorted(pair))}"
    # Tier 3: same-tag overlap is weakly meaningful (e.g. two
    # sac-outlets in one deck isn't a combo). Skip same-tag self-match.
    return None


def discover_new_combo_pairs(
    new_cards: Sequence[Dict[str, Any]],
    existing_cards: Optional[Sequence[Dict[str, Any]]] = None,
    db_path: Optional[Path] = None,
    snapshot_id: Optional[str] = None,
    min_confidence: float = 0.5,
) -> List[DiscoveredPair]:
    """For each new card with non-empty primitives, find existing
    cards whose primitives form a known combo pattern.

    Args:
      new_cards: list of `{name, primitives}` dicts. The `primitives`
        field is expected to be the v1 (Pillar C ontology) tag list.
      existing_cards: optional pre-loaded list. If None, loaded from
        `cards.primitives_v1_json` at (db_path, snapshot_id).
      db_path / snapshot_id: only used if existing_cards is None.
      min_confidence: drop pairs below this score (default 0.5).

    Returns: list of DiscoveredPair sorted by descending confidence.
    """
    new_names = {(c.get("name") or "").strip().lower() for c in new_cards}
    if existing_cards is None:
        if db_path is None or snapshot_id is None:
            existing_cards = []
        else:
            existing_cards = _load_existing_cards_with_primitives(
                db_path, snapshot_id, exclude_names=new_names,
            )

    combos_with = _ontology_combos_with()

    pairs: List[DiscoveredPair] = []
    seen_pair_keys: Set[frozenset] = set()
    for n_card in new_cards:
        n_name = (n_card.get("name") or "").strip()
        n_prims = list(n_card.get("primitives") or [])
        if not n_name or not n_prims:
            continue
        for e_card in existing_cards:
            e_name = (e_card.get("name") or "").strip()
            e_prims = list(e_card.get("primitives") or [])
            if not e_name or e_name.lower() == n_name.lower():
                continue
            # Avoid duplicate (name-pair) records.
            key = frozenset([n_name.lower(), e_name.lower()])
            if key in seen_pair_keys:
                continue
            best: Optional[Tuple[float, str, Tuple[str, str]]] = None
            for nt in n_prims:
                for et in e_prims:
                    res = _confidence_for_pair(nt, et, combos_with)
                    if res is None:
                        continue
                    conf, pattern = res
                    if best is None or conf > best[0]:
                        best = (conf, pattern, (nt, et))
            if best is not None and best[0] >= min_confidence:
                pairs.append(DiscoveredPair(
                    new_card=n_name, paired_with=e_name,
                    combo_pattern=best[1], confidence=best[0],
                    via_primitives=best[2],
                ))
                seen_pair_keys.add(key)

    pairs.sort(key=lambda p: (-p.confidence, p.new_card, p.paired_with))
    return pairs


# ============================================================
# Appended-registry write.
# ============================================================


def append_discovered_pairs(
    pairs: Sequence[DiscoveredPair],
    path: Optional[Path] = None,
) -> int:
    """Append discovered pairs to `combo_brackets_v1_set_appended.json`.

    The file's shape is `{discovered: [...]}` so the base
    `combo_brackets_v1.json` (which is the canonical Spellbook registry)
    is never modified. Returns the count of pairs appended.
    """
    p = path or APPENDED_REGISTRY_PATH
    existing: Dict[str, Any] = {"version": NEW_COMBO_DISCOVERY_VERSION, "discovered": []}
    if p.is_file():
        try:
            existing = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(existing, dict):
                existing = {"version": NEW_COMBO_DISCOVERY_VERSION, "discovered": []}
            if "discovered" not in existing or not isinstance(existing["discovered"], list):
                existing["discovered"] = []
        except (json.JSONDecodeError, OSError):
            existing = {"version": NEW_COMBO_DISCOVERY_VERSION, "discovered": []}

    seen_keys = {
        (e.get("new_card", "").lower(), e.get("paired_with", "").lower())
        for e in existing["discovered"]
    }
    added = 0
    for pair in pairs:
        key = (pair.new_card.lower(), pair.paired_with.lower())
        if key in seen_keys:
            continue
        existing["discovered"].append(pair.to_dict())
        seen_keys.add(key)
        added += 1

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    return added
