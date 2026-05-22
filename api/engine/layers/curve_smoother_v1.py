"""
curve_smoother_v1 — Pillar E v0.3 (Mega-task v5 Phase 9).

Analyzes the deck's mana-cost distribution, compares to an archetype-
typical curve loaded from `curve_targets_by_archetype_v1.json`, and
returns a structured `CurveAnalysis` containing:

  - `archetype_target`: the target {cmc_slot: count} for the active
    archetype (e.g. "tribal" wants 5-of-CMC-2, 13-of-CMC-3, etc.).
  - `deck_curve`: the actual {cmc_slot: count} from the deck's nonland
    nonbasic cards.
  - `bricks`: cards above the archetype's CMC ceiling (e.g. a 9-CMC
    big-creature is a brick in a tribal-aggro deck whose ceiling is 6).
    Each brick is `{"card_name": str, "cmc": float, "ceiling": int}`.
  - `holes`: CMC slots significantly under target (slot count <=
    target * hole_pct, default 0.5). Each hole is `{"cmc": str,
    "actual": int, "target": int}`.
  - `significant`: True iff bricks or holes exist (the LLM critique
    only fires when significant; tracks the mana_base / card_advantage
    convention).
  - `discrepancies`: human-readable summary list, used for warning
    messages and the optional LLM critique prompt.

The output is informational — Phase 9 itself does NOT mutate the deck.
A future phase (or the LLM critique) decides whether to act on the
recommendations.

Public API:
  - `analyze_curve(deck, archetype_hint, pool) -> CurveAnalysis`
  - `CurveAnalysis` dataclass
  - `load_archetype_curves(path=None) -> Dict[str, Dict[str, Any]]`
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


CURVE_SMOOTHER_VERSION = "curve_smoother_v1.0"

_CURVES_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "curve_targets_by_archetype_v1.json"
)

_CMC_SLOTS = ("0", "1", "2", "3", "4", "5", "6", "7+")

# Cached load. Module-level — every fresh process re-reads once.
_CURVES_CACHE: Dict[str, Any] = {}


@dataclass
class CurveAnalysis:
    archetype_hint: Optional[str]
    resolved_archetype: str  # what we actually matched in the JSON ("default" if unknown)
    archetype_target: Dict[str, int]
    deck_curve: Dict[str, int]
    bricks: List[Dict[str, Any]] = field(default_factory=list)
    holes: List[Dict[str, Any]] = field(default_factory=list)
    significant: bool = False
    discrepancies: List[str] = field(default_factory=list)
    nonland_card_count: int = 0
    version: str = CURVE_SMOOTHER_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def load_archetype_curves(path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the curve_targets JSON, caching by path."""
    p = Path(path) if path else _CURVES_PATH
    if _CURVES_CACHE.get("_path") == str(p):
        return _CURVES_CACHE.get("archetypes") or {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        _CURVES_CACHE["_path"] = str(p)
        _CURVES_CACHE["archetypes"] = {}
        return {}
    arches = data.get("archetypes") if isinstance(data, dict) else None
    if not isinstance(arches, dict):
        arches = {}
    _CURVES_CACHE["_path"] = str(p)
    _CURVES_CACHE["archetypes"] = arches
    return arches


def _cmc_slot(cmc: float) -> str:
    """Bucket a floating CMC into one of the 8 fixed slots."""
    try:
        n = int(cmc)
    except (TypeError, ValueError):
        return "0"
    if n < 0:
        return "0"
    if n >= 7:
        return "7+"
    return str(n)


def _is_land(card: Dict[str, Any], basic_land_names: Optional[set] = None) -> bool:
    """Return True if the deck entry is a land. We use the same heuristic
    the mana_base optimizer uses: source-tagged 'mana_base', a known basic
    land name, or a [slot=land] marker in the reason text."""
    if card.get("source") == "mana_base":
        return True
    name = (card.get("card_name") or "").strip()
    if basic_land_names is not None and name in basic_land_names:
        return True
    if "[slot=land]" in (card.get("reason") or ""):
        return True
    return False


def analyze_curve(
    *,
    deck: List[Dict[str, Any]],
    archetype_hint: Optional[str],
    pool: Optional[Dict[str, Any]] = None,
    basic_land_names: Optional[set] = None,
    archetype_curves: Optional[Dict[str, Any]] = None,
) -> CurveAnalysis:
    """Compute the deck's curve analysis against an archetype-typical
    target.

    Args:
      deck: the 100-card deck. Each entry should have `card_name`. CMC
        is pulled from `pool['candidates']` by name; if the pool isn't
        provided or the lookup fails, the deck entry's own `cmc` field
        is used (falls back to 0).
      archetype_hint: e.g. "tribal", "combo", "counters_matter". Maps
        to a `archetypes[key]` entry in the JSON. Unknown archetypes
        fall back to `"default"`.
      pool: the candidate pool dict (optional, for cmc/mana_cost lookup).
      basic_land_names: optional set of names treated as basic lands
        for land detection. If None, no basics are skipped via name
        (mana_base source-tagging still applies).
      archetype_curves: optional pre-loaded archetype curves dict (for
        tests). When None, loads from the canonical JSON file.

    Returns: `CurveAnalysis`.
    """
    arches = archetype_curves if archetype_curves is not None else load_archetype_curves()

    hint_key = (archetype_hint or "").strip().lower()
    if hint_key not in arches:
        resolved = "default"
    else:
        resolved = hint_key
    arch_spec = arches.get(resolved) or arches.get("default") or {}
    target = {s: int(arch_spec.get("target", {}).get(s, 0)) for s in _CMC_SLOTS}
    ceiling = int(arch_spec.get("ceiling", 7))
    hole_pct = float(arch_spec.get("hole_pct", 0.5))

    # Build the deck's CMC distribution from nonland nonbasic cards.
    pool_by_name_lower = {
        (c.get("name") or "").strip().lower(): c
        for c in (pool or {}).get("candidates") or []
    }
    deck_curve: Dict[str, int] = {s: 0 for s in _CMC_SLOTS}
    bricks: List[Dict[str, Any]] = []
    nonland_count = 0
    for card in deck:
        if _is_land(card, basic_land_names):
            continue
        name = (card.get("card_name") or "").strip()
        if not name:
            continue
        # CMC: prefer pool match, fall back to card field.
        cmc = 0.0
        match = pool_by_name_lower.get(name.lower())
        if match is not None:
            cmc = float(match.get("cmc") or 0.0)
        else:
            try:
                cmc = float(card.get("cmc") or 0.0)
            except (TypeError, ValueError):
                cmc = 0.0
        slot = _cmc_slot(cmc)
        deck_curve[slot] = deck_curve.get(slot, 0) + 1
        nonland_count += 1
        # Brick check — integer CMC above the ceiling.
        if int(cmc) > ceiling:
            bricks.append({
                "card_name": name,
                "cmc": cmc,
                "ceiling": ceiling,
            })

    # Hole check: any slot where actual < target * hole_pct.
    holes: List[Dict[str, Any]] = []
    for slot in _CMC_SLOTS:
        t = target.get(slot, 0)
        actual = deck_curve.get(slot, 0)
        if t > 0 and actual < t * hole_pct:
            holes.append({"cmc": slot, "actual": actual, "target": t})

    discrepancies: List[str] = []
    if bricks:
        sample = ", ".join(f"{b['card_name']} (CMC {b['cmc']:g})" for b in bricks[:3])
        more = f", +{len(bricks)-3} more" if len(bricks) > 3 else ""
        discrepancies.append(
            f"{len(bricks)} bricks above the {resolved} archetype ceiling "
            f"(CMC > {ceiling}): {sample}{more}"
        )
    if holes:
        sample = ", ".join(
            f"CMC {h['cmc']} ({h['actual']}/{h['target']})" for h in holes[:3]
        )
        more = f", +{len(holes)-3} more" if len(holes) > 3 else ""
        discrepancies.append(
            f"{len(holes)} curve holes for {resolved} archetype "
            f"(below {int(hole_pct*100)}% of target): {sample}{more}"
        )

    return CurveAnalysis(
        archetype_hint=archetype_hint,
        resolved_archetype=resolved,
        archetype_target=target,
        deck_curve=deck_curve,
        bricks=bricks,
        holes=holes,
        significant=bool(bricks) or bool(holes),
        discrepancies=discrepancies,
        nonland_card_count=nonland_count,
    )
