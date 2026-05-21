"""
theme_target_blender_v1 — Mega-task v4 Phase 7.

Blends per-theme target counts from
`theme_target_count_matrix_v1.json` against a B2 theme_profile's
weights to produce theme-aware structural targets for Pillar E
optimizers (mana base, card advantage, future curve smoother).

Public API:
  - `load_target_matrix() -> Dict[str, Dict[str, int]]`
  - `blend_targets_for_profile(theme_profile, matrix=None) -> Dict[str, int]`
  - `MATRIX_PATH` — canonical location

The blender is intentionally simple: weighted sum of the per-theme
target dictionaries, rounded to integers per slot. Themes not in the
matrix fall back to the `default` profile. Pillar E optimizers can
pass the blended dict in as override targets.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


THEME_TARGET_BLENDER_VERSION = "theme_target_blender_v1.0"

MATRIX_PATH = (
    Path(__file__).resolve().parents[1]
    / "data" / "structural" / "theme_target_count_matrix_v1.json"
)


def load_target_matrix(
    path: Optional[Path] = None,
) -> Dict[str, Dict[str, int]]:
    """Load the per-theme target matrix from JSON.

    Returns a dict keyed by theme name with sub-dicts of slot:count.
    The `default` theme is always present as a fallback.
    """
    p = path or MATRIX_PATH
    if not p.is_file():
        return {"default": {"lands": 36, "ramp": 10, "draw": 10,
                            "interaction": 10, "creatures": 28,
                            "win_conditions": 4}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"default": {"lands": 36, "ramp": 10, "draw": 10,
                            "interaction": 10, "creatures": 28,
                            "win_conditions": 4}}
    themes = data.get("themes") if isinstance(data, dict) else None
    if not isinstance(themes, dict) or "default" not in themes:
        themes = themes or {}
        themes.setdefault("default", {
            "lands": 36, "ramp": 10, "draw": 10, "interaction": 10,
            "creatures": 28, "win_conditions": 4,
        })
    return themes


def blend_targets_for_profile(
    theme_profile: Optional[Dict[str, Any]],
    matrix: Optional[Dict[str, Dict[str, int]]] = None,
) -> Dict[str, int]:
    """Blend per-theme targets per the profile's weights.

    Args:
      theme_profile: B2's structured output with primary/secondary/
        tertiary slots, each {theme, weight}.
      matrix: optional pre-loaded target matrix.

    Returns:
      Blended slot → int dict. Slots present in any contributing
      theme's row are included; missing slots default to 0 per row.

    Defaults to the `default` theme's row when profile is missing or
    no slot weighs more than 0.
    """
    if matrix is None:
        matrix = load_target_matrix()

    if not isinstance(theme_profile, dict):
        return dict(matrix.get("default", {}))

    contributions = []
    total_weight = 0.0
    for slot_name in ("primary", "secondary", "tertiary"):
        entry = theme_profile.get(slot_name)
        if not isinstance(entry, dict):
            continue
        theme = (entry.get("theme") or "").strip().lower()
        try:
            weight = float(entry.get("weight") or 0.0)
        except (TypeError, ValueError):
            weight = 0.0
        if not theme or weight <= 0:
            continue
        theme_row = matrix.get(theme) or matrix.get("default", {})
        contributions.append((weight, theme_row))
        total_weight += weight

    if not contributions:
        return dict(matrix.get("default", {}))

    # Union of slots across contributing rows.
    all_slots = set()
    for _, row in contributions:
        all_slots.update(row.keys())
    blended: Dict[str, float] = {s: 0.0 for s in all_slots}
    for weight, row in contributions:
        for slot in all_slots:
            blended[slot] += (row.get(slot, 0) * weight)
    # Normalize by total weight + round.
    if total_weight > 0:
        for slot in blended:
            blended[slot] /= total_weight
    return {slot: int(round(v)) for slot, v in blended.items()}
