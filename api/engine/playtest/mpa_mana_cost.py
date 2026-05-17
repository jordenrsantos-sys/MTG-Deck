"""
mpa_mana_cost — Phase 5b.3 prerequisite.

Parses mana cost strings like `{2}{R}{R}` into structured (generic, colored)
representations the MPA can use to validate color requirements precisely.

Currently the MPA uses just `cmc + first color from color_identity` (an
approximation). To pass DESIGN_DECISIONS rule 1.4 calibration (B4 vs precon
must hit ~85%), the pilot needs to know whether a 3-CMC card actually costs
{1}{U}{U} (needs 2 blue mana available) vs {3} (generic only).

This module is the parser; integration into the MPA's _can_pay_cost +
_auto_tap_for_cost is the next ship.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


COLORS = ("W", "U", "B", "R", "G", "C")


@dataclass
class ManaCost:
    """Parsed mana cost. e.g. {2}{R}{R} → generic=2, colored={"R": 2}."""
    generic: int = 0
    colored: Dict[str, int] = field(default_factory=dict)
    hybrid: List[List[str]] = field(default_factory=list)  # e.g. [["W","U"]] for {W/U}
    phyrexian: Dict[str, int] = field(default_factory=dict)  # {W/P} = 1 W or 2 life
    x_cost: int = 0  # count of X symbols
    snow: int = 0
    raw: str = ""

    def total_cmc(self) -> int:
        total = self.generic
        for v in self.colored.values():
            total += v
        for pair in self.hybrid:
            total += 1
        for v in self.phyrexian.values():
            total += v
        return total


# Single-mana-pip patterns we recognize
_PIP_RE = re.compile(r"\{([^{}]+)\}")


def parse_mana_cost(raw: str) -> ManaCost:
    """Parse a mana_cost string from Scryfall format.

    Examples:
      "{2}{R}{R}"  → generic=2, colored={"R": 2}
      "{X}{3}{U}"  → x_cost=1, generic=3, colored={"U": 1}
      "{W/U}"      → hybrid=[["W", "U"]]
      "{2/G}{2/G}" → 2× hybrid mana — modeled as generic=2 (fallback)
      "{R/P}"      → phyrexian={"R": 1}
      "{S}"        → snow=1
      "" or None   → empty cost (e.g. lands)
    """
    cost = ManaCost(raw=raw or "")
    if not raw or not isinstance(raw, str):
        return cost
    for m in _PIP_RE.finditer(raw):
        pip = m.group(1).strip().upper()
        if pip == "":
            continue
        if pip == "X":
            cost.x_cost += 1
            continue
        if pip == "S":
            cost.snow += 1
            continue
        # Numeric (generic)
        if pip.isdigit():
            cost.generic += int(pip)
            continue
        # Phyrexian: e.g. {W/P}
        if "/P" in pip:
            color = pip.split("/")[0]
            if color in COLORS:
                cost.phyrexian[color] = cost.phyrexian.get(color, 0) + 1
            continue
        # Hybrid: e.g. {W/U}, {2/G}
        if "/" in pip:
            parts = pip.split("/")
            valid_parts = [p for p in parts if p in COLORS]
            if valid_parts:
                cost.hybrid.append(valid_parts)
            else:
                # Numeric-or-color hybrid like {2/G} — treat as generic 2 fallback
                num_parts = [int(p) for p in parts if p.isdigit()]
                if num_parts:
                    cost.generic += min(num_parts)
            continue
        # Single colored pip
        if pip in COLORS:
            cost.colored[pip] = cost.colored.get(pip, 0) + 1
            continue
        # Unrecognized — accumulate as generic to keep CMC sane
        # (covers exotic symbols like {HW}, {P}, two-brid like {C/W}, etc.)
    return cost


def can_pay(cost: ManaCost, available: Dict[str, int], x_value: int = 0) -> bool:
    """Check whether `available` (sum of mana pool + untapped sources) is
    sufficient to pay `cost`. Conservative — treats hybrid as needing one
    of either color; phyrexian as needing the colored payment (life
    alternative isn't modeled here).

    Args:
        cost: parsed ManaCost
        available: {color: count} of mana available (W/U/B/R/G/C)
        x_value: chosen value for X (caller decides)
    """
    pool = dict(available)
    for k in COLORS:
        pool.setdefault(k, 0)

    # Pay colored first
    for color, n in cost.colored.items():
        if pool.get(color, 0) < n:
            return False
        pool[color] -= n

    # Pay phyrexian colored (life-alt not modeled; pay colored if avail)
    for color, n in cost.phyrexian.items():
        if pool.get(color, 0) < n:
            return False
        pool[color] -= n

    # Pay hybrid — for each pip, use one mana of any of its colors
    for pair in cost.hybrid:
        paid = False
        for color in pair:
            if pool.get(color, 0) > 0:
                pool[color] -= 1
                paid = True
                break
        if not paid:
            return False

    # Generic + X — any remaining mana suffices
    generic_needed = cost.generic + (cost.x_cost * x_value)
    total_remaining = sum(pool.values())
    return total_remaining >= generic_needed


def select_lands_to_tap(
    cost: ManaCost,
    lands_by_color: Dict[str, List],  # {color: [land_card, ...]}
    x_value: int = 0,
) -> List:
    """Pick a concrete set of lands to tap that satisfies `cost`. Returns the
    list of land card references to tap, or [] if cost can't be paid.

    Tap strategy: satisfy colored requirements first (prefer single-color
    lands), then hybrid (prefer the rarer color among the pair), then
    generic (any color). Doesn't consider dual lands beyond their color list.
    """
    result = []
    pool = {color: list(lands_by_color.get(color, [])) for color in COLORS}

    # Colored
    for color, n in cost.colored.items():
        avail = pool.get(color, [])
        if len(avail) < n:
            return []
        for _ in range(n):
            result.append(avail.pop(0))

    # Phyrexian (treat as colored — life-alt not yet wired)
    for color, n in cost.phyrexian.items():
        avail = pool.get(color, [])
        if len(avail) < n:
            return []
        for _ in range(n):
            result.append(avail.pop(0))

    # Hybrid — pick rarer color first
    for pair in cost.hybrid:
        best = None
        best_count = -1
        for color in pair:
            cnt = len(pool.get(color, []))
            if cnt > best_count:
                best_count = cnt
                best = color
        if best is None or len(pool.get(best, [])) == 0:
            return []
        result.append(pool[best].pop(0))

    # Generic + X — tap any-color, prefer colorless first
    generic_needed = cost.generic + (cost.x_cost * x_value)
    for color in ("C", "W", "U", "B", "R", "G"):
        while generic_needed > 0 and pool.get(color, []):
            result.append(pool[color].pop(0))
            generic_needed -= 1
    if generic_needed > 0:
        return []

    return result
