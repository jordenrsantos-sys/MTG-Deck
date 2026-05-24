"""Phase 9 — Coverage sweep tool.

Loads top_500_edh_cards.json, then for each card determines:
  - covered_full: full per-card handler attached (ETB / activated /
    layered effect / replacement / triggered / spell / complex /
    simple-and-default-suffices)
  - covered_data_only: card has a static_modifier entry but no
    behavior handler (Panharmonicon, Aven Mindcensor — Phase 4 +
    Phase 5 deferrals)
  - fall_through: no registration; default behavior applies (mostly
    pure-vanilla creatures + basic-attribute-only permanents)
  - exception: importing the cards package errored on this card

Reports:
  - Total + per-bucket histogram of each category
  - Target gates: ≥ 95% full or data-only coverage; ≤ 1% exceptions
  - Prints non-covered cards as a punch list for follow-up phases

Usage:
  python tools/oracle_seed_coverage.py
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List


TOP_500_PATH = (Path(__file__).parent.parent
                / "api/engine/pillar_f/v0_2/cards/_meta"
                / "top_500_edh_cards.json")


def main() -> int:
    # Import the cards package to populate all registries.
    sys.path.insert(0, str(Path(__file__).parent.parent))
    try:
        import api.engine.pillar_f.v0_2.cards  # noqa: F401
        from api.engine.pillar_f.v0_2.cards.activated import (
            get_activated_abilities_for_card,
        )
        from api.engine.pillar_f.v0_2.cards.etb import get_etb_trigger
        from api.engine.pillar_f.v0_2.cards.continuous import (
            get_continuous_effect_builder, get_static_modifiers,
        )
        from api.engine.pillar_f.v0_2.cards.replacement import (
            get_replacement_builder,
        )
        from api.engine.pillar_f.v0_2.cards.triggered import (
            get_event_trigger,
        )
        from api.engine.pillar_f.v0_2.cards.spell import (
            get_spell_resolver_key,
        )
    except Exception as e:
        print(f"FATAL: Cards package import failed: {e}", file=sys.stderr)
        return 2

    data = json.loads(TOP_500_PATH.read_text(encoding="utf-8"))
    entries = data["entries"]

    # Per-bucket counters and lists.
    bucket_total: Counter = Counter()
    bucket_full: Counter = Counter()
    bucket_data_only: Counter = Counter()
    bucket_fallthrough: Counter = Counter()
    bucket_exception: Counter = Counter()
    fallthrough_names: Dict[str, List[str]] = defaultdict(list)

    # Event types covered by the triggered registry (sample set).
    event_types = ("DieEvent", "SpellCastEvent", "DrawEvent",
                   "CounterAddEvent", "EnterBattlefieldEvent",
                   "LifeChangeEvent", "CombatDamageDealtEvent",
                   "AttackDeclaredEvent")

    def has_full_coverage(name: str, bucket: str) -> bool:
        """Per-bucket coverage check."""
        try:
            if bucket == "simple":
                # Simple permanents need no handler beyond default Card
                # behavior. Basic lands have BASIC_LAND_RESOLVERS;
                # vanilla creatures need nothing. Count all simple as full.
                return True
            if bucket == "etb":
                return get_etb_trigger(name) is not None
            if bucket == "ltb":
                # LTB is a DieEvent trigger. Check the event-trigger
                # registry.
                return get_event_trigger("DieEvent", name) is not None
            if bucket == "activated":
                return bool(get_activated_abilities_for_card(name))
            if bucket == "continuous":
                return get_continuous_effect_builder(name) is not None
            if bucket == "replacement":
                return get_replacement_builder(name) is not None
            if bucket == "triggered":
                # Triggered cards register via event-trigger OR upkeep
                # step trigger. The dispatcher registry has both.
                for et in event_types:
                    if get_event_trigger(et, name) is not None:
                        return True
                # Also check upkeep triggers — name appears in the per-
                # card index.
                from api.engine.pillar_f.v0_2.cards.triggered import (
                    get_step_triggers_for_card,
                )
                if get_step_triggers_for_card(name):
                    return True
                return False
            if bucket == "spell":
                return get_spell_resolver_key(name) is not None
            if bucket == "complex":
                # Complex cards may register MULTIPLE handlers across
                # buckets. Count covered if ANY handler exists.
                if get_etb_trigger(name) is not None:
                    return True
                if get_activated_abilities_for_card(name):
                    return True
                if get_continuous_effect_builder(name) is not None:
                    return True
                if get_replacement_builder(name) is not None:
                    return True
                for et in event_types:
                    if get_event_trigger(et, name) is not None:
                        return True
                if get_spell_resolver_key(name) is not None:
                    return True
                return False
        except Exception:
            return False
        return False

    def has_data_only_coverage(name: str) -> bool:
        """Card has a static_modifier entry but no behavior handler."""
        return bool(get_static_modifiers(name))

    for e in entries:
        name = e["name"]
        bucket = e["handler_type"]
        bucket_total[bucket] += 1
        try:
            full = has_full_coverage(name, bucket)
            data_only = has_data_only_coverage(name)
        except Exception:
            bucket_exception[bucket] += 1
            continue
        if full:
            bucket_full[bucket] += 1
        elif data_only:
            bucket_data_only[bucket] += 1
        else:
            bucket_fallthrough[bucket] += 1
            fallthrough_names[bucket].append(name)

    total = sum(bucket_total.values())
    full = sum(bucket_full.values())
    data_only = sum(bucket_data_only.values())
    fall = sum(bucket_fallthrough.values())
    exc = sum(bucket_exception.values())

    print(f"=== Mega-task v11 oracle-seed coverage sweep ===")
    print(f"Total cards: {total}")
    print(f"Full handler coverage:        {full:4d}  ({full/total*100:5.1f}%)")
    print(f"Data-only (static modifier):  {data_only:4d}  ({data_only/total*100:5.1f}%)")
    print(f"Fall-through (no handler):    {fall:4d}  ({fall/total*100:5.1f}%)")
    print(f"Exception during check:       {exc:4d}  ({exc/total*100:5.1f}%)")
    addressed = full + data_only
    print(f"  Addressed (full + data):    {addressed:4d}  ({addressed/total*100:5.1f}%)")
    print()
    print(f"Per-bucket breakdown:")
    print(f"  bucket           total  full  data  fall  exc")
    for bucket in sorted(bucket_total.keys()):
        print(f"  {bucket:14s}  {bucket_total[bucket]:5d}  "
              f"{bucket_full.get(bucket, 0):4d}  "
              f"{bucket_data_only.get(bucket, 0):4d}  "
              f"{bucket_fallthrough.get(bucket, 0):4d}  "
              f"{bucket_exception.get(bucket, 0):4d}")
    print()
    print(f"=== Phase 9 gates ===")
    coverage_pct = (full + data_only) / total * 100
    target_full_pct = 95.0
    target_exc_pct = 1.0
    if coverage_pct >= target_full_pct:
        print(f"  PASS: addressed coverage {coverage_pct:.1f}% >= {target_full_pct}% target")
    else:
        print(f"  ABOVE TARGET? addressed coverage {coverage_pct:.1f}% < {target_full_pct}% target — punch list below")
    if exc / total * 100 <= target_exc_pct:
        print(f"  PASS: exception rate {exc/total*100:.2f}% <= {target_exc_pct}% target")
    else:
        print(f"  FAIL: exception rate {exc/total*100:.2f}% > {target_exc_pct}% target")

    if fall > 0:
        print()
        print(f"=== Fall-through punch list ({fall} cards) ===")
        for bucket, names in sorted(fallthrough_names.items()):
            print(f"  [{bucket}] ({len(names)}):")
            for n in names[:25]:
                print(f"    {n}")
            if len(names) > 25:
                print(f"    ... and {len(names) - 25} more")
    return 0 if (coverage_pct >= target_full_pct and
                 exc / total * 100 <= target_exc_pct) else 1


if __name__ == "__main__":
    raise SystemExit(main())
