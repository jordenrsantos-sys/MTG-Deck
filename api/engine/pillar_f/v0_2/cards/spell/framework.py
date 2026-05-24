"""Spell-resolve framework — thin wrapper over substrate register_resolver.

Each one-shot spell registers a `spell_resolve_fn` that the substrate's
resolve_top invokes when the spell resolves off the stack. The
framework's `register_spell(name, fn)` is a thin alias to
register_resolver that also indexes by card name for discoverability.

Per-card modules:
  1. Define the resolve fn: `def counterspell_resolve(state, entry): ...`
  2. Register: `register_spell("Counterspell", counterspell_resolve)`.

The resolver name in the substrate's registry is `spell_<slug>`, so
test code constructs the stack entry with `payment={"resolver":
"spell_counterspell"}`.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


SpellResolveFn = Callable[[GameState, StackEntry], Optional[List[Dict[str, Any]]]]


_SPELL_RESOLVE_REGISTRY: Dict[str, str] = {}  # card_name -> resolver key


def _slug(name: str) -> str:
    return name.lower().replace(" ", "_").replace("'", "").replace(",", "")


def register_spell(card_name: str, fn: SpellResolveFn) -> str:
    """Register a spell's resolver. Returns the resolver key (which the
    test code uses in `payment["resolver"]`)."""
    key = f"spell_{_slug(card_name)}"
    register_resolver(key, fn)
    _SPELL_RESOLVE_REGISTRY[card_name] = key
    return key


def get_spell_resolver_key(card_name: str) -> Optional[str]:
    return _SPELL_RESOLVE_REGISTRY.get(card_name)


def all_registered_spell_names() -> List[str]:
    return sorted(_SPELL_RESOLVE_REGISTRY.keys())


def build_spell_payload(card_name: str, **extra: Any) -> Dict[str, Any]:
    """Build the payment dict for a spell. Raises KeyError if name not
    registered."""
    key = _SPELL_RESOLVE_REGISTRY.get(card_name)
    if key is None:
        raise KeyError(f"No spell resolver registered for {card_name!r}")
    payload: Dict[str, Any] = {"resolver": key}
    payload.update(extra)
    return payload
