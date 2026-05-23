"""Build top_500_edh_cards.json from the strength-oracle corpus.

Two-stage:
  1. Aggregate card-appearance frequency across all decklists +
     commander zone in `data/corpus/corpus_v1.json` (13,408 decks).
  2. Enrich the top 500 with Scryfall oracle metadata from
     `data/scryfall/bulk/default-cards.json` and categorize each card
     into one of the 9 handler-type buckets per the kickoff doc.

Output: top_500_edh_cards.json in this directory. Each entry:
  {oracle_id, name, usage_rate, archetype_hint, primary_function,
   handler_type, type_line, mana_cost, oracle_text}

Categorization heuristics (Oracle-text pattern matching — coarse first
pass; per-card overrides go in `_categorization_overrides.json` next to
this file):

  simple       — vanilla creature (no oracle_text) OR a basic land
  etb          — "When ~ enters the battlefield" / "When you cast ~"
                  (ETB / cast-time trigger)
  ltb          — "When ~ dies" / "When ~ leaves the battlefield"
  activated    — "{X}: " or "{T}: " activated ability syntax
  continuous   — anthems, lords, static "Other creatures you control get"
  replacement  — "if ... would ... instead" or "would enter the battlefield"
                  replacement-effect language
  triggered    — "Whenever ..." / "At the beginning of ..." (non-ETB)
  spell        — Instants and Sorceries — single-resolution spells
  complex      — multi-trigger / multi-ability cards routed to per-card
                  handlers in Phase 8

This script is idempotent — run again to refresh the JSON. Per-card
overrides in `_categorization_overrides.json` always win.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


CORPUS_PATH = Path("E:/MTG Root/mtg-engine/repo/api/engine/data/corpus/corpus_v1.json")
SCRYFALL_PATH = Path("E:/MTG Root/mtg-engine/repo/data/scryfall/bulk/default-cards.json")
OVERRIDES_PATH = Path(__file__).parent / "_categorization_overrides.json"
OUTPUT_PATH = Path(__file__).parent / "top_500_edh_cards.json"
TOP_N = 500


BASIC_LAND_NAMES = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes",
                    "Snow-Covered Plains", "Snow-Covered Island",
                    "Snow-Covered Swamp", "Snow-Covered Mountain",
                    "Snow-Covered Forest"}


def aggregate_corpus_counts() -> Tuple[Counter, int]:
    """Walk corpus_v1.json, count each card's appearance (deck +
    commander zone). Returns (counter, total_decks)."""
    with CORPUS_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    decks: List[Dict[str, Any]] = data.get("decks") or []
    counts: Counter = Counter()
    for d in decks:
        cmd = (d.get("commander") or "").strip()
        if cmd:
            counts[cmd] += 1
        decklist = d.get("decklist") or []
        # Dedupe within a deck for "appearance in N decks" semantics.
        seen = set()
        for raw in decklist:
            name = (raw or "").strip()
            if not name or name in seen:
                continue
            counts[name] += 1
            seen.add(name)
    return counts, len(decks)


def load_scryfall_index() -> Dict[str, Dict[str, Any]]:
    """Return {name → minimal-card-record} from Scryfall default-cards
    bulk. Keeps only one printing per name (the first encountered).
    Filters out tokens, art series, and other non-playable cards.
    Handles dual-faced cards by indexing both `name` and individual
    face names."""
    print("Loading Scryfall bulk (this takes a few seconds)...", file=sys.stderr)
    with SCRYFALL_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    index: Dict[str, Dict[str, Any]] = {}
    skip_layouts = {"token", "double_faced_token", "emblem", "art_series",
                    "vanguard", "planar", "scheme"}
    for c in data:
        layout = c.get("layout", "")
        if layout in skip_layouts:
            continue
        type_line = c.get("type_line", "")
        if "Token" in type_line:
            continue
        # Build minimal record.
        rec = {
            "oracle_id": c.get("oracle_id", ""),
            "name": c.get("name", ""),
            "mana_cost": c.get("mana_cost", ""),
            "cmc": c.get("cmc", 0.0),
            "type_line": type_line,
            "oracle_text": c.get("oracle_text", ""),
            "power": c.get("power"),
            "toughness": c.get("toughness"),
            "loyalty": c.get("loyalty"),
            "colors": c.get("colors") or [],
            "color_identity": c.get("color_identity") or [],
            "keywords": c.get("keywords") or [],
            "layout": layout,
        }
        # Dual-faced: pull front + back faces if present.
        faces = c.get("card_faces") or []
        if faces and not rec["oracle_text"]:
            # Compose oracle_text from faces.
            rec["oracle_text"] = "\n//\n".join(
                f.get("oracle_text", "") for f in faces
            )
            if not rec["mana_cost"] and faces:
                rec["mana_cost"] = faces[0].get("mana_cost", "")
            if not rec["type_line"] and faces:
                rec["type_line"] = faces[0].get("type_line", "")
        name = rec["name"]
        if name and name not in index:
            index[name] = rec
        # Also index individual face names (Akki Lavarunner // Tok-Tok…).
        for f in faces:
            fname = f.get("name", "")
            if fname and fname not in index:
                # Index face as standalone reference, but keep parent record.
                index[fname] = rec
    print(f"  Scryfall index: {len(index)} card entries", file=sys.stderr)
    return index


# ============================================================
# Categorization heuristics
# ============================================================

# Activated-ability pattern: `{cost}: effect`. Cost can be `{T}`, `{X}`,
# `{1}{G}`, `{2}`, etc. Matches a colon after a `}` that follows a
# braced cost (with optional sacrifice/pay-life additions).
_ACTIVATED_RE = re.compile(
    r"(?:\{[^}]+\}|,\s*[Tt]ap|,\s*Sacrifice|,\s*Pay|,\s*Discard)+\s*:",
)
_ETB_RE = re.compile(
    r"(?:[Ww]hen\b[^.]*\benters the battlefield\b"
    r"|[Ww]hen\s+~\s+enters\b"
    r"|[Ww]hen\b[^.]*\benters\b)",
)
_CAST_TRIGGER_RE = re.compile(r"[Ww]hen you cast\b")
_LTB_RE = re.compile(
    r"(?:[Ww]hen\b[^.]*\bdies\b|[Ww]hen\b[^.]*\bleaves the battlefield\b)",
)
_TRIGGERED_RE = re.compile(r"(?:[Ww]henever\b|At the beginning of\b)")
_REPLACEMENT_RE = re.compile(
    r"(?:\bwould\b[^.]*\binstead\b|\bIf\b[^.]*\bwould\b[^.]*\binstead\b"
    r"|\bskip\b[^.]*\bstep\b|\bif you would\b|\benters the battlefield with\b"
    r"|\benters the battlefield tapped\b"
    r"|\benters tapped\b)",
)
_ANTHEM_RE = re.compile(
    r"(?:[Oo]ther\s+(?:[Cc]reatures|[A-Z]\w+)\s+you control get \+|"
    r"[Cc]reatures you control get \+|"
    r"[Oo]ther\s+\w+\s+(?:creatures|you control) get \+|"
    r"As long as you control)",
)


def _ends_in_punct(s: str) -> bool:
    return bool(s) and s.rstrip()[-1:] in ".!?"


def categorize(rec: Dict[str, Any]) -> str:
    """Return one of the 9 handler-type buckets for this card."""
    name = rec.get("name", "")
    type_line = rec.get("type_line", "")
    text = (rec.get("oracle_text") or "").strip()
    types_lower = type_line.lower()

    # 1. Basic lands + vanilla creatures.
    if name in BASIC_LAND_NAMES:
        return "simple"
    if "basic" in types_lower and "land" in types_lower:
        return "simple"
    is_creature = "creature" in types_lower
    is_land = "land" in types_lower
    is_instant_sorcery = ("instant" in types_lower or "sorcery" in types_lower)
    is_enchantment = "enchantment" in types_lower
    is_artifact = "artifact" in types_lower
    is_planeswalker = "planeswalker" in types_lower
    is_legendary = "legendary" in types_lower

    # 2. Instants + sorceries → spell bucket (unless flagged complex by length).
    if is_instant_sorcery:
        # Long, multi-paragraph oracle text suggests complex combo piece.
        if len(text) > 350:
            return "complex"
        return "spell"

    # 3. Vanilla creature with no oracle text → simple.
    if is_creature and not text:
        return "simple"
    # 4. Vanilla land with no oracle text (non-basic but plain) → simple.
    if is_land and not text:
        return "simple"
    # 4b. Lands with only mana-producing activated abilities → activated.
    if is_land and text:
        # Strip lines that are just keyword-style flavor like "({T}: Add {C}.)"
        # We'll just check activated pattern.
        if _ACTIVATED_RE.search(text) and "When" not in text and "Whenever" not in text:
            return "activated"

    # Count "ability paragraphs" — separated by blank lines. Long multi-
    # ability cards (≥ 3 distinct ability paragraphs) → complex.
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if len(paragraphs) >= 3 and not is_planeswalker:
        return "complex"
    if len(text) > 500:
        return "complex"

    # Per-pattern dispatch.
    has_etb = bool(_ETB_RE.search(text)) or bool(_CAST_TRIGGER_RE.search(text))
    has_ltb = bool(_LTB_RE.search(text))
    has_repl = bool(_REPLACEMENT_RE.search(text))
    has_trig = bool(_TRIGGERED_RE.search(text)) and not has_etb
    has_act = bool(_ACTIVATED_RE.search(text))
    has_anthem = bool(_ANTHEM_RE.search(text))

    # Replacement effects win over ETB/LTB (Doubling Season "if a player
    # would put a counter... they put twice as many" is a replacement).
    if has_repl and not (has_etb or has_ltb):
        return "replacement"

    # Multi-pattern → complex (multi-trigger).
    pattern_count = sum([has_etb, has_ltb, has_trig, has_act, has_anthem])
    if pattern_count >= 3:
        return "complex"
    if pattern_count == 2:
        # 2 patterns: ETB + activated (Skullclamp ETB? no — Skullclamp
        # is pure activated/triggered-on-death). ETB + LTB pattern
        # (Reveillark) → use the more specific bucket. Keep it simple:
        # default to triggered or complex.
        if has_etb and has_act and not (has_ltb or has_trig or has_anthem):
            # Common pattern: ETB creature with an activated ability
            # (e.g., Reclamation Sage doesn't have this; but Snapcaster
            # Mage = ETB + flashback grant). Treat as complex for safety.
            return "complex"
        return "complex"

    if has_etb:
        return "etb"
    if has_ltb:
        return "ltb"
    if has_repl:
        return "replacement"
    if has_anthem and (is_enchantment or is_artifact or is_creature):
        return "continuous"
    if has_trig:
        return "triggered"
    if has_act:
        return "activated"

    # Static "lord-style" P/T modifier or keyword grant w/o trigger keyword.
    if "you control get " in text or "you control have " in text:
        return "continuous"

    # Fallthrough: simple permanent (creature with just keyword line).
    if is_creature or is_artifact or is_enchantment:
        return "simple"
    if is_planeswalker:
        # Planeswalkers are all activated-ability dispatch.
        return "activated"
    return "simple"


# ============================================================
# Main
# ============================================================


def main() -> int:
    counts, total_decks = aggregate_corpus_counts()
    print(f"Aggregated {len(counts)} unique cards across {total_decks} decks",
          file=sys.stderr)
    scryfall = load_scryfall_index()

    # Apply per-card categorization overrides.
    overrides: Dict[str, str] = {}
    if OVERRIDES_PATH.exists():
        with OVERRIDES_PATH.open("r", encoding="utf-8") as f:
            overrides = json.load(f) or {}

    # Sort by frequency desc; take cards we can identify in Scryfall.
    ranked: List[Tuple[str, int]] = counts.most_common()
    entries: List[Dict[str, Any]] = []
    skipped_missing: List[str] = []
    for name, n in ranked:
        if len(entries) >= TOP_N:
            break
        rec = scryfall.get(name)
        if rec is None:
            skipped_missing.append(name)
            continue
        usage_rate = round(n / max(1, total_decks), 4)
        handler_type = overrides.get(name) or categorize(rec)
        entries.append({
            "rank": len(entries) + 1,
            "name": name,
            "oracle_id": rec.get("oracle_id", ""),
            "usage_rate": usage_rate,
            "deck_count": n,
            "handler_type": handler_type,
            "mana_cost": rec.get("mana_cost", ""),
            "cmc": rec.get("cmc", 0.0),
            "type_line": rec.get("type_line", ""),
            "colors": rec.get("colors", []),
            "color_identity": rec.get("color_identity", []),
            "keywords": rec.get("keywords", []),
            "power": rec.get("power"),
            "toughness": rec.get("toughness"),
            "loyalty": rec.get("loyalty"),
            "oracle_text": rec.get("oracle_text", ""),
            # archetype_hint + primary_function are descriptive bucket
            # tags layered later by per-phase work; for now empty strings
            # so the schema is stable.
            "archetype_hint": "",
            "primary_function": "",
        })

    # Bucket histogram for quick visibility.
    hist: Counter = Counter(e["handler_type"] for e in entries)
    print(f"Top {len(entries)} handler-type histogram:", file=sys.stderr)
    for bucket, n in hist.most_common():
        print(f"  {bucket:14s} {n:4d}", file=sys.stderr)
    if skipped_missing[:10]:
        print(f"Skipped {len(skipped_missing)} corpus names not found in Scryfall index "
              f"(first 10: {skipped_missing[:10]})", file=sys.stderr)

    out = {
        "version": "v11_oracle_seed_top500_v1",
        "generated_at": "2026-05-23",
        "source": {
            "corpus": "data/corpus/corpus_v1.json",
            "scryfall": "data/scryfall/bulk/default-cards.json",
            "total_decks": total_decks,
        },
        "count": len(entries),
        "bucket_histogram": dict(hist),
        "entries": entries,
    }
    OUTPUT_PATH.write_text(json.dumps(out, indent=2, sort_keys=False), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH} ({len(entries)} entries)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
