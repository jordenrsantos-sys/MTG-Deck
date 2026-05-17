"""
deck_library_v1 — Pillar C.2 Obsidian DECK_LIBRARY auto-generation.

When a deck is saved, write a markdown page to the Obsidian vault under
`Mtg deck building brain/20_DECK_LIBRARY/<commander_slug>_<deck_slug>.md`.
The page includes themes, gaps, bracket, strength check, and the full
decklist — readable by both humans and future AI agents.

Architectural rules served:
  - 1.3 Strength oracle / self-learning: user-approved decks can optionally
    ingest into the corpus on save (per ingest_user_approved_deck()).
  - 1.4 Honest signal: page generation is best-effort; if analyze fails we
    still write the decklist and warn in frontmatter.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


DECK_LIBRARY_VERSION = "deck_library_v1.0"

# Library lives in the Obsidian vault at the user's filesystem. Resolved via
# env var to keep the engine path-portable. Falls back to repo-relative if env
# is unset (useful for tests).
_DEFAULT_LIBRARY_PATH = os.environ.get(
    "MTG_DECK_LIBRARY_PATH",
    str(Path("E:/MTG Root/Mtg deck building brain/20_DECK_LIBRARY")),
)


def save_deck_to_library_v1(
    *,
    db_snapshot_id: str,
    commander: str,
    raw_decklist_text: str,
    deck_name: Optional[str] = None,
    archetype: Optional[str] = None,
    bracket: Optional[str] = None,
    user_id: Optional[str] = None,
    ingest_to_corpus: bool = False,
    library_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Save a deck as a markdown page in the Obsidian DECK_LIBRARY.

    Args:
        db_snapshot_id: Required snapshot ID for analyze + strength check.
        commander: Commander card name.
        raw_decklist_text: Decklist text.
        deck_name: Optional friendly deck name (e.g., "Krenko v2"). Defaults to commander.
        archetype: Optional archetype hint (e.g., "Goblin Tribal Aggro").
        bracket: Optional bracket hint (B1..B5). If not provided, the analyze
            estimate is used.
        user_id: Optional user identifier for source attribution.
        ingest_to_corpus: When True, also ingest this deck into the strength
            oracle corpus (subject to per-event ±0.03 cap — DESIGN_DECISIONS 1.3).
        library_path: Override library directory (testing).

    Returns:
        Dict with status, page_path (absolute), corpus_id (if ingested),
        warnings.
    """
    warnings: List[Dict[str, str]] = []
    library_dir = Path(library_path or _DEFAULT_LIBRARY_PATH)

    # ---- Step 1: ensure directory exists ----
    try:
        library_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return {
            "status": "FAILED",
            "reason": f"Cannot create library dir: {exc}",
            "page_path": None,
            "corpus_id": None,
            "warnings": warnings,
        }

    # ---- Step 2: run analyze to enrich the page ----
    analyze_payload: Dict[str, Any] = {}
    try:
        from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1
        analyze_payload = compute_deck_analyze_v1(
            db_snapshot_id=db_snapshot_id,
            commander=commander,
            raw_decklist_text=raw_decklist_text,
            include_debug=False,
        )
    except Exception as exc:
        warnings.append({"code": "ANALYZE_FAILED", "message": str(exc)})

    # ---- Step 3: run strength check ----
    strength_payload: Dict[str, Any] = {}
    try:
        from api.engine.layers.deck_strength_check_v1 import compute_deck_strength_check_v1
        strength_payload = compute_deck_strength_check_v1(
            db_snapshot_id=db_snapshot_id,
            commander=commander,
            raw_decklist_text=raw_decklist_text,
            k_nearest=3,
        )
    except Exception as exc:
        warnings.append({"code": "STRENGTH_CHECK_FAILED", "message": str(exc)})

    # ---- Step 4: compose markdown ----
    saved_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    effective_bracket = bracket or analyze_payload.get("bracket_estimate") or "B3"
    effective_archetype = (
        archetype
        or (strength_payload.get("measurement_a", {}) or {}).get("archetype_consensus")
        or "Unknown"
    )
    effective_name = deck_name or f"{commander} build"

    page_md = _compose_markdown(
        deck_name=effective_name,
        commander=commander,
        archetype=effective_archetype,
        bracket=effective_bracket,
        saved_at=saved_at,
        user_id=user_id,
        analyze=analyze_payload,
        strength=strength_payload,
        raw_decklist_text=raw_decklist_text,
    )

    # ---- Step 5: write the page ----
    slug = _slugify(effective_name) or "deck"
    timestamp_suffix = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filename = f"{_slugify(commander)}_{slug}_{timestamp_suffix}.md"
    page_path = library_dir / filename
    try:
        with open(page_path, "w", encoding="utf-8") as f:
            f.write(page_md)
    except Exception as exc:
        return {
            "status": "FAILED",
            "reason": f"Cannot write page: {exc}",
            "page_path": None,
            "corpus_id": None,
            "warnings": warnings,
        }

    # ---- Step 6: update index ----
    try:
        _update_index(library_dir, filename, effective_name, commander,
                      effective_archetype, effective_bracket, saved_at)
    except Exception as exc:
        warnings.append({"code": "INDEX_UPDATE_FAILED", "message": str(exc)})

    # ---- Step 7: optional corpus ingest ----
    corpus_id: Optional[str] = None
    if ingest_to_corpus:
        try:
            from api.engine.layers.deck_strength_check_v1 import ingest_user_approved_deck
            decklist_names = _extract_card_names(raw_decklist_text)
            ingest_result = ingest_user_approved_deck(
                db_snapshot_id=db_snapshot_id,
                commander=commander,
                decklist=decklist_names,
                archetype=effective_archetype,
                bracket=effective_bracket,
                user_id=user_id,
            )
            if ingest_result.get("status") == "OK":
                corpus_id = ingest_result.get("corpus_id")
            else:
                warnings.append({
                    "code": "CORPUS_INGEST_FAILED",
                    "message": ingest_result.get("reason", "unknown"),
                })
        except Exception as exc:
            warnings.append({"code": "CORPUS_INGEST_EXCEPTION", "message": str(exc)})

    return {
        "status": "OK",
        "page_path": str(page_path),
        "corpus_id": corpus_id,
        "deck_name": effective_name,
        "archetype": effective_archetype,
        "bracket": effective_bracket,
        "warnings": warnings,
    }


# ============================================================
# Helpers
# ============================================================


def _slugify(value: str) -> str:
    if not isinstance(value, str):
        return ""
    s = value.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:40]


def _extract_card_names(raw: str) -> List[str]:
    """Lightweight extraction — count-prefixed lines under any section."""
    names: List[str] = []
    if not isinstance(raw, str):
        return names
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        if line.lower() in ("commander", "deck", "sideboard", "mainboard"):
            continue
        # Pattern: optional count + space + name
        m = re.match(r"^(\d+)?[xX]?\s*(.+)$", line)
        if m:
            count = int(m.group(1)) if m.group(1) else 1
            name = m.group(2).strip()
            if name:
                for _ in range(count):
                    names.append(name)
    return names


def _compose_markdown(
    *,
    deck_name: str,
    commander: str,
    archetype: str,
    bracket: str,
    saved_at: str,
    user_id: Optional[str],
    analyze: Dict[str, Any],
    strength: Dict[str, Any],
    raw_decklist_text: str,
) -> str:
    lines: List[str] = []
    # Frontmatter
    lines.append("---")
    lines.append(f"deck_name: \"{deck_name}\"")
    lines.append(f"commander: \"{commander}\"")
    lines.append(f"archetype: \"{archetype}\"")
    lines.append(f"bracket: \"{bracket}\"")
    lines.append(f"saved_at: \"{saved_at}\"")
    if user_id:
        lines.append(f"user_id: \"{user_id}\"")
    lines.append(f"library_version: \"{DECK_LIBRARY_VERSION}\"")
    lines.append("---")
    lines.append("")
    # Title
    lines.append(f"# {deck_name}")
    lines.append("")
    lines.append(f"**Commander:** {commander}")
    lines.append(f"**Archetype:** {archetype}")
    lines.append(f"**Bracket:** {bracket}")
    lines.append(f"**Saved:** {saved_at}")
    if user_id:
        lines.append(f"**Source:** {user_id}")
    lines.append("")

    # Themes
    themes = analyze.get("deck_themes_v1") or []
    if themes:
        lines.append("## Themes detected")
        lines.append("")
        for t in themes[:10]:
            tid = t.get("theme_id", "?")
            score = t.get("score", 0)
            band = t.get("confidence_band", "?")
            lines.append(f"- `{tid}` — score {score} ({band})")
        lines.append("")

    # Composition signal
    lines.append("## Composition signal")
    lines.append("")
    ci = analyze.get("color_identity") or []
    if ci:
        lines.append(f"- **Color identity:** {', '.join(ci)}")
    cc = analyze.get("card_count")
    if cc:
        lines.append(f"- **Card count:** {cc}")
    curve = analyze.get("mana_curve") or {}
    if curve:
        curve_str = ", ".join(f"{k}: {v}" for k, v in sorted(curve.items(), key=lambda kv: int(kv[0])))
        lines.append(f"- **Mana curve (non-land):** {curve_str}")
    pd = analyze.get("primitive_density") or {}
    if pd:
        top = sorted(pd.items(), key=lambda kv: -kv[1])[:10]
        lines.append(f"- **Top primitives:** " + ", ".join(f"{k} ({v})" for k, v in top))
    sd = analyze.get("subtype_density") or {}
    if sd:
        top_sub = sorted(sd.items(), key=lambda kv: -kv[1])[:8]
        lines.append(f"- **Subtype density:** " + ", ".join(f"{k} ({v})" for k, v in top_sub))
    be = analyze.get("bracket_envelope") or {}
    if be:
        lines.append(f"- **Bracket envelope:** {be.get('min_bracket_possible', '?')}–{be.get('max_bracket_possible', '?')} (estimate {be.get('current_estimate', '?')})")
    lines.append("")

    # Strength check
    ma = strength.get("measurement_a") or {}
    band = strength.get("combined_strength_band")
    if band:
        lines.append("## Strength check")
        lines.append("")
        lines.append(f"- **Band:** {band}")
        interp = strength.get("interpretation")
        if interp:
            lines.append(f"- **Interpretation:** {interp}")
        neighbors = ma.get("nearest_neighbors") or []
        if neighbors:
            lines.append(f"- **Top corpus neighbor:** {neighbors[0].get('corpus_id')} (similarity {neighbors[0].get('similarity')})")
        flags = ma.get("axis_deviation_flags") or []
        if flags:
            lines.append("- **Axis deviation flags:**")
            for f in flags[:5]:
                lines.append(f"  - {f.get('axis')}: {f.get('delta'):+d} vs corpus (your {f.get('your_count')}, corpus {f.get('corpus_count')}) — {f.get('severity')}")
        lines.append("")

    # Combos
    combos = analyze.get("detected_combos_v1") or []
    if combos:
        lines.append("## Detected combos")
        lines.append("")
        for c in combos[:10]:
            lines.append(f"- {c.get('label', c.get('variant_id', '?'))}")
        lines.append("")

    # Decklist
    lines.append("## Decklist")
    lines.append("")
    lines.append("```")
    lines.append(raw_decklist_text.strip() or "(empty)")
    lines.append("```")
    lines.append("")

    # Footer
    lines.append("---")
    lines.append("")
    lines.append(f"Generated by `deck_library_v1` ({DECK_LIBRARY_VERSION}). To regenerate, re-call `/deck/save_to_library_v1`.")
    return "\n".join(lines) + "\n"


def _update_index(
    library_dir: Path,
    filename: str,
    deck_name: str,
    commander: str,
    archetype: str,
    bracket: str,
    saved_at: str,
) -> None:
    """Append (or refresh) an entry in the library's _INDEX.md."""
    index_path = library_dir / "_INDEX.md"
    header = (
        "# DECK_LIBRARY index\n\n"
        "Auto-generated by deck_library_v1. Each row links to a saved deck page. "
        "Newest first.\n\n"
        "| Saved | Deck | Commander | Archetype | Bracket | File |\n"
        "|---|---|---|---|---|---|\n"
    )
    new_row = f"| {saved_at} | {deck_name} | {commander} | {archetype} | {bracket} | [{filename}]({filename}) |\n"

    if not index_path.exists():
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(header + new_row)
        return

    # Read existing, prepend new row after header
    try:
        existing = index_path.read_text(encoding="utf-8")
    except Exception:
        existing = ""
    if "|---|" in existing:
        # Insert after the table header divider
        parts = existing.split("|---|---|---|---|---|---|\n", 1)
        if len(parts) == 2:
            new_content = parts[0] + "|---|---|---|---|---|---|\n" + new_row + parts[1]
        else:
            new_content = existing + new_row
    else:
        new_content = header + new_row + existing
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(new_content)
