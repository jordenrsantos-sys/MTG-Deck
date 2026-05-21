"""
new_set_report_writer_v1 — Mega-task v3 Phase 6.

LLM-driven "what's new in [set]" markdown report generator. Consumes
the structured output of the v3 pipeline (new cards + primitives +
theme scores + discovered combo pairs + archetype-impact scores) and
produces a 5-section markdown report via Claude Sonnet 4.6.

Architecture:
  - Deterministic pre-processing: gather + rank pipeline outputs.
  - One LLM call with all structured data in the user prompt.
  - Validate the returned JSON envelope against the expected schema.
  - Fall back to a deterministic report skeleton if the LLM is
    unavailable (so the pipeline doesn't lose its final output).

Cost budget per report: $0.10-0.30 (5-10k input, 2-4k output tokens).

Public API:
  - `build_report_inputs(set_code, set_name, pipeline_data)` → dict
  - `write_set_report(set_code, set_name, ingest_data, llm_client=None)`
    → ReportEnvelope (markdown + metadata)
  - `ReportEnvelope` — dataclass with `markdown`, `set_code`, `set_name`,
    `released_at`, `processed_at`, `card_count`, `cost_usd`, `status`.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence


NEW_SET_REPORT_WRITER_VERSION = "new_set_report_writer_v1.0"

_REPORT_INPUT_TOKEN_BUDGET = 16000
_REPORT_OUTPUT_TOKEN_BUDGET = 4000


@dataclass
class ReportEnvelope:
    markdown: str
    set_code: str
    set_name: str
    released_at: str
    processed_at: str
    card_count: int
    cost_usd: float = 0.0
    status: str = "ok"      # ok / fallback / failed
    warnings: List[str] = field(default_factory=list)
    version: str = NEW_SET_REPORT_WRITER_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Input shaping.
# ============================================================


def _rank_impactful_cards(
    pipeline_data: Dict[str, Any], k: int = 10,
) -> List[Dict[str, Any]]:
    """Rank new cards by max archetype-impact delta + combo participation.

    Combines two signals:
      - max(|archetype delta|) across all archetypes from Phase 5
      - count of distinct combo pairs from Phase 4 the card participates in

    Returns top-k entries: {name, max_delta, top_archetype, combo_count,
                            primitives}.
    """
    cards = pipeline_data.get("cards") or []
    impacts = pipeline_data.get("archetype_impacts") or {}
    combo_pairs = pipeline_data.get("combo_pairs") or []
    combo_counts: Dict[str, int] = {}
    for cp in combo_pairs:
        combo_counts[(cp.get("new_card") or "").strip()] = (
            combo_counts.get((cp.get("new_card") or "").strip(), 0) + 1
        )

    ranked: List[Dict[str, Any]] = []
    for c in cards:
        name = (c.get("name") or "").strip()
        if not name:
            continue
        card_impacts = impacts.get(name) or {}
        max_delta = 0.0
        top_arch = "none"
        for arch, entry in card_impacts.items():
            delta = abs(float(entry.get("delta") or 0.0))
            if delta > max_delta:
                max_delta = delta
                top_arch = arch
        ranked.append({
            "name": name,
            "max_delta": round(max_delta, 4),
            "top_archetype": top_arch,
            "combo_count": combo_counts.get(name, 0),
            "primitives": list(c.get("primitives") or []),
        })
    ranked.sort(key=lambda x: (-(x["max_delta"] + 0.05 * x["combo_count"]),
                               x["name"]))
    return ranked[:k]


def _rank_combo_pairs(
    pipeline_data: Dict[str, Any], k: int = 10,
) -> List[Dict[str, Any]]:
    """Rank Phase 4 discovered pairs by confidence."""
    pairs = list(pipeline_data.get("combo_pairs") or [])
    pairs.sort(key=lambda p: (-float(p.get("confidence") or 0.0),
                              p.get("new_card") or ""))
    return pairs[:k]


def _archetype_winners_losers(
    pipeline_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Aggregate Phase 5 deltas per archetype across all cards in the
    set. Returns the top-3 winners (positive cumulative delta) and the
    top-3 losers (negative — rare but tracked).
    """
    impacts = pipeline_data.get("archetype_impacts") or {}
    cumulative: Dict[str, float] = {}
    matched_counts: Dict[str, int] = {}
    for _name, per_arch in impacts.items():
        for arch, entry in per_arch.items():
            delta = float(entry.get("delta") or 0.0)
            cumulative[arch] = cumulative.get(arch, 0.0) + delta
            if entry.get("matched_primitives"):
                matched_counts[arch] = matched_counts.get(arch, 0) + 1
    winners = sorted(cumulative.items(), key=lambda kv: -kv[1])[:3]
    losers = sorted(cumulative.items(), key=lambda kv: kv[1])[:3]
    return {
        "winners": [
            {"archetype": a, "cumulative_delta": round(d, 4),
             "matched_cards": matched_counts.get(a, 0)}
            for a, d in winners if d > 0
        ],
        "losers": [
            {"archetype": a, "cumulative_delta": round(d, 4)}
            for a, d in losers if d < 0
        ],
    }


def _primitive_dimension_coverage(
    pipeline_data: Dict[str, Any],
) -> Dict[str, int]:
    """Count card hits per ontology dimension."""
    try:
        from api.engine.extractors.primitive_extractor_v1 import load_ontology
        ontology = load_ontology()
    except Exception:
        return {}
    dim_by_tag = {tag_id: tag.dimension for tag_id, tag in ontology.items()}
    counts: Dict[str, int] = {}
    for c in pipeline_data.get("cards") or []:
        seen_dims_for_card: set = set()
        for p in c.get("primitives") or []:
            d = dim_by_tag.get(p)
            if d and d not in seen_dims_for_card:
                counts[d] = counts.get(d, 0) + 1
                seen_dims_for_card.add(d)
    return counts


def build_report_inputs(
    set_code: str, set_name: str, pipeline_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Pre-process the pipeline data into the structured input the LLM
    will see in the user prompt. Pure-Python; no LLM calls."""
    return {
        "set_code": set_code,
        "set_name": set_name,
        "released_at": pipeline_data.get("released_at") or "",
        "card_count": len(pipeline_data.get("cards") or []),
        "primitive_dimension_coverage": _primitive_dimension_coverage(pipeline_data),
        "most_impactful_cards": _rank_impactful_cards(pipeline_data, k=10),
        "top_combo_pairs": _rank_combo_pairs(pipeline_data, k=10),
        "archetype_winners_losers": _archetype_winners_losers(pipeline_data),
        "deck_library_entries": pipeline_data.get("deck_library_entries") or [],
    }


# ============================================================
# LLM prompt.
# ============================================================


_SYSTEM_PROMPT = (
    "You are an analyst writing a 'what's new in <set>' report for an MTG "
    "Commander deck-building engine. The engine has just ingested a new "
    "set's worth of cards and run its primitive-tag extractor, statistical "
    "approximator, and combo-pair discovery against the existing corpus. "
    "Your job: convert the structured pipeline output into a 5-section "
    "markdown report that an experienced Commander deck-builder would find "
    "useful.\n\n"
    "Sections (use these exact level-2 markdown headers):\n"
    "  ## Set overview\n"
    "  ## Most impactful new cards\n"
    "  ## New combo pairs\n"
    "  ## Archetype winners and losers\n"
    "  ## Suggested deck updates\n\n"
    "Constraints:\n"
    "  - Reference only cards present in the structured input. Do NOT "
    "    hallucinate card names or invent combos.\n"
    "  - In 'Most impactful new cards', cite each card's top archetype + "
    "    primary primitives + (if relevant) combo participation count.\n"
    "  - In 'New combo pairs', list the top 5-10 pairs with their cards, "
    "    confidence (1.0 = ontology edge, 0.7 = canonical pair), and a "
    "    one-sentence outcome sketch.\n"
    "  - In 'Archetype winners and losers', call out the 1-3 archetypes "
    "    with the highest cumulative archetype-impact delta from this set.\n"
    "  - In 'Suggested deck updates', if `deck_library_entries` is empty, "
    "    write a single line: 'No DECK_LIBRARY entries to evaluate against.'\n"
    "  - Tone: concise, technical, factual. No hyperbole.\n\n"
    "Output VALID JSON ONLY:\n"
    "{\n"
    '  "markdown": "<the full 5-section markdown report>"\n'
    "}\n"
)


def _build_user_prompt(inputs: Dict[str, Any]) -> str:
    return (
        "Generate the markdown report from this structured input:\n\n"
        f"```json\n{json.dumps(inputs, indent=2)}\n```\n\n"
        "Return only the JSON envelope per the system prompt."
    )


# ============================================================
# Fallback (no-LLM) report.
# ============================================================


def _fallback_markdown(inputs: Dict[str, Any]) -> str:
    """Deterministic baseline report when the LLM layer is unavailable.
    Loses interpretive prose but preserves the structured data."""
    parts: List[str] = []
    parts.append(f"# What's new in {inputs.get('set_name', inputs.get('set_code'))}")
    parts.append("")
    parts.append("## Set overview")
    parts.append("")
    parts.append(f"- New cards: {inputs.get('card_count', 0)}")
    parts.append(f"- Released: {inputs.get('released_at') or 'unknown'}")
    cov = inputs.get("primitive_dimension_coverage") or {}
    if cov:
        parts.append("- Primitive coverage by dimension:")
        for dim, n in sorted(cov.items(), key=lambda kv: -kv[1]):
            parts.append(f"  - {dim}: {n} cards")
    parts.append("")
    parts.append("## Most impactful new cards")
    parts.append("")
    cards = inputs.get("most_impactful_cards") or []
    if not cards:
        parts.append("_No archetype-impactful cards detected._")
    else:
        for c in cards:
            parts.append(
                f"- **{c['name']}** — top archetype: `{c['top_archetype']}` "
                f"(delta {c['max_delta']:+.3f}); "
                f"combo participation: {c['combo_count']}; "
                f"primitives: {', '.join(c['primitives']) or 'none'}"
            )
    parts.append("")
    parts.append("## New combo pairs")
    parts.append("")
    pairs = inputs.get("top_combo_pairs") or []
    if not pairs:
        parts.append("_No new combo pairs discovered._")
    else:
        for p in pairs:
            parts.append(
                f"- {p.get('new_card')} + {p.get('paired_with')} "
                f"(confidence {p.get('confidence', '?')}; pattern: "
                f"`{p.get('combo_pattern', '?')}`)"
            )
    parts.append("")
    parts.append("## Archetype winners and losers")
    parts.append("")
    wl = inputs.get("archetype_winners_losers") or {}
    if wl.get("winners"):
        parts.append("**Winners:**")
        for w in wl["winners"]:
            parts.append(
                f"- {w['archetype']}: +{w['cumulative_delta']:.3f} "
                f"(from {w['matched_cards']} cards)"
            )
    else:
        parts.append("_No archetype winners._")
    if wl.get("losers"):
        parts.append("")
        parts.append("**Losers:**")
        for l in wl["losers"]:
            parts.append(f"- {l['archetype']}: {l['cumulative_delta']:.3f}")
    parts.append("")
    parts.append("## Suggested deck updates")
    parts.append("")
    if not inputs.get("deck_library_entries"):
        parts.append("No DECK_LIBRARY entries to evaluate against.")
    else:
        parts.append("_(DECK_LIBRARY evaluation requires Obsidian MCP "
                     "integration; populated via Phase 7.)_")
    parts.append("")
    parts.append("---")
    parts.append(
        f"_Fallback report generated deterministically "
        f"({NEW_SET_REPORT_WRITER_VERSION}). The LLM layer was "
        f"unavailable; structured pipeline data is preserved above._"
    )
    return "\n".join(parts)


# ============================================================
# Public entry point.
# ============================================================


def write_set_report(
    set_code: str,
    set_name: str,
    ingest_data: Dict[str, Any],
    llm_client: Optional[Any] = None,
) -> ReportEnvelope:
    """Generate the 5-section markdown report for the given set's
    pipeline output.

    Args:
      set_code: 3-letter Scryfall set code (lowercased).
      set_name: human-readable set name.
      ingest_data: dict with `cards` (list of {name, primitives, ...}),
        `archetype_impacts` ({card_name: {arch: {delta, ...}}}),
        `combo_pairs` (list of DiscoveredPair dicts),
        `released_at` (str), `deck_library_entries` (optional).
      llm_client: optional pre-built LLM client; default fetched from
        `agent_llm_client_v1.get_default_client()`.

    Returns: ReportEnvelope.
    """
    inputs = build_report_inputs(set_code, set_name, ingest_data)
    processed_at = datetime.now(timezone.utc).isoformat()
    base_envelope_kwargs = dict(
        set_code=set_code, set_name=set_name,
        released_at=inputs.get("released_at") or "",
        processed_at=processed_at,
        card_count=int(inputs.get("card_count") or 0),
    )

    # Resolve LLM client.
    if llm_client is None:
        from api.engine.layers.agent_llm_client_v1 import get_default_client
        llm_client = get_default_client()

    if not llm_client.is_available():
        return ReportEnvelope(
            markdown=_fallback_markdown(inputs),
            cost_usd=0.0, status="fallback",
            warnings=["LLM layer unavailable; using deterministic fallback."],
            **base_envelope_kwargs,
        )

    user = _build_user_prompt(inputs)
    result = llm_client.call_with_budget(
        system=_SYSTEM_PROMPT, user=user,
        max_input_tokens=_REPORT_INPUT_TOKEN_BUDGET,
        max_output_tokens=_REPORT_OUTPUT_TOKEN_BUDGET,
    )

    if not result.ok or not isinstance(result.parsed_json, dict):
        return ReportEnvelope(
            markdown=_fallback_markdown(inputs),
            cost_usd=float(getattr(result, "cost_usd", 0.0)),
            status="failed",
            warnings=[
                f"LLM call failed or returned non-JSON: "
                f"error_code={getattr(result, 'error_code', '?')}; "
                f"falling back to deterministic report.",
            ],
            **base_envelope_kwargs,
        )

    md = str(result.parsed_json.get("markdown") or "").strip()
    if not md:
        return ReportEnvelope(
            markdown=_fallback_markdown(inputs),
            cost_usd=float(getattr(result, "cost_usd", 0.0)),
            status="failed",
            warnings=["LLM returned empty markdown; using fallback."],
            **base_envelope_kwargs,
        )

    return ReportEnvelope(
        markdown=md, cost_usd=float(getattr(result, "cost_usd", 0.0)),
        status="ok", **base_envelope_kwargs,
    )
