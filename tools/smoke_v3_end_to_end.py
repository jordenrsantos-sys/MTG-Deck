"""
smoke_v3_end_to_end — Mega-task v3 Phase 10.

Simulates a "new set drop" by feeding a 10-card synthetic payload
through the full v3 chain:

  1. Phase 2 ingestion (writes to a TEMPORARY sqlite + cleans up).
  2. Phase 3 pipeline (primitive tagging + theme scoring + corpus
     metadata + embedding update [skipped here to avoid Voyage spend]
     + combo flagging).
  3. Phase 4 combo-pair discovery against the synthetic payload itself.
  4. Phase 5 archetype-impact scoring per card.
  5. Phase 6 LLM discovery report writer (real Claude call).
  6. Phase 7 Obsidian publication via the FILESYSTEM fallback
     (writes to a temp vault dir; live MCP write skipped because the
     Obsidian Local REST API isn't running in this environment).
  7. Phase 8 notification (file-only path; env-var-gated).

After the run, verifies:
  - The synthetic cards land in the temp DB with primitives.
  - Combo pairs are discovered.
  - The Obsidian report file appears in the temp vault under
    `NEW_SETS/` with expected frontmatter + 5 sections.
  - The pipeline returns no errors.

Cleanup: removes the temp directory before exiting.

Usage:
    python tools/smoke_v3_end_to_end.py
    python tools/smoke_v3_end_to_end.py --output-dir <path>
        Save the smoke's outputs to a persistent directory (useful for
        manually inspecting the generated report).
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# Synthetic payload — 10 cards with known primitive signatures.
SYNTHETIC_CARDS: List[Dict[str, Any]] = [
    {"oracle_id": "syn-1", "id": "sid-syn-1", "lang": "en",
     "name": "Phantom Sacrificer",
     "mana_cost": "{B}", "cmc": 1,
     "type_line": "Creature - Vampire Wizard",
     "oracle_text": "Sacrifice a creature: Scry 1.",
     "colors": ["B"], "color_identity": ["B"],
     "produced_mana": [], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-2", "id": "sid-syn-2", "lang": "en",
     "name": "Echo Drifter",
     "mana_cost": "{4}{U}", "cmc": 5,
     "type_line": "Creature - Elemental",
     "oracle_text": "Flying.\nWhen Echo Drifter enters the battlefield, draw two cards.\nEvoke {2}{U}",
     "colors": ["U"], "color_identity": ["U"],
     "produced_mana": [], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-3", "id": "sid-syn-3", "lang": "en",
     "name": "Star Forge",
     "mana_cost": "{1}", "cmc": 1,
     "type_line": "Artifact",
     "oracle_text": "{T}: Add {C}{C}.",
     "colors": [], "color_identity": [],
     "produced_mana": ["C"], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-4", "id": "sid-syn-4", "lang": "en",
     "name": "Counter Hex",
     "mana_cost": "{4}{G}{W}{U}{B}", "cmc": 8,
     "type_line": "Enchantment",
     "oracle_text": "If an effect would put one or more counters on a permanent you control, it puts twice that many of those counters on that permanent instead.",
     "colors": ["G", "U", "W", "B"], "color_identity": ["B", "G", "U", "W"],
     "produced_mana": [], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-5", "id": "sid-syn-5", "lang": "en",
     "name": "Time Wedge",
     "mana_cost": "{5}{U}{U}", "cmc": 7,
     "type_line": "Sorcery",
     "oracle_text": "Take an extra turn after this one.",
     "colors": ["U"], "color_identity": ["U"],
     "produced_mana": [], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-6", "id": "sid-syn-6", "lang": "en",
     "name": "Tribal Anthem",
     "mana_cost": "{2}{W}", "cmc": 3,
     "type_line": "Enchantment",
     "oracle_text": "Vampire creatures you control get +1/+1.",
     "colors": ["W"], "color_identity": ["W"],
     "produced_mana": [], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-7", "id": "sid-syn-7", "lang": "en",
     "name": "Mill Spire",
     "mana_cost": "{3}{U}", "cmc": 4,
     "type_line": "Creature - Sphinx",
     "oracle_text": "Each player mills three cards.",
     "colors": ["U"], "color_identity": ["U"],
     "produced_mana": [], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-8", "id": "sid-syn-8", "lang": "en",
     "name": "Ritual Spark",
     "mana_cost": "{R}", "cmc": 1,
     "type_line": "Instant",
     "oracle_text": "Storm.\nAdd one mana of any color.",
     "colors": ["R"], "color_identity": ["R"],
     "produced_mana": [], "keywords": ["Storm"],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-9", "id": "sid-syn-9", "lang": "en",
     "name": "Vanilla Grunt",
     "mana_cost": "{1}{G}", "cmc": 2,
     "type_line": "Creature - Beast",
     "oracle_text": "",
     "colors": ["G"], "color_identity": ["G"],
     "produced_mana": [], "keywords": [],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
    {"oracle_id": "syn-10", "id": "sid-syn-10", "lang": "en",
     "name": "Landfall Echo",
     "mana_cost": "{1}{G}", "cmc": 2,
     "type_line": "Creature - Elemental",
     "oracle_text": "Landfall — Whenever a land enters the battlefield under your control, draw a card.",
     "colors": ["G"], "color_identity": ["G"],
     "produced_mana": [], "keywords": ["Landfall"],
     "legalities": {"commander": "legal"},
     "image_uris": {}, "card_faces": [],
     "image_status": "highres_scan", "released_at": "2026-05-21"},
]


def _setup_temp_db(path: Path) -> None:
    """Create a fresh sqlite with cards + cards_raw schema for the smoke."""
    con = sqlite3.connect(str(path))
    try:
        con.execute("""
            CREATE TABLE cards (
                snapshot_id TEXT, oracle_id TEXT, name TEXT,
                mana_cost TEXT, cmc REAL, type_line TEXT, oracle_text TEXT,
                colors TEXT, color_identity TEXT, produced_mana TEXT,
                keywords TEXT, legalities_json TEXT, primitives_json TEXT,
                primitives_v1_json TEXT,
                image_uris_json TEXT, card_faces_json TEXT, image_status TEXT,
                released_at TEXT,
                PRIMARY KEY (snapshot_id, oracle_id)
            )
        """)
        con.execute("""
            CREATE TABLE cards_raw (
                snapshot_id TEXT, scryfall_id TEXT, oracle_id TEXT,
                lang TEXT, name TEXT, json TEXT,
                PRIMARY KEY (snapshot_id, scryfall_id)
            )
        """)
        con.commit()
    finally:
        con.close()


def run_smoke(output_dir: Path) -> Dict[str, Any]:
    """Run the full v3 chain against the synthetic payload. Returns
    structured results for the caller to verify."""
    from tools.new_set_pipeline_v1 import ingest_new_cards_v1
    from api.engine.extractors.new_combo_discovery_v1 import (
        discover_new_combo_pairs,
    )
    from api.engine.layers.agent_statistical_approximator_v1 import (
        score_card_archetype_impact,
    )
    from api.engine.layers.new_set_report_writer_v1 import write_set_report
    from api.engine.integrations.obsidian_new_set_writer_v1 import (
        publish_via_filesystem,
    )
    from api.engine.integrations.new_set_notifier_v1 import (
        compose_notification, notify,
    )

    # 1. Set up temp DB.
    db_path = output_dir / "smoke.sqlite"
    _setup_temp_db(db_path)

    # 2. Run the v3 pipeline (corpus rows + primitives + themes +
    # heuristic combo flags + embedding-skip).
    result = ingest_new_cards_v1(
        SYNTHETIC_CARDS, db_path, "smoke_snap", skip_embedding=True,
    )

    # 3. Pull each card's primitives back from the temp DB.
    con = sqlite3.connect(str(db_path))
    try:
        rows = con.execute(
            "SELECT name, primitives_v1_json FROM cards "
            "WHERE snapshot_id=?",
            ("smoke_snap",),
        ).fetchall()
    finally:
        con.close()
    primitives_by_name = {
        n: json.loads(p or "[]") for n, p in rows
    }
    cards_with_prims = [
        {"name": n, "primitives": primitives_by_name.get(n) or []}
        for n in [c["name"] for c in SYNTHETIC_CARDS]
    ]

    # 4. Phase 4 combo-pair discovery (in-set only — no real corpus available).
    combo_pairs = discover_new_combo_pairs(
        cards_with_prims, existing_cards=cards_with_prims,
    )

    # 5. Phase 5 archetype-impact scoring per card.
    archetype_impacts: Dict[str, Dict[str, Any]] = {}
    for c in cards_with_prims:
        archetype_impacts[c["name"]] = score_card_archetype_impact(c)

    # 6. Phase 6 LLM report writer.
    ingest_data = {
        "released_at": "2026-05-21",
        "cards": cards_with_prims,
        "archetype_impacts": archetype_impacts,
        "combo_pairs": [p.to_dict() for p in combo_pairs],
    }
    envelope = write_set_report(
        "synv3", "Synthetic V3 Smoke Set", ingest_data,
    )

    # 7. Phase 7 Obsidian publication via filesystem.
    vault_dir = output_dir / "vault"
    pub = publish_via_filesystem(envelope, vault_dir)

    # 8. Phase 8 notification (env-var-gated; skipped if disabled).
    top_archetypes = []
    if archetype_impacts:
        # Aggregate cumulative delta per archetype.
        cum: Dict[str, float] = {}
        for per_arch in archetype_impacts.values():
            for arch, entry in per_arch.items():
                cum[arch] = cum.get(arch, 0.0) + float(entry.get("delta") or 0.0)
        top_archetypes = [
            a for a, _ in sorted(cum.items(), key=lambda kv: -kv[1])[:3]
        ]
    n = compose_notification(
        set_code="synv3", set_name="Synthetic V3 Smoke Set",
        card_count=len(SYNTHETIC_CARDS), top_archetypes=top_archetypes,
        report_path=pub.primary_filepath,
    )
    notification_result = notify(n, allow_desktop_toast=False)

    return {
        "pipeline_result": result.to_dict(),
        "primitives_by_name": primitives_by_name,
        "combo_pairs": [p.to_dict() for p in combo_pairs],
        "archetype_impacts": archetype_impacts,
        "report": {
            "status": envelope.status,
            "cost_usd": envelope.cost_usd,
            "markdown_length": len(envelope.markdown),
            "warnings": envelope.warnings,
        },
        "publication": {
            "status": pub.status,
            "actions": pub.actions,
            "primary_filepath": pub.primary_filepath,
        },
        "notification": {
            "status": notification_result.status,
            "actions": notification_result.actions,
            "warnings": notification_result.warnings,
        },
    }


def verify(results: Dict[str, Any]) -> List[str]:
    """Return list of verification failures (empty list = all passed)."""
    failures: List[str] = []
    # 1. Pipeline ran without errors on every step.
    for step, status in results["pipeline_result"]["per_step_status"].items():
        if "ERROR" in status:
            failures.append(f"pipeline step {step}: {status}")
    # 2. Cards landed in DB with primitives.
    prims = results["primitives_by_name"]
    if len(prims) != len(SYNTHETIC_CARDS):
        failures.append(
            f"primitives table has {len(prims)} entries, expected "
            f"{len(SYNTHETIC_CARDS)}"
        )
    # 3. At least 5 cards have non-empty primitives (vanilla cards may not).
    n_tagged = sum(1 for tags in prims.values() if tags)
    if n_tagged < 5:
        failures.append(
            f"only {n_tagged} cards tagged with primitives (expected ≥5)"
        )
    # 4. Combo pairs discovered.
    if len(results["combo_pairs"]) < 1:
        failures.append("no combo pairs discovered (expected ≥1)")
    # 5. Report writer produced output.
    if results["report"]["status"] not in ("ok", "fallback"):
        failures.append(f"report status: {results['report']['status']}")
    if results["report"]["markdown_length"] < 500:
        failures.append(
            f"report markdown too short: "
            f"{results['report']['markdown_length']} chars"
        )
    # 6. Obsidian publication succeeded.
    if results["publication"]["status"] != "ok":
        failures.append(f"publication status: {results['publication']['status']}")
    # 7. Primary file exists.
    primary = Path(results["publication"]["primary_filepath"])
    if not primary.is_file():
        failures.append(f"primary file missing: {primary}")
    else:
        content = primary.read_text(encoding="utf-8")
        for section in (
            "## Set overview", "## Most impactful new cards",
            "## New combo pairs", "## Archetype winners and losers",
            "## Suggested deck updates",
        ):
            if section not in content:
                failures.append(f"report missing section: {section}")
    return failures


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", default=None,
        help="Persistent output directory (default: temp dir, cleaned up).",
    )
    args = parser.parse_args(argv)

    if args.output_dir:
        out = Path(args.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        cleanup = False
    else:
        td = tempfile.mkdtemp(prefix="mtg_v3_smoke_")
        out = Path(td)
        cleanup = True

    try:
        print(f"Running v3 end-to-end smoke under {out}…", file=sys.stderr)
        results = run_smoke(out)
        failures = verify(results)
        print(json.dumps({
            "output_dir": str(out),
            "results_summary": {
                "primitives_tagged": sum(
                    1 for v in results["primitives_by_name"].values() if v
                ),
                "combo_pairs": len(results["combo_pairs"]),
                "report_status": results["report"]["status"],
                "report_cost_usd": results["report"]["cost_usd"],
                "report_markdown_length": results["report"]["markdown_length"],
                "publication_status": results["publication"]["status"],
                "publication_primary": results["publication"]["primary_filepath"],
                "notification_status": results["notification"]["status"],
            },
            "verification_failures": failures,
        }, indent=2))
        return 0 if not failures else 3
    finally:
        if cleanup:
            shutil.rmtree(out, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
