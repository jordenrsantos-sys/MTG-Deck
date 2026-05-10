# mtg-engine — repo

Engineer quickstart. For project intro + architecture, see `../README.md`.

## Setup

```bash
# Python engine (Python 3.10+)
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Node UI (Node 18+)
cd ui_harness
npm install
cd ..
```

`mtg.sqlite` is read-only at runtime. The image cache lives at `data/card_images/`. Snapshot data lands under `data/snapshots/<snapshot_id>/`.

## Run

Engine (terminal 1):

```bash
.venv\Scripts\python.exe -m uvicorn api.main:app --reload --port 8000
```

UI (terminal 2):

```bash
cd ui_harness
npm run dev    # localhost:5173
```

The UI proxies `/build`, `/import`, `/cards`, `/health`, `/snapshots`, `/snapshot` to localhost:8000 (see `ui_harness/vite.config.ts`).

## Test

```bash
# Targeted regression (fast — pre-flight before any task)
.venv\Scripts\python.exe -m pytest \
  tests/test_score_formula_v1.py \
  tests/test_seed_synergy_detection_v1_layer.py \
  tests/test_pipeline_seed_synergy_detection_v1.py \
  tests/test_schema_freeze_guard.py \
  tests/test_curated_pack_manifest_v1.py \
  tests/test_update_external_deck_corpus.py \
  tests/test_run_calibration_validator.py

# Full pytest (716 collected; 711 pass + 3 skip + 2 pre-existing fail)
.venv\Scripts\python.exe -m pytest -q

# Vitest (244 across 20 files post-Phase 4.14)
cd ui_harness && npx vitest run

# UI build smoke
cd ui_harness && npm run build
```

**Pre-existing failures (out of scope unless task addresses one):**
- `tests/test_pipeline_profile_bracket_enforcement_v1.py::test_pipeline_reports_profile_bracket_enforcement_payload_and_panel` — `assert 0 == 1` from `{'count': 0, 'policy': 'DISALLOW', 'supported': True}.get('count')`. Calibration-honest signal preserved across 18+ landings.
- `tests/test_bracket_gc_limits_v1.py::BracketGcLimitsV1Tests::test_b4_and_b5_are_unlimited` — same bracket-domain category.

## Layout

```
repo/
├── api/
│   ├── engine/
│   │   ├── layers/                    Pipeline stages (1 file per layer)
│   │   ├── scoring/                   Score formula DSL + evaluator
│   │   ├── data/                      Curated packs
│   │   │   ├── calibration/           calibration_only:true (skipped at runtime)
│   │   │   ├── themes/                Theme definitions + signal vocabularies
│   │   │   ├── primitives/            Card primitives + tags
│   │   │   ├── packs/                 curated_pack_manifest_v1.json
│   │   │   ├── recommendations/       Commander recommendation weights
│   │   │   └── ...
│   │   ├── curated_pack_manifest_v1.py  Runtime pack loader (skips calibration)
│   │   ├── layer_registry.py          Layer dependency graph
│   │   └── pipeline_build.py          Orchestrator
│   ├── main.py                        FastAPI app (POST /build, /import/url, etc.)
│   └── import_url_v1.py               Engine-4A URL importer
├── ui_harness/                        React + Vite + TypeScript
│   ├── src/
│   │   ├── views/                     Top-level routes
│   │   ├── components/                Per-feature panels (stats / recommendation / seed / playtest / workspace / cards / deck)
│   │   ├── lib/                       Pure helpers (no React)
│   │   ├── ui/primitives/             Design system (Button / Card / Tabs / Dialog / etc.)
│   │   └── parsers/                   Decklist parsers (Archidekt / Arena / MTGO / plain / file)
│   ├── tests/                         Vitest (.test.ts pattern; node env, no jsdom)
│   ├── vite.config.ts
│   └── tailwind.config.js
├── tools/                             Offline tooling
│   ├── playtest/                      Phase 5b sim framework (calibration_only)
│   ├── perf/                          Per-layer timing harness
│   └── ...
├── tests/                             Pytest
├── data/
│   ├── card_images/                   Image cache (ignored by git; populated at runtime)
│   ├── snapshots/                     Snapshot rotation
│   └── ...
├── docs/                              ONBOARDING / DEPLOY_RUNBOOK / OPS_CONSIDERATIONS / etc.
├── scripts/                           Release verification
└── requirements.txt
```

## Conventions

- **Stable JSON serialization**: `sort_keys=True, ensure_ascii=False, separators=(",", ":"), single trailing newline`. The Spellbook ingestion script is the reference implementation. All on-disk packs follow this.
- **Closed-world rule**: no runtime network calls; no oracle-text parsing at runtime; no inventing data.
- **Calibration boundary**: runtime engine never reads `calibration_only:true` packs. Offline sim outputs feed user-reviewed curation; the curated pack lands in `data/calibration/`; the engine reads it on next run. Direction is one-way.
- **Frozen contracts**: `ui_contract_v1`, `structural_snapshot_v1`, `graph_v1` shapes are stable. Don't widen unless a task explicitly requires it.
- **Determinism**: outputs must not depend on wall-clock time, dict ordering (no Python <3.7 reliance), locale, or unseeded randomness.

## Editing

Read `../control/AUTOMATION_RULES.md` first. Standing requirements (pre-flight regression + manifest verification + sanity import + pre-existing failure list + frozen contracts + calibration boundary + closed-world + halt-and-surface discipline) apply to every task.
