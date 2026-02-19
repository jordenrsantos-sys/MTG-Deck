# Deterministic Taxonomy Compiler

This folder contains the compile-time taxonomy pack format and tooling used by `snapshot_build.tag_snapshot`.

## Goals

- Deterministic outputs for the same `(snapshot_id, taxonomy pack, patch input)`.
- Closed-world processing against local SQLite only.
- No runtime AI and no runtime taxonomy parsing inside deck/build endpoints.
- Reuse existing DB connection entrypoint: `engine.db.connect()`.

## Compiler Model

The taxonomy workflow is split into two phases:

1. **Export phase (`taxonomy.exporter`)**
   - Input: taxonomy workbook (`.xlsx`).
   - Output: versioned pack folder containing JSON sheets + `pack_manifest.json`.
   - Manifest stores deterministic metadata and per-file SHA256 hashes.

2. **Compile phase (`snapshot_build.tag_snapshot`)**
   - Input: `snapshot_id` + taxonomy pack folder (+ optional patch rows).
   - Reads cards from `cards` for the snapshot.
   - Compiles and applies taxonomy rules deterministically.
   - Routes ambiguous/unresolved matches into unknown queue.
   - Persists outputs to dedicated tables.

## Taxonomy Pack Format

A pack folder must contain:

- `rulespec_rules.json`
- `rulespec_facets.json`
- `qa_rules.json`
- `pack_manifest.json`

`pack_manifest.json` includes:

- `taxonomy_version`
- `generated_at`
- `files[]` with `file_name`, `sha256`, `size_bytes`

Loader validation is strict for required files and hash/size integrity.

## DB Outputs

`tag_snapshot` creates/writes the following tables when needed:

- `card_tags`
- `unknowns_queue`
- `patches_applied`

It does **not** alter the existing `cards` table schema.

## CLI Usage

Compiler entrypoints remain module-based (`taxonomy.exporter`, `snapshot_build.tag_snapshot`).

For local syntax/test tooling, use the repo task scripts below.

Compile and build runtime indices:

```powershell
.\.venv\Scripts\python.exe -B -m snapshot_build.tag_snapshot --snapshot_id <SNAPSHOT_ID> --taxonomy_pack taxonomy/packs/<TAXONOMY_VERSION> --build_indices
```

Check runtime tag status for one snapshot/taxonomy:

```powershell
.\.venv\Scripts\python.exe -B -m snapshot_build.tag_snapshot --snapshot_id <SNAPSHOT_ID> --taxonomy_version <TAXONOMY_VERSION> --status
```

Unknowns triage report (top rules + snippets):

```powershell
.\.venv\Scripts\python.exe -B -m snapshot_build.tag_snapshot --snapshot_id <SNAPSHOT_ID> --taxonomy_version <TAXONOMY_VERSION> --unknowns_report
```

Runtime build pipeline now reads compiled `card_tags` (no oracle parsing fallback).
If tags are missing for any requested oracle_id, runtime returns `TAGS_NOT_COMPILED` and instructs to run `snapshot_build.tag_snapshot`.

## Repo Task Scripts (Recommended)

Use these script entrypoints for deterministic tooling runs in Windows terminals:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\check_syntax.ps1
```

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_tests.ps1
```

If the terminal host still cancels direct runs, use an explicit wrapper call (the scripts also retry this pattern automatically):

```powershell
powershell -NoProfile -Command "& '.\\scripts\\check_syntax.ps1'"
```

## Determinism Notes

- Rules are compiled in stable sorted order.
- Per-card evidence and facets are sorted before persistence.
- Equivalence class IDs are hash-derived from stable components.
- Final run hash is derived from sorted `(oracle_id, primitive_ids, equiv_class_ids)` tuples.
