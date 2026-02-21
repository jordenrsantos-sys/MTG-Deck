# CURATED PACK MANIFEST CONTRACT V1
Version: curated_pack_manifest_contract_v1_1

## Purpose
- Define the runtime contract for `api/engine/data/packs/curated_pack_manifest_v1.json`.
- This manifest is the single source of truth for curated runtime pack resolution.
- Runtime must not use directory globbing or heuristic pack discovery.

## Deterministic Load and Selection Rules
- Manifest version must be exactly `curated_pack_manifest_v1`.
- Each `packs[]` entry is normalized then sorted by the stable key:
  1. `load_order`
  2. `pack_id`
  3. `pack_version`
  4. `path`
  5. `sha256`
  6. `created_by`
- Duplicate `(pack_id, pack_version)` entries are invalid and fail load.
- `resolve_pack_entry(pack_id=..., pack_version=None)` selects the deterministic latest candidate as the last item in the stable sorted candidate list.

## Path Rules
- `path` must be a normalized repo-relative path.
- Absolute/rooted paths are invalid (examples: `/abs/path.json`, `C:/abs/path.json`).
- Traversal is invalid (`..` segments after normalization).
- Runtime resolves only via repo-root + validated relative `path`.

## Hash Rules
- Every manifest entry must include `sha256` as a 64-char lowercase hex digest.
- `validate_manifest_hashes()` compares on-disk file SHA-256 to manifest `sha256`.
- Missing file or hash mismatch is a hard failure (no best-effort fallback).

## Taxonomy Reference Filtering/Ordering
- `collect_taxonomy_pack_refs(taxonomy_version=...)` includes only entries where:
  - `pack_version == taxonomy_version`, and
  - `pack_id` in `{taxonomy_primitives, taxonomy_primitive_mappings}`.
- Returned refs are deterministically sorted with the same stable entry sort key.

## Official Error-Code Mapping Rules
Curated manifest lookup/load errors must map as follows:
- Spellbook loader (`api/engine/combos/commander_spellbook_variants_v1.py`):
  - `CURATED_PACK_MANIFEST_V1_*` lookup failures -> `SPELLBOOK_VARIANTS_V1_MISSING`
- Two-card combos v1 loader (`api/engine/two_card_combos.py`):
  - `CURATED_PACK_MANIFEST_V1_*` lookup failures -> `TWO_CARD_COMBOS_V1_MISSING`
- Two-card combos v2 loader (`api/engine/combos/two_card_combos_v2.py`):
  - `CURATED_PACK_MANIFEST_V1_*` lookup failures -> `TWO_CARD_COMBOS_V2_MISSING`

Covered curated-manifest lookup/load errors for mapping:
- `CURATED_PACK_MANIFEST_V1_MISSING`
- `CURATED_PACK_MANIFEST_V1_INVALID_JSON`
- `CURATED_PACK_MANIFEST_V1_INVALID`
- `CURATED_PACK_MANIFEST_V1_DUPLICATE_ENTRY`
- `CURATED_PACK_MANIFEST_V1_PACK_NOT_FOUND`
- `CURATED_PACK_MANIFEST_V1_FILE_MISSING`

## Required-Effects Deterministic Fallback
- In `api/engine/required_effects_v1.py`, taxonomy primitive resolution is curated-manifest driven.
- On curated manifest lookup/load failure, resolver behavior is deterministic fallback:
  - `taxonomy_primitive_ids = []`
  - no runtime network lookup; local closed-world only.
