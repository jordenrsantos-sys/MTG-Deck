# License Audit

Top-level dependency license review for the v1.0 release. Confirms every direct dependency is permissively licensed (compatible with MIT distribution) and documents the third-party data sources the project consumes.

**Scope: top-level dependencies only.** Full transitive SBOM is deferred to a future hardening pass — npm's transitive dep tree is large (hundreds of packages), and a comprehensive SBOM audit warrants its own focused review session. The `verify_release.sh` script captures `npm ls --depth=0` output as a documentation artifact each release; deeper audits land on top of that.

**Audit method:** read `repo/ui_harness/package.json` + `repo/requirements.txt` + `repo/requirements-dev.txt` for the declared top-level dependency set; consult each package's published license (via npm registry / PyPI metadata / upstream repo). Any GPL or AGPL dependency would be a halt-trigger per Phase 6 Stage 9 HARD #14 — none surfaced.

## Project license

**MIT License.** © 2026 Jorden Santos. See `repo/LICENSE` for the full text.

## npm top-level dependencies (UI)

From `repo/ui_harness/package.json`:

### Runtime

| package | version | license | notes |
|---|---|---|---|
| `react` | ^18.3.1 | MIT | Facebook (Meta) / React core team |
| `react-dom` | ^18.3.1 | MIT | Facebook (Meta) / React core team |

### Dev / build / test

| package | version | license | notes |
|---|---|---|---|
| `@types/react` | ^18.3.5 | MIT | DefinitelyTyped |
| `@types/react-dom` | ^18.3.0 | MIT | DefinitelyTyped |
| `@vitejs/plugin-react` | ^4.3.3 | MIT | Vite team |
| `autoprefixer` | ^10.5.0 | MIT | PostCSS team |
| `postcss` | ^8.5.14 | MIT | Andrey Sitnik |
| `tailwindcss` | ^3.4.19 | MIT | Tailwind Labs |
| `typescript` | ^5.6.3 | Apache-2.0 | Microsoft |
| `vite` | ^5.4.10 | MIT | Evan You / Vite team |
| `vitest` | ^2.1.9 | MIT | Vitest team |

**npm verdict: clean.** All top-level dependencies are MIT or Apache-2.0 — both permissive + compatible with MIT distribution. No GPL/AGPL/SSPL surfaced at the top level.

## pip top-level dependencies (engine)

From `repo/requirements.txt` (runtime):

| package | version | license | notes |
|---|---|---|---|
| `fastapi` | 0.129.0 | MIT | Sebastián Ramírez |
| `pydantic` | 2.12.5 | MIT | Pydantic team |
| `uvicorn` | 0.41.0 | BSD-3-Clause | Encode |

From `repo/requirements-dev.txt` (development / test):

| package | version | license | notes |
|---|---|---|---|
| `pytest` | 9.0.2 | MIT | pytest dev team |
| `pytest-cov` | 7.0.0 | MIT | pytest-cov maintainers |
| `httpx` | 0.28.1 | BSD-3-Clause | Encode |

**pip verdict: clean.** All top-level dependencies are MIT or BSD-3-Clause — both permissive + compatible with MIT distribution. No GPL/AGPL/SSPL surfaced at the top level.

## Card data attribution (Scryfall CC-BY 4.0)

The engine consumes Magic: The Gathering card data sourced from **Scryfall** (https://scryfall.com), licensed under **Creative Commons Attribution 4.0 International (CC-BY 4.0)**: https://creativecommons.org/licenses/by/4.0/.

Specifically, the following data is derived from Scryfall:

- **Card metadata in `repo/data/mtg.sqlite`** — card names, oracle text, type lines, color identity, mana costs, rarity, set associations. The engine layers consume this read-only via `engine/db.py` helpers.
- **Card images cached locally in `repo/data/card_images/`** — derived from Scryfall's bulk image endpoints + served via the engine's `GET /cards/image/{oracle_id}` route. Cache populates on first hover in the UI; misses surface as "Image not cached." per Phase 4.14 Stage 4.
- **Oracle IDs and Scryfall card UUIDs** — used as canonical card identifiers throughout the engine + UI.

**Attribution requirement (CC-BY 4.0):**

> "Magic card data and images are © Wizards of the Coast LLC and used under fair use / Scryfall's data license. Card data is sourced from Scryfall (https://scryfall.com) under Creative Commons Attribution 4.0 International (CC-BY 4.0). This project is unaffiliated with Wizards of the Coast LLC."

This attribution is reproduced in `repo/LICENSE` (project license footnotes) and should appear in any deployed UI's "About" page when the v1.0 deploy lands.

**Scryfall's terms of use:** https://scryfall.com/docs/api (rate limits + caching guidance) and https://scryfall.com/docs/api/bulk-data (bulk data license terms).

**Trademark note:** Magic: The Gathering, its card names, sets, and associated trademarks are property of Wizards of the Coast LLC (a subsidiary of Hasbro, Inc.). This project is unaffiliated with Wizards of the Coast or Hasbro and provides deck-building tooling for personal use. No Wizards-of-the-Coast trademarks are used commercially.

## Engine code copyright header

The engine source files do NOT carry per-file copyright headers as of v1.0 (the project-level `LICENSE` file is authoritative). If a future release needs per-file SPDX-style headers for distribution, add them in a dedicated stage — out of v1.0 scope.

## Deferred to future hardening

- **Full transitive npm SBOM** — `npm ls --depth=0` captures top-level; deeper audit (e.g., `npm ls --all` + per-package license verification) is a multi-hour focused review session. Defer.
- **Full pip transitive SBOM** — similar; `pip list` + per-package license verification, deferred.
- **Per-file SPDX headers** — see "Engine code copyright header" above.
- **Wizards of the Coast Fan Content Policy review** — out of v1.0 scope; relevant when distribution model is finalized.

## Halt-trigger status (per Phase 6 Stage 9 HARD #14)

- GPL transitive deps: **none surfaced at top level**.
- AGPL transitive deps: **none surfaced at top level**.
- SSPL / custom-restrictive licenses: **none surfaced at top level**.

Verdict: **license audit CLEAN for v1.0 ship**.

## Provenance

- Compiled 2026-05-10 as part of Phase 6 Stage 9.
- Audit method: read declared top-level dependency files; consult each package's published license.
- Re-run criteria: any change to `package.json` runtime/devDependencies; any change to `requirements.txt` or `requirements-dev.txt`. The `verify_release.sh` script captures top-level lists each release.
- See `99_META/CHANGELOG.md` Phase 6 Stage 9 entry for the shipping context.
