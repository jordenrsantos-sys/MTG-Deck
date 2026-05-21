# new_set_pipeline_v0 — Runbook

Status: scaffolded in Iter 3 Phase 13. Full functionality lands in
Iter 4 when the Pillar C primitive extractor and the Voyage AI
embedding backend are wired in.

## Purpose

Ingest a new set's worth of cards (within hours of release) so:

- The agent can build decks using newly-printed cards without a 6-
  month corpus-frequency lag.
- C2.2's wild-combo discovery surfaces synergies with cards the LLM
  hasn't seen overrepresented in its training data.
- Pillar F v0.1's win-path catalog can grow to include new mechanics
  as they're identified.

## Pipeline steps

1. **tag_with_primitives** — apply the Pillar C ontology extractor
   to each new card's oracle_text. STUB in iter 3 (returns empty
   tag lists). Iter 4 loads `primitives/ontology_v0.md`, applies
   each tag's `extraction_rule` regexes, writes results to the new
   `cards.primitive_tags_v1` column.

2. **score_for_themes** — apply the existing theme classifier from
   Phase 2.1a (compute_card_theme_score_v1). STUB in iter 3
   because the classifier requires a primed snapshot.

3. **update_corpus_metadata** — write rows to `cards` for the target
   snapshot. Idempotent via `INSERT OR REPLACE`. Includes
   `released_at` so the recent-set boost picks up the new cards
   immediately.

4. **update_embedding_index** — embed each new card via Voyage AI
   and add to `card_embeddings_v1.sqlite`. STUB in iter 3 (Voyage
   AI integration is iter 4 hand-off from Phase 7).

5. **flag_potential_combo_pairs** — heuristic scan of oracle_text
   for combo-relevant phrases (sacrifice, untap, ETB, damage
   equation, lifelink, exile-and-return, mana production). Each
   flagged card is a candidate for combo-pair scanning against the
   existing card pool. Iter 4 extends with primitive-tag-based
   identification.

## Invocation

### Dry-run (validate input only)

```bash
python tools/new_set_pipeline_v0.py --set-data new_set.json
```

No DB writes. Useful for sanity-checking the input shape and seeing
which cards get flagged for combo review.

### Full ingest

```bash
python tools/new_set_pipeline_v0.py \
    --set-data new_set.json \
    --snapshot 20260601_set_FIN_release \
    --db "E:/MTG Root/mtg-engine/data/mtg.sqlite"
```

Writes rows to the cards table under the given snapshot ID. The
snapshot ID convention is `YYYYMMDD_set_XXX_release`.

## Input format

Scryfall-shaped JSON:

```json
{
  "cards": [
    {
      "oracle_id": "abc-123-uuid",
      "name": "New Vampire Card",
      "mana_cost": "{2}{B}{B}",
      "cmc": 4,
      "type_line": "Creature — Vampire",
      "oracle_text": "When ~ enters, each opponent loses 2 life.",
      "colors": ["B"],
      "color_identity": ["B"],
      "released_at": "2026-06-01"
    }
  ]
}
```

Required fields: `oracle_id`, `name`, `released_at`. Other fields
default to empty.

## Iter 4 hand-off

The pipeline is structured so each step can be wired in
incrementally without rewriting the orchestrator:

1. Drop in the Pillar C extractor in `tag_with_primitives()` (1-2
   days once ontology_v0.md's extraction_rule patterns are
   compiled into a registry).
2. Set `VOYAGE_API_KEY` + `pip install voyageai`, then wire the
   actual embed call into `update_embedding_index()` (~30 min once
   the API key is staged).
3. Wire the live theme classifier in `score_for_themes()` once the
   classifier's snapshot-initialization quirk is resolved.

Total iter 4 effort to fully productionize this pipeline: ~3-5
days, gated on the iter 4 priority order in
`pillar_d_iteration_3_validation_report.md`.

## Smoke-test fixture

`tests/test_new_set_pipeline_v0.py` ships with a 5-card fixture
covering the common new-set shape: legendary creature, sorcery,
artifact, enchantment, land. Validates each pipeline step runs
without errors and each step records a status string.
