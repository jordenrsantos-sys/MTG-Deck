# mtg-engine MCP server

Exposes the MTG engine's AI-facing surface (Pillar A endpoints) as MCP tools so Claude Code (and any other MCP-aware AI agent) can call them as typed tool invocations.

## What it wraps

Eight tools, mapping to the engine's HTTP API:

| MCP tool | HTTP endpoint | Use |
|---|---|---|
| `analyze_deck` | `POST /deck/analyze_v1` | Diagnostic snapshot of a deck (themes, primitives, gaps, bracket) |
| `search_cards` | `POST /card/search_v1` | Rich-filter card lookup; returns wide pools |
| `candidate_pool` | `POST /deck/candidate_pool_v1` | Clustered candidates for improving a deck |
| `strength_check` | `POST /deck/strength_check_v1` | Corpus similarity + axis deviations + interpretation |
| `agent_context_bundle` | `POST /agent/context_bundle_v1` | One-call kickoff composite |
| `commander_archetype_brief` | `GET /commander/archetype_brief_v1` | Prior-art summary for a commander |
| `theme_top_cards` | `GET /theme/top_cards_v1` | Highest-signal cards for a theme |
| `corpus_similar_decks` | `POST /corpus/similar_decks_v1` | k corpus decks most similar to input |
| `engine_health` | `GET /health` | Connectivity + active snapshot diagnostic |

## Setup

### Prerequisites

- The engine running locally (`uvicorn api.main:app --reload` from `repo/`) at `http://localhost:8000`
- Python 3.11+
- `mcp` and `httpx` packages installed in the same venv:
  ```bash
  pip install mcp httpx
  ```

### Register with Claude Code

Add to your `~/.claude.json` (or project-level `.mcp.json`):

```json
{
  "mcpServers": {
    "mtg-engine": {
      "command": "python3",
      "args": ["/absolute/path/to/mtg-engine/repo/mcp_server/mtg_engine_mcp.py"],
      "env": {
        "MTG_ENGINE_URL": "http://localhost:8000"
      }
    }
  }
}
```

On Windows, use the full Python interpreter path (e.g., `C:\Python311\python.exe`).

After registering, restart Claude Code. The eight tools above will appear as namespaced tools (`mcp__mtg-engine__analyze_deck`, etc.).

### Test the server standalone

```bash
# Confirm it starts
python3 mcp_server/mtg_engine_mcp.py < /dev/null
# (will hang waiting for stdio MCP messages; Ctrl+C to exit)

# Or use MCP Inspector
npx @modelcontextprotocol/inspector python3 mcp_server/mtg_engine_mcp.py
```

## Architectural rules served

See `Mtg deck building brain/00_SYSTEM_CORE/DESIGN_DECISIONS.md` for full context. This MCP server inherits the engine's rules:

- **Creativity envelope (1.1)** — tool descriptions explicitly state when output is a wide pool vs a structured diagnostic; nothing returns top-N rankings.
- **Speed budget (1.2)** — HTTP timeout is 10s; engine endpoints respect their own <500ms/1000ms warm budgets.
- **Strength oracle (1.3)** — `strength_check` is Measurement A; `measurement_b` is null until Phase 5b ships.
- **Pilot anti-bias (1.4)** — applies to Phase 5b sims, not this surface.

## Typical AI usage flow

For an agent improving an existing deck:
1. `agent_context_bundle(db_snapshot_id, raw_decklist_text, commander, intent="more removal")` — one call, get analyze + candidate_pool + strength_check + reference_decks
2. AI ranks within the returned candidate clusters by its own criteria
3. `strength_check` on each proposed version to validate the improvement
4. Iterate

For an agent building from scratch:
1. `commander_archetype_brief(commander)` — understand the prior art
2. `theme_top_cards(theme_id="TYPAL_GOBLINS", color_identity="R")` — get tribal pool
3. `search_cards` with filters for ramp/draw/removal — get supporting cards
4. AI composes 99 cards from these pools
5. `strength_check` on the proposed build

## Troubleshooting

- **Tools not appearing in Claude Code.** Confirm the server starts standalone. Check `~/.claude.json` JSON is valid. Restart Claude Code.
- **`Connection refused`.** Engine not running. Start with `uvicorn api.main:app --reload` from `repo/`.
- **Slow responses.** First call to `strength_check` or `commander_archetype_brief` vectorizes the corpus (~2s); subsequent calls are <500ms warm. If consistently slow, check engine logs and confirm the snapshot is loaded.

## See also

- `Mtg deck building brain/13_AI_AGENT_SURFACE/ENGINE_API_GUIDE.md` — full endpoint specs
- `Mtg deck building brain/13_AI_AGENT_SURFACE/ARCHETYPE_CATALOG.md` — theme catalog
- `Mtg deck building brain/00_SYSTEM_CORE/DESIGN_DECISIONS.md` — architectural rules
