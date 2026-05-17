"""Phase 5b — MTG Playing Agent (MPA) and playtest sim infrastructure.

Architecture per `Mtg deck building brain/15_PILOT/MPA_SPEC.md`:
  - mpa_game_state.py   — game state dataclass (5b.1a)
  - mpa_actions.py      — legal action enumeration (5b.1b)
  - mpa_policy.py       — decision policy (5b.1c — heuristic baseline)
  - mpa_runner.py       — turn loop + anti-bias seat-swap (5b.1d, 5b.2)
  - mpa_calibration.py  — bracket-4-vs-precon benchmark suite (5b.3)

Current ship: 5b.1a + 5b.1b skeleton — proves the foundation. Decision
policy + runner + calibration are subsequent ships.
"""
