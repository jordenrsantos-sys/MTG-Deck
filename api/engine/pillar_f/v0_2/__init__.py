"""Pillar F v0.2 — rules-correct multiplayer MTG game engine substrate.

Sub-mega-task A (iter 10 / mega-task v9): core engine. Module layout:

    state/      — game-state object model + perspective_view + serialization
    stack/      — stack push/pop + priority loop + counterspell API
    turn/       — phase/step state machine + step transitions
    replacement/— replacement effects + state-based actions
    layers/     — 7-layer continuous effects (CR 613)
    combat/     — declare attackers/blockers + damage assignment

Sub-mega-task B (iter 11+): LLM strategic policy. Plumbs into the
substrate via callback hooks the priority loop exposes.

Sub-mega-task C (iter 12+): Stage 2 graduated playtest. Orchestrates
4 LLM-piloted instances of this engine for measured win-rate validation.

Scoping doc:
  ../../MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md

Iter-10 kickoff:
  api/engine/data/agent/mega_task_v9_kickoff.md
"""

PILLAR_F_V0_2_VERSION = "pillar_f_v0_2.0_sub_mega_task_a"
