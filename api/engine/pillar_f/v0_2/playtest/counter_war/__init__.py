"""Counter-war hook: counter_target_spell resolver (Phase 2 of sub-C).

Owns sub-B Phase 9 gate 6. Registers a policy-layer resolver via the
substrate's register_resolver hook (does NOT modify substrate); plus
updates ~12 counterspell-family card annotations with target_stack_top
so eligible_actions resolves the target at cast time.
"""
