from __future__ import annotations

__all__ = ["compile_snapshot_tags"]


def __getattr__(name: str):
    if name == "compile_snapshot_tags":
        from .tag_snapshot import compile_snapshot_tags

        return compile_snapshot_tags
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
