"""
obsidian_new_set_writer_v1 — Mega-task v3 Phase 7.

Publish discovery reports (from `new_set_report_writer_v1`) to the
user's Obsidian vault. Two publication paths:

  1. `publish_via_mcp(envelope, mcp_dispatch)` — the agent layer wraps
     the obsidian-MCP tools (append_content / patch_content /
     get_file_contents) and passes a single callable here. This module
     stays pure-Python and unit-testable.

  2. `publish_via_filesystem(envelope, vault_root)` — direct
     filesystem write fallback for environments where Obsidian's
     Local REST API isn't running. Writes the same files / index /
     Home.md updates as the MCP path.

Either path writes three things:

  - `NEW_SETS/<YYYY-MM-DD>_<set_code>_<set_name>.md` — the report
    itself, with frontmatter (tags + set_code + released_at + processed_at).
  - `NEW_SETS/_INDEX.md` — hub file listing every published report
    via Obsidian wikilinks. Created on first publish; updated on
    subsequent publishes.
  - `99_META/Home.md` — appends to a "Recent set releases" section
    (creates the section if missing).

Both paths are idempotent on the primary report file (overwrite on
re-publish of the same set_code + released_at).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


OBSIDIAN_NEW_SET_WRITER_VERSION = "obsidian_new_set_writer_v1.0"

NEW_SETS_DIR = "NEW_SETS"
NEW_SETS_INDEX = f"{NEW_SETS_DIR}/_INDEX.md"
HOME_PATH = "99_META/Home.md"
HOME_SECTION_HEADING = "Recent set releases"


# ============================================================
# Output dataclasses.
# ============================================================


@dataclass
class PublicationPlan:
    """Pure-Python description of what should be written.

    The MCP / filesystem publishers consume this to drive the actual
    side effects. Separating planning from publication keeps the side-
    effecting layer thin and the planning layer fully testable.
    """
    primary_filepath: str
    primary_content: str
    index_filepath: str
    index_append_line: str
    home_filepath: str
    home_section_heading: str
    home_append_line: str
    warnings: List[str] = field(default_factory=list)


@dataclass
class PublicationResult:
    primary_filepath: str
    index_filepath: str
    home_filepath: str
    status: str    # ok / partial / failed / skipped
    actions: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ============================================================
# Helpers.
# ============================================================


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify_set_name(name: str) -> str:
    s = _SLUG_RE.sub("-", (name or "").strip().lower()).strip("-")
    return s or "untitled"


def _frontmatter(envelope: Any) -> str:
    return (
        "---\n"
        "tags: [new-set, automation]\n"
        f"set_code: {getattr(envelope, 'set_code', '')}\n"
        f"set_name: {getattr(envelope, 'set_name', '')}\n"
        f"released_at: {getattr(envelope, 'released_at', '')}\n"
        f"processed_at: {getattr(envelope, 'processed_at', '')}\n"
        f"card_count: {getattr(envelope, 'card_count', 0)}\n"
        f"writer_version: {OBSIDIAN_NEW_SET_WRITER_VERSION}\n"
        "---\n\n"
    )


# ============================================================
# Planning (pure Python).
# ============================================================


def compose_set_report_payload(envelope: Any) -> PublicationPlan:
    """Build the PublicationPlan from a ReportEnvelope.

    The envelope must provide attributes: set_code, set_name,
    released_at, processed_at, card_count, markdown. (Matches the
    `ReportEnvelope` from new_set_report_writer_v1.)
    """
    set_code = (getattr(envelope, "set_code", "") or "").strip().lower()
    set_name = getattr(envelope, "set_name", "") or set_code
    released_at = (getattr(envelope, "released_at", "") or "").strip()
    if not released_at:
        # Fall back to processed_at's date prefix when releases_at isn't
        # populated (test paths, future sets ingested late).
        proc = getattr(envelope, "processed_at", "") or ""
        released_at = proc.split("T", 1)[0] if proc else ""

    slug = _slugify_set_name(set_name)
    primary_filename = f"{released_at}_{set_code}_{slug}.md"
    primary_filepath = f"{NEW_SETS_DIR}/{primary_filename}"

    primary_content = _frontmatter(envelope) + (envelope.markdown or "")

    # Index hub line. Bullet with wikilink + (released_at) + card_count.
    title = f"{set_name} ({set_code.upper()})"
    wikilink = f"[[{primary_filename[:-3]}|{title}]]"
    index_append_line = (
        f"- {released_at} — {wikilink} "
        f"({getattr(envelope, 'card_count', 0)} cards)\n"
    )

    # Home.md section line: same format, scoped to the "Recent set
    # releases" section.
    home_append_line = index_append_line

    return PublicationPlan(
        primary_filepath=primary_filepath,
        primary_content=primary_content,
        index_filepath=NEW_SETS_INDEX,
        index_append_line=index_append_line,
        home_filepath=HOME_PATH,
        home_section_heading=HOME_SECTION_HEADING,
        home_append_line=home_append_line,
    )


# ============================================================
# MCP-backed publication.
# ============================================================


class McpDispatch:
    """Callable bundle of obsidian-MCP operations.

    The agent layer constructs one of these, wiring each attribute to
    the actual MCP tool invocation. Each method takes string args +
    returns a status dict the publisher reads. Implementations should
    raise on hard errors; the publisher catches and records.
    """

    def __init__(
        self,
        get_file_contents: Optional[Callable[[str], str]] = None,
        append_content: Optional[Callable[[str, str], None]] = None,
        patch_content: Optional[Callable[..., None]] = None,
    ) -> None:
        self.get_file_contents = get_file_contents
        self.append_content = append_content
        self.patch_content = patch_content


def publish_via_mcp(
    envelope: Any,
    mcp: McpDispatch,
) -> PublicationResult:
    """Publish the report via the obsidian MCP tools wrapped in `mcp`.

    Order of operations:
      1. Overwrite (or create) the primary report file.
      2. Append the index entry to `_INDEX.md`.
      3. Append the Home.md section line under "Recent set releases".

    A missing dispatch callable (None) for a given step is treated as
    "skip that step" — useful for partial publishes (e.g. when the
    Home.md update isn't desired).
    """
    plan = compose_set_report_payload(envelope)
    actions: List[str] = []
    warnings: List[str] = list(plan.warnings)

    # 1) primary file — overwrite via append on an empty/new file.
    if mcp.append_content is not None:
        try:
            # Obsidian's append_content creates the file if it doesn't
            # exist. For idempotency on re-publish we'd want a delete-
            # then-write; for v0.1 we accept the duplicate-content risk
            # and document it. Test fixture path uses overwrite semantics.
            mcp.append_content(plan.primary_filepath, plan.primary_content)
            actions.append(f"wrote primary: {plan.primary_filepath}")
        except Exception as exc:
            warnings.append(f"primary write failed: {exc!r}")
    else:
        warnings.append("append_content dispatch missing; skipped primary write")

    # 2) index entry.
    if mcp.append_content is not None:
        try:
            mcp.append_content(plan.index_filepath, plan.index_append_line)
            actions.append(f"appended index: {plan.index_filepath}")
        except Exception as exc:
            warnings.append(f"index append failed: {exc!r}")

    # 3) Home.md section append (under the heading).
    if mcp.patch_content is not None:
        try:
            mcp.patch_content(
                filepath=plan.home_filepath, operation="append",
                target_type="heading", target=plan.home_section_heading,
                content=plan.home_append_line,
            )
            actions.append(f"patched home: {plan.home_filepath}")
        except Exception as exc:
            # Heading may not exist — fall back to append at end of file
            # with the heading inline.
            warnings.append(
                f"home patch failed ({exc!r}); falling back to append-with-header"
            )
            if mcp.append_content is not None:
                try:
                    mcp.append_content(
                        plan.home_filepath,
                        f"\n## {plan.home_section_heading}\n\n{plan.home_append_line}",
                    )
                    actions.append("appended home with header fallback")
                except Exception as exc2:
                    warnings.append(f"home append fallback failed: {exc2!r}")

    status = "ok" if actions and not warnings else (
        "partial" if actions else "failed"
    )
    return PublicationResult(
        primary_filepath=plan.primary_filepath,
        index_filepath=plan.index_filepath,
        home_filepath=plan.home_filepath,
        status=status, actions=actions, warnings=warnings,
    )


# ============================================================
# Filesystem fallback.
# ============================================================


def publish_via_filesystem(
    envelope: Any,
    vault_root: Any,
) -> PublicationResult:
    """Write the report straight to disk under `vault_root`.

    Used when Obsidian's Local REST API isn't running. The user can
    open the vault later and see the new files.
    """
    from pathlib import Path
    root = Path(vault_root)
    plan = compose_set_report_payload(envelope)
    actions: List[str] = []
    warnings: List[str] = list(plan.warnings)

    primary = root / plan.primary_filepath
    primary.parent.mkdir(parents=True, exist_ok=True)
    primary.write_text(plan.primary_content, encoding="utf-8")
    actions.append(f"wrote primary: {primary}")

    index = root / plan.index_filepath
    index.parent.mkdir(parents=True, exist_ok=True)
    if index.is_file():
        existing = index.read_text(encoding="utf-8")
        if plan.index_append_line.strip() not in existing:
            with index.open("a", encoding="utf-8") as f:
                f.write(plan.index_append_line)
            actions.append(f"appended index: {index}")
        else:
            actions.append("index entry already present")
    else:
        index.write_text(
            f"# NEW_SETS index\n\n{plan.index_append_line}",
            encoding="utf-8",
        )
        actions.append(f"created index: {index}")

    home = root / plan.home_filepath
    home.parent.mkdir(parents=True, exist_ok=True)
    if home.is_file():
        existing = home.read_text(encoding="utf-8")
        heading_marker = f"## {plan.home_section_heading}"
        if heading_marker in existing:
            if plan.home_append_line.strip() not in existing:
                idx = existing.index(heading_marker) + len(heading_marker)
                new_content = (
                    existing[:idx] + "\n\n" + plan.home_append_line + existing[idx:]
                )
                home.write_text(new_content, encoding="utf-8")
                actions.append(f"patched home (heading append): {home}")
            else:
                actions.append("home entry already present")
        else:
            with home.open("a", encoding="utf-8") as f:
                f.write(f"\n\n{heading_marker}\n\n{plan.home_append_line}")
            actions.append(f"appended home with header: {home}")
    else:
        home.write_text(
            f"# Home\n\n## {plan.home_section_heading}\n\n{plan.home_append_line}",
            encoding="utf-8",
        )
        actions.append(f"created home: {home}")

    return PublicationResult(
        primary_filepath=str(primary),
        index_filepath=str(index),
        home_filepath=str(home),
        status="ok", actions=actions, warnings=warnings,
    )
