"""Mega-task v3 Phase 7 — obsidian_new_set_writer tests.

Verifies:
  - compose_set_report_payload produces deterministic filepath + frontmatter
  - Filename slug strips special characters
  - publish_via_mcp dispatches the right calls in order
  - publish_via_mcp handles missing dispatch (skip + warning)
  - publish_via_filesystem writes file + index + home correctly
  - filesystem fallback is idempotent on re-publish (dedupes index/home)
"""
from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

from api.engine.integrations.obsidian_new_set_writer_v1 import (
    McpDispatch,
    NEW_SETS_DIR,
    NEW_SETS_INDEX,
    HOME_PATH,
    PublicationPlan,
    compose_set_report_payload,
    publish_via_filesystem,
    publish_via_mcp,
)


@dataclass
class _FakeEnvelope:
    set_code: str = "tst"
    set_name: str = "Test Set"
    released_at: str = "2026-05-01"
    processed_at: str = "2026-05-01T12:00:00+00:00"
    card_count: int = 10
    markdown: str = "## Set overview\n\nFake report content."


class ComposeTests(unittest.TestCase):
    def test_filepath_includes_date_code_slug(self) -> None:
        plan = compose_set_report_payload(_FakeEnvelope())
        self.assertEqual(plan.primary_filepath,
                         f"{NEW_SETS_DIR}/2026-05-01_tst_test-set.md")

    def test_frontmatter_present(self) -> None:
        plan = compose_set_report_payload(_FakeEnvelope())
        self.assertTrue(plan.primary_content.startswith("---\n"))
        self.assertIn("set_code: tst", plan.primary_content)
        self.assertIn("released_at: 2026-05-01", plan.primary_content)
        self.assertIn("tags: [new-set, automation]", plan.primary_content)

    def test_includes_markdown_after_frontmatter(self) -> None:
        plan = compose_set_report_payload(_FakeEnvelope())
        self.assertIn("Fake report content.", plan.primary_content)

    def test_slug_strips_punctuation(self) -> None:
        env = _FakeEnvelope(set_name="Spider-Man's Big Adventure!")
        plan = compose_set_report_payload(env)
        self.assertIn("spider-man-s-big-adventure", plan.primary_filepath)

    def test_index_and_home_lines_match(self) -> None:
        plan = compose_set_report_payload(_FakeEnvelope())
        self.assertEqual(plan.index_filepath, NEW_SETS_INDEX)
        self.assertEqual(plan.home_filepath, HOME_PATH)
        # Index line + home line should be the same wikilink format.
        self.assertEqual(plan.index_append_line, plan.home_append_line)


class PublishViaMcpTests(unittest.TestCase):
    def test_dispatches_primary_index_and_home(self) -> None:
        append_mock = MagicMock()
        patch_mock = MagicMock()
        mcp = McpDispatch(append_content=append_mock, patch_content=patch_mock)
        result = publish_via_mcp(_FakeEnvelope(), mcp)
        # 2 append calls (primary + index) + 1 patch call (home).
        self.assertEqual(append_mock.call_count, 2)
        self.assertEqual(patch_mock.call_count, 1)
        self.assertEqual(result.status, "ok")

    def test_missing_dispatch_falls_back(self) -> None:
        mcp = McpDispatch(append_content=None, patch_content=None)
        result = publish_via_mcp(_FakeEnvelope(), mcp)
        # No append → primary skipped → status failed (no actions).
        self.assertEqual(result.status, "failed")
        self.assertTrue(any("dispatch missing" in w for w in result.warnings))

    def test_home_patch_failure_falls_back_to_append(self) -> None:
        append_mock = MagicMock()
        patch_mock = MagicMock(side_effect=RuntimeError("heading not found"))
        mcp = McpDispatch(append_content=append_mock, patch_content=patch_mock)
        result = publish_via_mcp(_FakeEnvelope(), mcp)
        # 3 appends (primary + index + home-fallback with header).
        self.assertEqual(append_mock.call_count, 3)
        self.assertTrue(any("home patch failed" in w for w in result.warnings))


class PublishViaFilesystemTests(unittest.TestCase):
    def test_writes_primary_index_home_to_vault(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result = publish_via_filesystem(_FakeEnvelope(), td)
            primary = Path(result.primary_filepath)
            index = Path(result.index_filepath)
            home = Path(result.home_filepath)
            self.assertTrue(primary.is_file())
            self.assertTrue(index.is_file())
            self.assertTrue(home.is_file())
            # Primary content has frontmatter + markdown.
            content = primary.read_text(encoding="utf-8")
            self.assertIn("set_code: tst", content)
            self.assertIn("Fake report content", content)

    def test_idempotent_index_and_home(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            publish_via_filesystem(_FakeEnvelope(), td)
            publish_via_filesystem(_FakeEnvelope(), td)
            # Each entry should appear exactly once (count the wikilink
            # token, which is unique per entry).
            idx_path = Path(td) / NEW_SETS_INDEX
            content = idx_path.read_text(encoding="utf-8")
            self.assertEqual(content.count("[[2026-05-01_tst_test-set"), 1)
            home_path = Path(td) / HOME_PATH
            home_content = home_path.read_text(encoding="utf-8")
            self.assertEqual(home_content.count("[[2026-05-01_tst_test-set"), 1)

    def test_creates_section_in_existing_home(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            # Pre-populate Home.md WITHOUT the "Recent set releases" section.
            home = Path(td) / HOME_PATH
            home.parent.mkdir(parents=True, exist_ok=True)
            home.write_text("# Home\n\nExisting content.", encoding="utf-8")
            publish_via_filesystem(_FakeEnvelope(), td)
            content = home.read_text(encoding="utf-8")
            self.assertIn("## Recent set releases", content)
            self.assertIn("Existing content.", content)


if __name__ == "__main__":
    unittest.main()
