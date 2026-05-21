"""Mega-task v5 Phase 1: launch.py worker-resolution unit tests.

Verifies the MTG_ENGINE_API_WORKERS env-var override + default behavior
that drives ``uvicorn ... --workers N`` selection. The launcher itself is
exercised end-to-end at session-start time; these tests catch regressions
in the resolution helper without spinning up a real uvicorn server.
"""
from __future__ import annotations

import os
import unittest
from unittest import mock

import launch


class ResolveApiWorkersTests(unittest.TestCase):
    def test_default_when_env_unset(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(launch.API_WORKERS_ENV, None)
            self.assertEqual(launch._resolve_api_workers(), launch.DEFAULT_API_WORKERS)
            self.assertGreaterEqual(launch.DEFAULT_API_WORKERS, 2)

    def test_env_override_int(self) -> None:
        with mock.patch.dict(os.environ, {launch.API_WORKERS_ENV: "4"}, clear=False):
            self.assertEqual(launch._resolve_api_workers(), 4)

    def test_env_override_one(self) -> None:
        with mock.patch.dict(os.environ, {launch.API_WORKERS_ENV: "1"}, clear=False):
            self.assertEqual(launch._resolve_api_workers(), 1)

    def test_env_garbage_falls_back_to_default(self) -> None:
        with mock.patch.dict(os.environ, {launch.API_WORKERS_ENV: "not-an-int"}, clear=False):
            self.assertEqual(launch._resolve_api_workers(), launch.DEFAULT_API_WORKERS)

    def test_env_zero_clamps_to_one(self) -> None:
        with mock.patch.dict(os.environ, {launch.API_WORKERS_ENV: "0"}, clear=False):
            self.assertEqual(launch._resolve_api_workers(), 1)

    def test_env_negative_clamps_to_one(self) -> None:
        with mock.patch.dict(os.environ, {launch.API_WORKERS_ENV: "-3"}, clear=False):
            self.assertEqual(launch._resolve_api_workers(), 1)

    def test_env_whitespace_falls_back_to_default(self) -> None:
        with mock.patch.dict(os.environ, {launch.API_WORKERS_ENV: "   "}, clear=False):
            self.assertEqual(launch._resolve_api_workers(), launch.DEFAULT_API_WORKERS)


if __name__ == "__main__":
    unittest.main()
