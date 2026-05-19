"""Regression test: _http_request must NOT retry POSTs on TimeoutError.

Background — the 2026-05-18 Deadpool duplication incident:
the orchestrator submitted one logical batch ingest for Deadpool, the
server-side vectorization exceeded the client's 60s HTTP_TIMEOUT, and the
retry loop in _http_request fired 4 more POSTs. The engine processed each
retry as a fresh ingest call, writing 5 duplicate batches (75 corpus entries
for one logical attempt). After 5 attempts the client raised TimeoutError
and the orchestrator logged FAILED — but the corpus was already polluted.

The fix: POSTs are non-idempotent. On TimeoutError the server may have
completed the request and the retry would duplicate side effects, so the
exception must bubble up immediately. GETs and 429-backoff retries are
unaffected.
"""
from __future__ import annotations

import importlib.util
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch


def _load_orchestrator_module():
    """Import tools/bulk_corpus_ingest.py as a module (it lives outside packages)."""
    repo = Path(__file__).resolve().parents[1]
    path = repo / "tools" / "bulk_corpus_ingest.py"
    spec = importlib.util.spec_from_file_location("bulk_corpus_ingest_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bulk_corpus_ingest_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


class HttpRequestRetryPolicyTest(unittest.TestCase):
    """Validates the method-aware retry policy in _http_request."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = _load_orchestrator_module()

    def test_post_does_not_retry_on_timeout_error(self) -> None:
        """A POST that raises TimeoutError must bubble up on the FIRST attempt
        (no retry). This is the Deadpool regression: retrying a POST after a
        timeout duplicates non-idempotent server-side work."""
        call_count = {"n": 0}

        def fake_urlopen(req, timeout):  # noqa: ARG001
            call_count["n"] += 1
            raise TimeoutError("timed out")

        with patch.object(self.mod.urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(TimeoutError):
                self.mod._http_request(
                    "http://localhost:8000/corpus/batch_ingest_v1",
                    method="POST",
                    body={"x": 1},
                )

        self.assertEqual(
            call_count["n"], 1,
            f"POST timeout must NOT retry — got {call_count['n']} attempts. "
            f"This is the Deadpool regression."
        )

    def test_post_does_not_retry_when_urlerror_wraps_timeout(self) -> None:
        """urllib often wraps the underlying socket timeout in URLError.
        The fix unwraps `URLError(reason=TimeoutError(...))` and refuses to
        retry POSTs in that shape too."""
        call_count = {"n": 0}

        def fake_urlopen(req, timeout):  # noqa: ARG001
            call_count["n"] += 1
            raise urllib.error.URLError(TimeoutError("timed out"))

        with patch.object(self.mod.urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(urllib.error.URLError):
                self.mod._http_request(
                    "http://localhost:8000/corpus/batch_ingest_v1",
                    method="POST",
                    body={"x": 1},
                )

        self.assertEqual(call_count["n"], 1,
                         "URLError-wrapped TimeoutError on POST must not retry")

    def test_idempotent_post_does_retry_on_timeout(self) -> None:
        """`idempotent=True` opts a POST back into timeout-retry behavior.
        Used by callers that can guarantee no duplicate side effects
        (e.g. the version-ping that submits an empty entries list).
        Side effect: a cold engine first-call may take >60s while loading
        the corpus + warming vectorizers; without retry, the orchestrator
        cannot start. The retry is safe because the request body has no
        side effect on the server (empty payload returns version, 0 accepted)."""
        call_count = {"n": 0}
        max_retries = 3

        def fake_urlopen(req, timeout):  # noqa: ARG001
            call_count["n"] += 1
            raise TimeoutError("timed out")

        with patch.object(self.mod.urllib.request, "urlopen", fake_urlopen), \
             patch.object(self.mod.time, "sleep", lambda _s: None):
            with self.assertRaises(TimeoutError):
                self.mod._http_request(
                    "http://localhost:8000/corpus/batch_ingest_v1",
                    method="POST",
                    body={"entries": []},
                    max_retries=max_retries,
                    idempotent=True,
                )

        self.assertEqual(call_count["n"], max_retries,
                         f"idempotent POST should retry on timeout — got {call_count['n']} attempts")

    def test_get_still_retries_on_timeout_error(self) -> None:
        """GETs are idempotent — retry-on-timeout is safe and useful.
        After MAX_HTTP_RETRIES attempts the final exception should bubble."""
        call_count = {"n": 0}
        max_retries = 3

        def fake_urlopen(req, timeout):  # noqa: ARG001
            call_count["n"] += 1
            raise TimeoutError("timed out")

        with patch.object(self.mod.urllib.request, "urlopen", fake_urlopen), \
             patch.object(self.mod.time, "sleep", lambda _s: None):
            with self.assertRaises(TimeoutError):
                self.mod._http_request(
                    "http://localhost:8000/some/get/endpoint",
                    method="GET",
                    max_retries=max_retries,
                )

        self.assertEqual(call_count["n"], max_retries,
                         f"GET should still retry on timeout — got {call_count['n']} attempts (expected {max_retries})")


if __name__ == "__main__":
    unittest.main()
