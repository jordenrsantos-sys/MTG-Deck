"""Failure-mode coverage for /import/url — Phase 6 Stage 6 Stage 4.

Six simulated upstream failure modes verifying the structured FETCH_ERROR
+ reason_code mapping landed in import_url_v1 Stage 2:

  1. Cloudflare 429 challenge (Server: cloudflare + "Just a moment" body)
     → reason_code: cloudflare_challenge
  2. Generic 429 rate-limit (no Cloudflare signature)
     → reason_code: rate_limited
  3. 503 service unavailable
     → reason_code: upstream_5xx
  4. Network timeout (urllib.error.URLError wrapping TimeoutError)
     → reason_code: upstream_timeout
  5. DNS failure / connection refused (urllib.error.URLError; non-timeout)
     → reason_code: network_error
  6. Non-JSON response (HTML challenge body parses as JSON ValueError)
     → reason_code: non_json_response (or empty_response when body empty)
  7. Empty 200 body
     → reason_code: empty_response

Per autonomous_repair_log soft-safety #4: uses unittest.mock to inject a
custom fetcher into `import_from_url(...)`'s `fetcher=` kwarg — no
httpx.MockTransport dep required. The fetcher kwarg has been the test
seam since Engine-4A landed; this file leverages it directly.

These tests document the URL importer's expected behavior under upstream
failure — protects against silent degradation in production.
"""
from __future__ import annotations

import io
import json
import socket
import sys
import urllib.error
import urllib.request
import unittest
from email.message import Message
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from api.import_url_v1 import (  # noqa: E402
    REASON_CLOUDFLARE_CHALLENGE,
    REASON_EMPTY_RESPONSE,
    REASON_NETWORK_ERROR,
    REASON_NON_JSON_RESPONSE,
    REASON_RATE_LIMITED,
    REASON_UPSTREAM_5XX,
    REASON_UPSTREAM_TIMEOUT,
    STATUS_FETCH_ERROR,
    STATUS_OK,
    import_from_url,
)


CANONICAL_ARCHIDEKT_URL = "https://archidekt.com/decks/1010839/"


def _make_http_error(code: int, body: bytes = b"", headers: dict | None = None) -> urllib.error.HTTPError:
    """Construct a urllib HTTPError with the given status, body, and headers."""
    msg = Message()
    for k, v in (headers or {}).items():
        msg[k] = v
    err = urllib.error.HTTPError(
        url=CANONICAL_ARCHIDEKT_URL,
        code=code,
        msg=f"HTTP {code}",
        hdrs=msg,  # type: ignore[arg-type]
        fp=io.BytesIO(body),
    )
    return err


class ImportUrlFailureModeTests(unittest.TestCase):
    """Each test injects a fetcher that raises the simulated upstream condition."""

    def test_cloudflare_429_challenge_mapped_to_cloudflare_challenge(self) -> None:
        cf_body = b"<html><head><title>Just a moment...</title></head></html>"
        cf_headers = {"Server": "cloudflare", "Content-Type": "text/html"}

        def fetcher(url: str):
            raise _make_http_error(429, body=cf_body, headers=cf_headers)

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_FETCH_ERROR)
        self.assertEqual(envelope["reason_code"], REASON_CLOUDFLARE_CHALLENGE)
        self.assertEqual(envelope["source"], "archidekt")

    def test_generic_429_without_cf_signature_mapped_to_rate_limited(self) -> None:
        def fetcher(url: str):
            raise _make_http_error(429, body=b"{}", headers={"Server": "nginx"})

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_FETCH_ERROR)
        self.assertEqual(envelope["reason_code"], REASON_RATE_LIMITED)

    def test_503_service_unavailable_mapped_to_upstream_5xx(self) -> None:
        def fetcher(url: str):
            raise _make_http_error(503, body=b"")

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_FETCH_ERROR)
        self.assertEqual(envelope["reason_code"], REASON_UPSTREAM_5XX)

    def test_500_internal_server_error_also_mapped_to_upstream_5xx(self) -> None:
        def fetcher(url: str):
            raise _make_http_error(500, body=b"")

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["reason_code"], REASON_UPSTREAM_5XX)

    def test_network_timeout_mapped_to_upstream_timeout(self) -> None:
        def fetcher(url: str):
            raise urllib.error.URLError(reason=TimeoutError("connection timed out"))

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_FETCH_ERROR)
        self.assertEqual(envelope["reason_code"], REASON_UPSTREAM_TIMEOUT)

    def test_socket_timeout_via_oserror_mapped_to_upstream_timeout(self) -> None:
        def fetcher(url: str):
            raise urllib.error.URLError(reason=OSError("[Errno 60] Operation timed out"))

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["reason_code"], REASON_UPSTREAM_TIMEOUT)

    def test_dns_failure_mapped_to_network_error(self) -> None:
        def fetcher(url: str):
            raise urllib.error.URLError(reason=socket.gaierror(11001, "getaddrinfo failed"))

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_FETCH_ERROR)
        self.assertEqual(envelope["reason_code"], REASON_NETWORK_ERROR)

    def test_connection_refused_mapped_to_network_error(self) -> None:
        def fetcher(url: str):
            raise urllib.error.URLError(reason=ConnectionRefusedError("[Errno 111] Connection refused"))

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["reason_code"], REASON_NETWORK_ERROR)

    def test_non_json_response_mapped_to_non_json_response(self) -> None:
        def fetcher(url: str):
            # JSON parser raises ValueError for HTML / text / etc.
            raise ValueError("Expecting value: line 1 column 1 (char 0)")

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_FETCH_ERROR)
        # "Expecting value" is the empty-body signature; "non-json" is the
        # non-empty-junk signature. Either is acceptable per spec.
        self.assertIn(envelope["reason_code"], (REASON_EMPTY_RESPONSE, REASON_NON_JSON_RESPONSE))

    def test_genuinely_garbage_response_mapped_to_non_json_response(self) -> None:
        def fetcher(url: str):
            raise ValueError("Invalid control character at: line 5 column 12 (char 89)")

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["reason_code"], REASON_NON_JSON_RESPONSE)

    def test_empty_response_mapped_to_empty_response(self) -> None:
        def fetcher(url: str):
            # json.loads("") raises ValueError("Expecting value: line 1 column 1 (char 0)")
            raise json.JSONDecodeError("Expecting value", "", 0)

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["reason_code"], REASON_EMPTY_RESPONSE)

    def test_oserror_outside_urlerror_mapped_to_network_error(self) -> None:
        """Catch-all for unexpected network-layer failures (broken pipe, etc.)."""
        def fetcher(url: str):
            raise BrokenPipeError("Broken pipe")

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_FETCH_ERROR)
        self.assertEqual(envelope["reason_code"], REASON_NETWORK_ERROR)

    def test_ok_path_unaffected_by_failure_mode_handlers(self) -> None:
        """Sanity: the new error-classification logic doesn't break the OK path."""
        def fetcher(url: str):
            return {
                "cards": [
                    {
                        "quantity": 1,
                        "categories": ["Commander"],
                        "card": {"oracleCard": {"uid": "test-shelob-uuid", "name": "Shelob, Child of Ungoliant"}},
                    },
                    {
                        "quantity": 1,
                        "categories": [],
                        "card": {"oracleCard": {"uid": "test-sol-ring-uuid", "name": "Sol Ring"}},
                    },
                ],
            }

        envelope = import_from_url(CANONICAL_ARCHIDEKT_URL, fetcher=fetcher)
        self.assertEqual(envelope["status"], STATUS_OK)
        self.assertEqual(envelope["reason_code"], None)
        self.assertEqual(envelope["source"], "archidekt")
        self.assertEqual(envelope["deck"]["commander"]["name"], "Shelob, Child of Ungoliant")
        self.assertEqual(len(envelope["deck"]["cards"]), 1)


if __name__ == "__main__":
    unittest.main()
