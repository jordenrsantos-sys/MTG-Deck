"""URL importer registry + dispatch (Engine-4A).

Server-side CORS proxy + per-source parser registry feeding the
`POST /import/url` endpoint surfaced in Phase 4.1 OQ-4 (resolution: server-
side proxy bypasses browser CORS for moxfield/archidekt/edhrec/etc.).

Per Phase 5a.2.1 documented Cloudflare-pivot history (in
`05_VALIDATION/EXTERNAL_DECK_CORPUS_DESIGN.md`):
- **Archidekt**: `ACTIVE` — clean public API verified during 5a.2.1; embeds
  Scryfall oracle UUIDs at `cards[].card.oracleCard.uid`.
- **Moxfield**: `DEFERRED` — Cloudflare bot-protection challenge HTML
  returned for every endpoint via urllib (5a.2.1 pivot finding).
- **EDHREC**: `DEFERRED_REACHABILITY_UNVERIFIED` — Engine-4A's reachability
  probe was permission-blocked in this environment; production ops must
  verify before enabling.

Per Engine-4A autonomous_repair_log soft-safety #1: deferred sources stay in
the registry as scaffolds with `enabled=False`; the API surface ships intact
(callers get a structured "DEFERRED" envelope rather than a 404).

Per soft-safety #3: Phase 5a Archidekt extractor is calibration-only (under
`tools/update_external_deck_corpus.py`); Engine-4A authors a fresh runtime
parser here per [[CALIBRATION_BOUNDARY]] safety #3 (runtime modules MUST NOT
import calibration code paths).

Per soft-safety #4: name → oracle_id resolution misses surface in
`unknowns[]` rather than failing the whole import (Spellbook drop-on-partial
pattern from `tools/update_combo_packs_from_spellbook.py:142`).

Output envelope shape (matches Phase 4 spec):

    {
        "version": "import_url_v1",
        "status": "OK" | "DEFERRED" | "PARSE_ERROR" | "INVALID_REQUEST",
        "source": "archidekt" | "moxfield" | "edhrec" | "unknown",
        "deck": {
            "commander": {"name": str, "oracle_id": str | None} | None,
            "cards": [{"name": str, "count": int, "oracle_id": str | None}, ...],
            "format": "commander",
        } | None,
        "unknowns": [{"name": str, "reason": str}, ...],
        "reason_code": str | None,
        "deferred_reason": str | None,
    }

This module is RUNTIME (not calibration). It reads from runtime card DB
helpers; it does NOT read calibration packs (external_decks_v1 /
pilot_personalities_v1 / playtest_changelog_v1).
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

IMPORT_URL_V1_VERSION = "import_url_v1"

# Phase 6 Stage 6 Stage 2: raised from 15→30 sec to better tolerate
# Cloudflare-protected upstreams' challenge-response delays. A 30sec wall
# is generous for the OK path (Archidekt typically responds in <1 sec) but
# bounds the pathological DNS-failure / hung-connection cases.
DEFAULT_REQUEST_TIMEOUT_SEC = 30
DEFAULT_USER_AGENT = "mtg-engine-import-url/1.0"

STATUS_OK = "OK"
STATUS_DEFERRED = "DEFERRED"
STATUS_PARSE_ERROR = "PARSE_ERROR"
STATUS_INVALID_REQUEST = "INVALID_REQUEST"
STATUS_FETCH_ERROR = "FETCH_ERROR"

REASON_UNKNOWN_SOURCE = "unknown_source"
REASON_INVALID_URL = "invalid_url"
REASON_NO_DECK_ID = "no_deck_id_in_url"
REASON_HTTP_ERROR = "http_error"
REASON_BAD_RESPONSE_SHAPE = "response_shape_mismatch"
REASON_SOURCE_DEFERRED = "source_deferred"
# Phase 6 Stage 6 Stage 2: structured upstream-failure reason codes for
# graceful degradation. Each maps to a distinct telemetry signal so ops
# can distinguish "upstream is down" from "upstream is rate-limiting us"
# from "upstream returned junk". Stage 4 mocked failure-mode tests
# verify each path.
REASON_UPSTREAM_TIMEOUT = "upstream_timeout"
REASON_UPSTREAM_5XX = "upstream_5xx"
REASON_RATE_LIMITED = "rate_limited"
REASON_CLOUDFLARE_CHALLENGE = "cloudflare_challenge"
REASON_NETWORK_ERROR = "network_error"
REASON_NON_JSON_RESPONSE = "non_json_response"
REASON_EMPTY_RESPONSE = "empty_response"


@dataclass(frozen=True)
class SourceSpec:
    """Catalog entry for one import source."""
    name: str
    enabled: bool
    deferred_reason: Optional[str]
    url_pattern: re.Pattern
    fetch_url_template: str  # `{deck_id}` substituted in
    parser: Callable[[Dict[str, Any]], Dict[str, Any]]


def _parse_archidekt(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Parse an Archidekt API response into the normalized deck envelope.

    Archidekt shape (verified during Phase 5a.2.1; documented in
    EXTERNAL_DECK_CORPUS_DESIGN.md): `{cards: [{quantity, categories: [str],
    card: {oracleCard: {uid: <oracle_uuid>, name: str}}}, ...]}` with
    `categories: ["Commander"]` flagging the commander slot.

    NOTE: Engine-4A authors this parser fresh (calibration-boundary safety
    #3); the Phase 5a `tools/update_external_deck_corpus.py` Archidekt
    extractor is calibration-only and not reusable at runtime.
    """
    cards_raw = payload.get("cards") if isinstance(payload, dict) else None
    if not isinstance(cards_raw, list):
        return {
            "deck": None,
            "unknowns": [],
            "parse_error": "archidekt response missing cards[] array",
        }

    parsed_cards: List[Dict[str, Any]] = []
    commander: Optional[Dict[str, Any]] = None
    unknowns: List[Dict[str, str]] = []

    for entry in cards_raw:
        if not isinstance(entry, dict):
            continue
        oracle_card = (entry.get("card") or {}).get("oracleCard") or {}
        name = oracle_card.get("name")
        oracle_id = oracle_card.get("uid")
        quantity = entry.get("quantity") or 1
        categories = entry.get("categories") or []
        if not isinstance(name, str) or name == "":
            unknowns.append({
                "name": "<missing_name>",
                "reason": "archidekt entry without card.oracleCard.name",
            })
            continue
        is_commander = isinstance(categories, list) and "Commander" in categories
        record = {
            "name": name,
            "count": int(quantity) if isinstance(quantity, int) else 1,
            "oracle_id": oracle_id if isinstance(oracle_id, str) and oracle_id else None,
        }
        if is_commander:
            commander = record
        else:
            parsed_cards.append(record)

    deck = {
        "commander": commander,
        "cards": parsed_cards,
        "format": "commander",
    }
    return {"deck": deck, "unknowns": unknowns, "parse_error": None}


def _parse_deferred_stub(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Stub parser for DEFERRED sources (called only if accidentally invoked)."""
    return {
        "deck": None,
        "unknowns": [],
        "parse_error": "source is DEFERRED; parser stub returned no deck",
    }


# Source registry. Patterns are compiled at module load.
_ARCHIDEKT_PATTERN = re.compile(
    r"https?://(?:www\.)?archidekt\.com/decks/(?P<deck_id>\d+)/?"
)
_MOXFIELD_PATTERN = re.compile(
    r"https?://(?:www\.)?moxfield\.com/decks/(?P<deck_id>[A-Za-z0-9_-]+)/?"
)
_EDHREC_PATTERN = re.compile(
    r"https?://(?:www\.)?edhrec\.com/.*"
)


SOURCE_REGISTRY: Dict[str, SourceSpec] = {
    "archidekt": SourceSpec(
        name="archidekt",
        enabled=True,
        deferred_reason=None,
        url_pattern=_ARCHIDEKT_PATTERN,
        fetch_url_template="https://archidekt.com/api/decks/{deck_id}/",
        parser=_parse_archidekt,
    ),
    "moxfield": SourceSpec(
        name="moxfield",
        enabled=False,
        deferred_reason=(
            "moxfield_cloudflare_gated_5a_2_1_pivot — urllib cannot pass "
            "the Cloudflare bot-protection challenge; needs cloudscraper or "
            "browser-impersonation tooling (separate sub-task)"
        ),
        url_pattern=_MOXFIELD_PATTERN,
        fetch_url_template="https://api.moxfield.com/v2/decks/all/{deck_id}",
        parser=_parse_deferred_stub,
    ),
    "edhrec": SourceSpec(
        name="edhrec",
        enabled=False,
        deferred_reason=(
            "edhrec_reachability_unverified_engine_4a — production ops must "
            "verify reachability + parser feasibility before enabling"
        ),
        url_pattern=_EDHREC_PATTERN,
        fetch_url_template="https://json.edhrec.com{path}",
        parser=_parse_deferred_stub,
    ),
}


def _identify_source(url: str) -> Optional[Tuple[SourceSpec, Dict[str, str]]]:
    """Match `url` against each source pattern; return (spec, named-groups)."""
    if not isinstance(url, str) or url == "":
        return None
    for spec in SOURCE_REGISTRY.values():
        m = spec.url_pattern.match(url)
        if m:
            return spec, m.groupdict()
    return None


def _envelope(
    *,
    status: str,
    source: str = "unknown",
    deck: Optional[Dict[str, Any]] = None,
    unknowns: Optional[List[Dict[str, str]]] = None,
    reason_code: Optional[str] = None,
    deferred_reason: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "version": IMPORT_URL_V1_VERSION,
        "status": status,
        "source": source,
        "deck": deck,
        "unknowns": list(unknowns or []),
        "reason_code": reason_code,
        "deferred_reason": deferred_reason,
    }


def _default_fetcher(url: str) -> Dict[str, Any]:
    """Default urllib-based fetcher used by `import_from_url` when no fetcher
    is injected. Production callers (FastAPI route) inject a fetcher with the
    right timeout / user-agent / proxy settings; tests inject a stub.

    Returns the parsed JSON dict OR raises urllib.error.URLError /
    urllib.error.HTTPError on transport failure.
    """
    req = urllib.request.Request(
        url,
        headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=DEFAULT_REQUEST_TIMEOUT_SEC) as resp:
        body = resp.read()
    return json.loads(body.decode("utf-8"))


def import_from_url(
    url: str,
    *,
    fetcher: Optional[Callable[[str], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Server-side CORS-proxy importer.

    1. Identify source from URL pattern.
    2. If source is DEFERRED, return STATUS_DEFERRED envelope without fetching.
    3. Construct the fetch URL via the source's template.
    4. Call the injected fetcher (or default urllib) to retrieve JSON.
    5. Dispatch to the source's parser; return normalized envelope.

    `fetcher` defaults to `_default_fetcher`; tests inject a stub to avoid
    real HTTP calls.
    """
    if not isinstance(url, str) or url == "":
        return _envelope(
            status=STATUS_INVALID_REQUEST,
            reason_code=REASON_INVALID_URL,
        )

    matched = _identify_source(url)
    if matched is None:
        return _envelope(
            status=STATUS_INVALID_REQUEST,
            reason_code=REASON_UNKNOWN_SOURCE,
        )
    spec, groups = matched

    if not spec.enabled:
        return _envelope(
            status=STATUS_DEFERRED,
            source=spec.name,
            reason_code=REASON_SOURCE_DEFERRED,
            deferred_reason=spec.deferred_reason,
        )

    deck_id = groups.get("deck_id")
    if not deck_id:
        return _envelope(
            status=STATUS_INVALID_REQUEST,
            source=spec.name,
            reason_code=REASON_NO_DECK_ID,
        )
    fetch_url = spec.fetch_url_template.format(deck_id=deck_id)

    fetcher_fn = fetcher if fetcher is not None else _default_fetcher
    try:
        payload = fetcher_fn(fetch_url)
    except urllib.error.HTTPError as e:
        # Phase 6 Stage 6 Stage 2: classify HTTP error types into structured
        # reason codes. 429 → rate_limited (or cloudflare_challenge if the
        # response signature matches); 5xx → upstream_5xx; everything else
        # falls through to http_error_<code> for unknown upstream behavior.
        body_snippet = ""
        try:
            body_snippet = (e.read() or b"")[:512].decode("utf-8", errors="replace")
        except Exception:
            body_snippet = ""
        cf_signature = (
            getattr(e, "headers", None) is not None
            and (e.headers.get("Server") or "").lower().startswith("cloudflare")
            and "Just a moment" in body_snippet
        )
        if e.code == 429 and cf_signature:
            reason_code = REASON_CLOUDFLARE_CHALLENGE
        elif e.code == 429:
            reason_code = REASON_RATE_LIMITED
        elif 500 <= e.code <= 599:
            reason_code = REASON_UPSTREAM_5XX
        else:
            reason_code = f"{REASON_HTTP_ERROR}_{e.code}"
        return _envelope(
            status=STATUS_FETCH_ERROR,
            source=spec.name,
            reason_code=reason_code,
            deferred_reason=f"HTTP {e.code}",
        )
    except urllib.error.URLError as e:
        # Phase 6 Stage 6 Stage 2: URLError covers DNS failures + socket
        # timeouts + connection refused. Inspect the underlying reason to
        # distinguish timeout from generic network error.
        underlying = getattr(e, "reason", None)
        # socket.timeout is a TimeoutError subclass on Py 3.10+; the
        # urllib.urlopen `timeout=` param raises it as the URLError.reason.
        is_timeout = isinstance(underlying, TimeoutError) or (
            isinstance(underlying, OSError) and "timed out" in str(underlying).lower()
        )
        reason_code = REASON_UPSTREAM_TIMEOUT if is_timeout else REASON_NETWORK_ERROR
        return _envelope(
            status=STATUS_FETCH_ERROR,
            source=spec.name,
            reason_code=reason_code,
            deferred_reason=str(e)[:200],
        )
    except (ValueError, json.JSONDecodeError) as e:
        # Phase 6 Stage 6 Stage 2: non-JSON response (Cloudflare challenge
        # HTML page, empty body parsed as ""→ValueError, etc.). Distinguish
        # empty body from junk-content explicitly so ops can tell rate-
        # limiting from genuinely-empty responses.
        msg = str(e).lower()
        if "expecting value" in msg or "empty" in msg or "no json" in msg:
            reason_code = REASON_EMPTY_RESPONSE
        else:
            reason_code = REASON_NON_JSON_RESPONSE
        return _envelope(
            status=STATUS_FETCH_ERROR,
            source=spec.name,
            reason_code=reason_code,
            deferred_reason=str(e)[:200],
        )
    except OSError as e:
        # Catch-all for unexpected network-layer failures (connection reset,
        # broken pipe, etc.); separate from URLError above.
        return _envelope(
            status=STATUS_FETCH_ERROR,
            source=spec.name,
            reason_code=REASON_NETWORK_ERROR,
            deferred_reason=str(e)[:200],
        )

    parsed = spec.parser(payload)
    if parsed.get("parse_error"):
        return _envelope(
            status=STATUS_PARSE_ERROR,
            source=spec.name,
            reason_code=REASON_BAD_RESPONSE_SHAPE,
            deferred_reason=parsed["parse_error"],
        )

    return _envelope(
        status=STATUS_OK,
        source=spec.name,
        deck=parsed["deck"],
        unknowns=parsed["unknowns"],
    )


def list_supported_sources() -> List[Dict[str, Any]]:
    """Return per-source enabled/deferred status for documentation surfaces."""
    return [
        {
            "name": spec.name,
            "enabled": spec.enabled,
            "deferred_reason": spec.deferred_reason,
            "url_pattern_str": spec.url_pattern.pattern,
        }
        for spec in sorted(SOURCE_REGISTRY.values(), key=lambda s: s.name)
    ]
