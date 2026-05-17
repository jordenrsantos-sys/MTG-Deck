#!/usr/bin/env python3
"""Phase 5a bulk corpus ingest orchestrator.

Reads top-N commanders from EDHREC, runs the 16-entry sweep workflow per
commander (1 precon if exists + 5 per-bracket averages + 10 ranked top-2),
ingests via /corpus/batch_ingest_v1, captures commander strategy notes,
auto-commits every M commanders. Resumable via JSONL progress file.

CLI:
  python tools/bulk_corpus_ingest.py [--count N=250] [--commit-every M=10]
                                     [--start-rank K=1] [--dry-run]
                                     [--commander-slugs slug1,slug2,...]
                                     [--retry-failed] [--no-git]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Config
# ============================================================

ENGINE_HOST = "http://localhost:8000"
EDHREC_HTML = "https://edhrec.com"
EDHREC_JSON = "https://json.edhrec.com/pages"
DB_SNAPSHOT_ID = "20260217_190902_tagpass_20260222"
EXPECTED_INGEST_VERSION = "corpus_batch_ingest_v1.5_spellbook_brackets"
BRACKET_SLUGS = ["exhibition", "core", "upgraded", "optimized", "cedh"]
BRACKET_CLAIMS = {
    "exhibition": "B1", "core": "B2", "upgraded": "B3",
    "optimized": "B4", "cedh": "B5",
}
MIN_AVG_SAMPLE = 50
DEFAULT_TOP_N = 250
DEFAULT_COMMIT_EVERY = 10
EDHREC_REQUEST_DELAY = 1.5
ENGINE_REQUEST_DELAY = 0.5
HTTP_TIMEOUT = 60
MAX_HTTP_RETRIES = 5
BRIEF_WARMUP_RETRIES = 5
PER_COMMANDER_DEADLINE_SECONDS = 1200  # 20-minute halt criterion
USER_AGENT = "mtg-engine-bulk-ingest/1.0"

# Paths
REPO_ROOT = Path(__file__).resolve().parents[1]  # tools/THIS -> repo/
CORPUS_DIR = REPO_ROOT / "api" / "engine" / "data" / "corpus"
PROGRESS_PATH = CORPUS_DIR / "_phase5a_progress.jsonl"
STRATEGY_NOTES_PATH = CORPUS_DIR / "commander_strategy_notes_v1.json"
CORPUS_V1_PATH = CORPUS_DIR / "corpus_v1.json"


# ============================================================
# Logging
# ============================================================

def _log(msg: str) -> None:
    timestamp = datetime.utcnow().strftime("%H:%M:%SZ")
    print(f"[{timestamp}] {msg}", flush=True)


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================
# HTTP helpers (with 429 backoff)
# ============================================================

def _http_request(
    url: str,
    *,
    method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
    timeout: int = HTTP_TIMEOUT,
    max_retries: int = MAX_HTTP_RETRIES,
) -> Tuple[int, str]:
    """Return (status_code, body_text). Retries on 429 with exponential backoff."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json,text/html"}
    data_bytes: Optional[bytes] = None
    if body is not None:
        data_bytes = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url=url, data=data_bytes, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < max_retries - 1:
                retry_after = 0
                try:
                    retry_after = int(exc.headers.get("Retry-After", "0") or "0")
                except (TypeError, ValueError):
                    retry_after = 0
                wait_s = retry_after if retry_after > 0 else min(300, 30 * (2 ** attempt))
                _log(f"  HTTP 429 from {url[:80]}; backing off {wait_s}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait_s)
                continue
            return exc.code, ""
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait_s = min(60, 2 ** attempt)
                _log(f"  network error {exc!r}; retrying in {wait_s}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait_s)
                continue
            raise
    if last_exc is not None:
        raise last_exc
    return 0, ""


def _http_get_json(url: str) -> Optional[Dict[str, Any]]:
    status, body = _http_request(url)
    if status != 200 or not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def _http_get_html(url: str) -> Optional[str]:
    status, body = _http_request(url)
    if status != 200:
        return None
    return body


def _engine_post(path: str, body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    status, text = _http_request(f"{ENGINE_HOST}{path}", method="POST", body=body)
    if status != 200:
        _log(f"  engine POST {path} returned {status}: {text[:200]}")
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _engine_get(path: str, params: Dict[str, str]) -> Optional[Dict[str, Any]]:
    qs = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
    status, text = _http_request(f"{ENGINE_HOST}{path}?{qs}")
    if status != 200:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


# ============================================================
# Slug + normalization
# ============================================================

def _slugify(name: str) -> str:
    """Convert commander name to EDHREC URL slug.

    Examples:
      "The Ur-Dragon" -> "the-ur-dragon"
      "Y'shtola, Night's Blessed" -> "yshtola-nights-blessed"
      "Ms. Bumbleflower" -> "ms-bumbleflower"
      "Sauron, the Dark Lord" -> "sauron-the-dark-lord"
    """
    s = name.lower()
    s = s.replace("'", "").replace(",", "").replace(".", "")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s


def _norm_commander_name(name: str) -> str:
    """Normalize for dedup comparison (case + whitespace)."""
    return " ".join(name.strip().split()).lower()


# ============================================================
# Top-commanders list
# ============================================================

def _fetch_top_commanders(count: int) -> List[Dict[str, Any]]:
    """Fetch top N commanders from EDHREC. Paginate via `more` field.

    Returns list of {name, slug, rank, deck_count}.
    """
    results: List[Dict[str, Any]] = []
    url = f"{EDHREC_JSON}/commanders/year.json"
    rank = 1
    while url and len(results) < count:
        data = _http_get_json(url)
        if not data:
            break
        cardlists = (data.get("container") or {}).get("json_dict", {}).get("cardlists", [])
        if not cardlists and isinstance(data.get("cardlists"), list):
            cardlists = data["cardlists"]
        for entry_group in cardlists:
            cardviews = entry_group.get("cardviews", []) if isinstance(entry_group, dict) else []
            for cv in cardviews:
                if not isinstance(cv, dict):
                    continue
                name = cv.get("name")
                slug = cv.get("sanitized") or _slugify(name) if name else None
                if not name or not slug:
                    continue
                deck_count = 0
                cardkingdom = cv.get("cardkingdom") or {}
                num_decks = cv.get("num_decks")
                if isinstance(num_decks, int):
                    deck_count = num_decks
                results.append({
                    "name": name,
                    "slug": slug,
                    "rank": rank,
                    "deck_count": deck_count,
                })
                rank += 1
                if len(results) >= count:
                    break
            if len(results) >= count:
                break
        more = data.get("more")
        if isinstance(more, str) and more.strip():
            url = f"{EDHREC_JSON}/{more.lstrip('/')}" if not more.startswith("http") else more
            if not url.endswith(".json"):
                url += ".json"
            time.sleep(EDHREC_REQUEST_DELAY)
        else:
            url = ""
    return results[:count]


# ============================================================
# Existing corpus + progress state
# ============================================================

def _load_existing_corpus_commanders() -> set:
    """Return set of normalized commander names already in corpus_v1.json."""
    try:
        with open(CORPUS_V1_PATH, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return set()
    decks = data.get("decks") if isinstance(data, dict) else None
    if not isinstance(decks, list):
        return set()
    names = set()
    for entry in decks:
        if not isinstance(entry, dict):
            continue
        commander = entry.get("commander")
        if isinstance(commander, str) and commander.strip():
            names.add(_norm_commander_name(commander))
    return names


def _load_progress() -> Dict[str, Dict[str, Any]]:
    """Return slug -> latest progress entry."""
    progress: Dict[str, Dict[str, Any]] = {}
    if not PROGRESS_PATH.exists():
        return progress
    with open(PROGRESS_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            slug = entry.get("commander_slug")
            if isinstance(slug, str) and slug:
                progress[slug] = entry
    return progress


def _append_progress(entry: Dict[str, Any]) -> None:
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ============================================================
# Strategy notes file (atomic)
# ============================================================

def _load_strategy_notes() -> Dict[str, Any]:
    try:
        with open(STRATEGY_NOTES_PATH, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"version": "commander_strategy_notes_v1.0", "by_commander": {}}
    if "by_commander" not in data or not isinstance(data.get("by_commander"), dict):
        data["by_commander"] = {}
    return data


def _save_strategy_notes(data: Dict[str, Any]) -> None:
    STRATEGY_NOTES_PATH.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8",
                                     dir=str(STRATEGY_NOTES_PATH.parent), newline="\n") as tmp:
        tmp.write(text)
        tmp_path = tmp.name
    os.replace(tmp_path, str(STRATEGY_NOTES_PATH))


# ============================================================
# EDHREC fetchers
# ============================================================

def _extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _fetch_commander_page_data(slug: str) -> Optional[Dict[str, Any]]:
    """Returns the commander page's data dict (description, articles, etc.)."""
    url = f"{EDHREC_JSON}/commanders/{slug}.json"
    return _http_get_json(url)


def _fetch_canonical_commander_name(slug: str) -> Optional[str]:
    """Look up the canonical card name for a slug via EDHREC's commander page.

    The page's `header` field is typically "Card Name (EDH Recommendations)"
    or similar; the `title` field is sometimes cleaner. We try title first,
    then strip suffixes from header.
    """
    data = _fetch_commander_page_data(slug)
    if not data:
        return None
    title = data.get("title")
    if isinstance(title, str) and title.strip():
        # Title sometimes has " - Commander (EDH) ..." suffix; strip after the dash
        clean = re.split(r"\s+-\s+", title)[0].strip()
        if clean:
            return clean
    header = data.get("header")
    if isinstance(header, str) and header.strip():
        clean = re.split(r"\s+\(", header)[0].strip()
        if clean:
            return clean
    return None


def _discover_precon_url(slug: str) -> Optional[str]:
    """Returns full precon URL if present, else None.

    Checks the commander page HTML for /precon/<slug> or /precon/<set>/<commander>.
    """
    url = f"{EDHREC_HTML}/commanders/{slug}"
    html = _http_get_html(url)
    if not html:
        return None
    # Multi-commander pattern is more specific, try first
    m = re.search(r'href="(/precon/[a-z0-9-]+/[a-z0-9-]+)"', html)
    if m:
        return f"{EDHREC_HTML}{m.group(1)}"
    m = re.search(r'href="(/precon/[a-z0-9-]+)"', html)
    if m:
        return f"{EDHREC_HTML}{m.group(1)}"
    return None


def _parse_decklist_strings(raw_deck: List[str]) -> List[str]:
    """Convert ['1 Sol Ring', '4 Mountain', ...] -> ['Sol Ring', 'Mountain', ...].

    Strips quantity prefix (keeps unique entries; collapse is per the playbook's
    basic-land caveat — engine accepts list >= 50).
    """
    cards: List[str] = []
    for item in raw_deck:
        if not isinstance(item, str):
            continue
        s = item.strip()
        m = re.match(r"^(\d+)\s+(.+)$", s)
        if m:
            cards.append(m.group(2).strip())
        elif s:
            cards.append(s)
    return cards


def _fetch_deckpreview(urlhash: str) -> Optional[Dict[str, Any]]:
    """Returns {cards, tags, coloridentity, commanders} or None."""
    url = f"{EDHREC_HTML}/deckpreview/{urlhash}"
    html = _http_get_html(url)
    if not html:
        return None
    nd = _extract_next_data(html)
    if not nd:
        return None
    data = (nd.get("props") or {}).get("pageProps", {}).get("data")
    if not isinstance(data, dict):
        return None
    raw_deck = data.get("deck")
    if not isinstance(raw_deck, list):
        return None
    cards = _parse_decklist_strings(raw_deck)
    tags = data.get("edhrec_tags") or []
    if not isinstance(tags, list):
        tags = []
    return {
        "cards": cards,
        "tags": [str(t) for t in tags if isinstance(t, (str, int))],
        "coloridentity": data.get("coloridentity") or [],
        "commanders": data.get("commanders") or [],
    }


def _fetch_average_deck(slug: str, bracket_slug: str) -> Optional[Dict[str, Any]]:
    """Returns {cards, num_decks_avg} or None."""
    url = f"{EDHREC_JSON}/average-decks/{slug}/{bracket_slug}.json"
    data = _http_get_json(url)
    if not data:
        return None
    raw_deck = data.get("deck")
    if not isinstance(raw_deck, list):
        return None
    cards = _parse_decklist_strings(raw_deck)
    return {"cards": cards, "num_decks_avg": data.get("num_decks_avg") or 0}


def _fetch_bracket_index_top2(slug: str, bracket_slug: str) -> List[str]:
    """Return top-2 deck urlhashes for the bracket. Empty list if page missing.

    EDHREC's bracket-index JSON puts deck rows at `data.table` — NOT at
    `cardlists`/`decks`/`results` (those keys exist on combo pages and commander
    pages but not here). Each table entry has {urlhash, savedate, price, tags,
    salt, <card-type counts>}.
    """
    url = f"{EDHREC_JSON}/decks/{slug}/{bracket_slug}.json"
    data = _http_get_json(url)
    if not data:
        return []
    table = data.get("table")
    if not isinstance(table, list):
        return []
    hashes: List[str] = []
    for d in table[:2]:
        if isinstance(d, dict) and isinstance(d.get("urlhash"), str):
            hashes.append(d["urlhash"])
    return hashes


def _fetch_bracket_index_top2_with_tags(slug: str, bracket_slug: str) -> List[Tuple[str, List[str]]]:
    """Return top-2 (urlhash, tags) tuples. Index tags are an alternate source
    for archetype_hint when the deckpreview's edhrec_tags is empty."""
    url = f"{EDHREC_JSON}/decks/{slug}/{bracket_slug}.json"
    data = _http_get_json(url)
    if not data:
        return []
    table = data.get("table")
    if not isinstance(table, list):
        return []
    out: List[Tuple[str, List[str]]] = []
    for d in table[:2]:
        if isinstance(d, dict) and isinstance(d.get("urlhash"), str):
            tags = d.get("tags") if isinstance(d.get("tags"), list) else []
            out.append((d["urlhash"], [str(t) for t in tags if isinstance(t, (str, int))]))
    return out


def _fetch_precon_decklist(precon_url: str) -> Optional[Tuple[List[str], str]]:
    """Returns (cards, precon_name) or None.

    precon_name is derived from the URL slug for source_label use.
    """
    html = _http_get_html(precon_url)
    if not html:
        return None
    nd = _extract_next_data(html)
    if not nd:
        return None
    data = (nd.get("props") or {}).get("pageProps", {}).get("data")
    if not isinstance(data, dict):
        return None
    raw_deck = data.get("deck")
    if not isinstance(raw_deck, list):
        return None
    cards = _parse_decklist_strings(raw_deck)
    # Extract precon slug from URL: /precon/<set>/<commander> or /precon/<slug>
    parts = precon_url.rstrip("/").split("/")
    precon_slug = parts[-1] if len(parts) >= 2 else "unknown"
    if len(parts) >= 3 and parts[-2] != "precon":
        precon_slug = parts[-2]
    return cards, precon_slug


def _fetch_strategy_notes(slug: str) -> Optional[Dict[str, Any]]:
    """Returns commander strategy notes for the strategy_notes file.

    EDHREC's commander page JSON exposes:
      - container.description: short generic blurb (e.g. "Popular decks and cards for X")
      - panels.articles[]: list of article references with `value` (title) + `href` (URL)
        — no body text exposed; would need per-article fetch to get excerpts.

    We capture: description + article titles/URLs as topical hints. Article bodies
    are not fetched (cost not worth the value for ~250 commanders).
    """
    data = _fetch_commander_page_data(slug)
    if not data:
        return None
    description = ""
    container = data.get("container")
    if isinstance(container, dict):
        description = (container.get("description") or "").strip()
    if not description:
        description = (data.get("description") or "").strip()

    article_titles: List[str] = []
    article_urls: List[str] = []
    panels = data.get("panels")
    panel_articles = panels.get("articles") if isinstance(panels, dict) else []
    if isinstance(panel_articles, list):
        for art in panel_articles[:8]:
            if isinstance(art, dict):
                title = art.get("value") or art.get("title") or ""
                href = art.get("href") or ""
                if isinstance(title, str) and title.strip():
                    article_titles.append(title.strip())
                if isinstance(href, str) and href.strip():
                    article_urls.append(href.strip())

    strategy_text = description
    if article_titles:
        if strategy_text:
            strategy_text += "\n\n"
        strategy_text += "Recent articles:\n" + "\n".join(f"- {t}" for t in article_titles)

    return {
        "strategy_text": strategy_text,
        "article_titles": article_titles,
        "article_urls": article_urls,
        "scraped_at": _now_iso(),
    }


# ============================================================
# Engine calls
# ============================================================

def _engine_version_check_via_ingest_ping() -> Optional[str]:
    """Returns the corpus_batch_ingest VERSION string, or None on failure.

    Uses a deliberately-rejected request (decklist too short) to ping the
    endpoint without ingesting; the response still carries the VERSION field.
    """
    body = {
        "db_snapshot_id": DB_SNAPSHOT_ID,
        "entries": [],
        "skip_bracket_verification": False,
    }
    resp = _engine_post("/corpus/batch_ingest_v1", body)
    if not resp:
        return None
    return resp.get("version")


def _engine_call_ingest(entries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    body = {
        "db_snapshot_id": DB_SNAPSHOT_ID,
        "entries": entries,
        "skip_bracket_verification": False,
    }
    return _engine_post("/corpus/batch_ingest_v1", body)


def _engine_warmup_brief(commander_name: str) -> Optional[Dict[str, Any]]:
    """Calls archetype_brief; retries with backoff. None if all retries failed."""
    for attempt in range(BRIEF_WARMUP_RETRIES):
        resp = _engine_get(
            "/commander/archetype_brief_v1",
            {"commander": commander_name, "db_snapshot_id": DB_SNAPSHOT_ID},
        )
        if resp and isinstance(resp.get("corpus_deck_count"), int):
            return resp
        wait_s = min(30, 2 ** attempt)
        _log(f"  brief warm-up retry {attempt+1}/{BRIEF_WARMUP_RETRIES} (waiting {wait_s}s)")
        time.sleep(wait_s)
    return None


# ============================================================
# Per-commander workflow
# ============================================================

def _build_archetype_hint(tags: List[str]) -> str:
    cleaned = [str(t).strip() for t in tags if isinstance(t, (str, int)) and str(t).strip()]
    if not cleaned:
        return ""
    return " / ".join(cleaned)


def _process_commander(slug: str, name: str, rank: int) -> Dict[str, Any]:
    """Run full 16-entry workflow for one commander. Returns progress entry."""
    started_at = _now_iso()
    started_ts = time.monotonic()
    entry: Dict[str, Any] = {
        "commander_slug": slug,
        "commander_name": name,
        "rank": rank,
        "started_at": started_at,
        "completed_at": None,
        "status": "FAILED",
        "reason": "",
        "entries_ingested": 0,
        "auto_bumps": 0,
        "color_identity": [],
    }

    ingest_entries: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # 1. Precon discovery
    try:
        precon_url = _discover_precon_url(slug)
    except Exception as exc:
        precon_url = None
        warnings.append(f"precon_discovery_failed:{exc.__class__.__name__}")
    time.sleep(EDHREC_REQUEST_DELAY)
    if precon_url:
        try:
            precon = _fetch_precon_decklist(precon_url)
        except Exception as exc:
            precon = None
            warnings.append(f"precon_fetch_failed:{exc.__class__.__name__}")
        time.sleep(EDHREC_REQUEST_DELAY)
        if not precon:
            warnings.append(f"precon_fetch_returned_none:{precon_url}")
        else:
            cards, precon_slug = precon
            if len(cards) < 50:
                warnings.append(f"precon_too_short_n={len(cards)}:{precon_url}")
            else:
                ingest_entries.append({
                    "commander": name,
                    "decklist": cards,
                    "claimed_bracket": "B2",
                    "source_url": precon_url,
                    "source_label": f"edhrec_precon_{precon_slug}",
                    "archetype_hint": f"Precon ({precon_slug.replace('-', ' ').title()})",
                })
    else:
        warnings.append("no_precon")

    # 2. 5 per-bracket averages
    for bracket_slug in BRACKET_SLUGS:
        try:
            avg = _fetch_average_deck(slug, bracket_slug)
        except Exception as exc:
            avg = None
            warnings.append(f"avg_{bracket_slug}_fetch_failed:{exc.__class__.__name__}")
        time.sleep(EDHREC_REQUEST_DELAY)
        if not avg:
            warnings.append(f"avg_{bracket_slug}_missing")
            continue
        n = int(avg.get("num_decks_avg") or 0)
        if n < MIN_AVG_SAMPLE:
            warnings.append(f"avg_{bracket_slug}_low_sample_n={n}")
            continue
        cards = avg["cards"]
        if len(cards) < 50:
            warnings.append(f"avg_{bracket_slug}_short_decklist")
            continue
        ingest_entries.append({
            "commander": name,
            "decklist": cards,
            "claimed_bracket": BRACKET_CLAIMS[bracket_slug],
            "source_url": f"{EDHREC_HTML}/average-decks/{slug}/{bracket_slug}",
            "source_label": f"edhrec_average_{slug.replace('-', '_')}_b{BRACKET_CLAIMS[bracket_slug][1]}",
            "archetype_hint": f"Average {BRACKET_CLAIMS[bracket_slug]} Deck (n={n})",
        })

    # 3. 10 ranked (top-2 per bracket)
    for bracket_slug in BRACKET_SLUGS:
        try:
            hashes_with_tags = _fetch_bracket_index_top2_with_tags(slug, bracket_slug)
        except Exception as exc:
            hashes_with_tags = []
            warnings.append(f"index_{bracket_slug}_fetch_failed:{exc.__class__.__name__}")
        time.sleep(EDHREC_REQUEST_DELAY)
        if len(hashes_with_tags) < 2:
            warnings.append(f"index_{bracket_slug}_fewer_than_2:got_{len(hashes_with_tags)}")
        for i, (h, index_tags) in enumerate(hashes_with_tags[:2]):
            if time.monotonic() - started_ts > PER_COMMANDER_DEADLINE_SECONDS:
                entry["reason"] = "per_commander_deadline_exceeded"
                entry["completed_at"] = _now_iso()
                return entry
            try:
                dp = _fetch_deckpreview(h)
            except Exception as exc:
                dp = None
                warnings.append(f"deck_{bracket_slug}_r{i+1}_fetch_failed:{exc.__class__.__name__}")
            time.sleep(EDHREC_REQUEST_DELAY)
            if not dp or len(dp.get("cards", [])) < 50:
                warnings.append(f"deck_{bracket_slug}_r{i+1}_too_short")
                continue
            # Prefer deckpreview tags; fall back to index tags
            dp_tags = dp.get("tags") or []
            tags_for_hint = dp_tags if dp_tags else index_tags
            archetype_hint = _build_archetype_hint(tags_for_hint)
            if not archetype_hint:
                archetype_hint = f"{BRACKET_CLAIMS[bracket_slug]} Ranked Deck"
            ingest_entries.append({
                "commander": name,
                "decklist": dp["cards"],
                "claimed_bracket": BRACKET_CLAIMS[bracket_slug],
                "source_url": f"{EDHREC_HTML}/deckpreview/{h}",
                "source_label": f"edhrec_{slug.replace('-', '_')}_{bracket_slug}_rank{i+1}",
                "archetype_hint": archetype_hint,
            })

    if not ingest_entries:
        entry["status"] = "FAILED"
        entry["reason"] = f"no_entries_collected;warnings={','.join(warnings)}"
        entry["completed_at"] = _now_iso()
        return entry

    # 4. Ingest
    time.sleep(ENGINE_REQUEST_DELAY)
    resp = _engine_call_ingest(ingest_entries)
    if not resp:
        entry["status"] = "FAILED"
        entry["reason"] = f"ingest_call_failed;warnings={','.join(warnings)}"
        entry["completed_at"] = _now_iso()
        return entry

    if resp.get("version") != EXPECTED_INGEST_VERSION:
        entry["status"] = "ENGINE_VERSION_MISMATCH"
        entry["reason"] = f"got_version={resp.get('version')};expected={EXPECTED_INGEST_VERSION}"
        entry["completed_at"] = _now_iso()
        return entry

    accepted = int(resp.get("accepted", 0))
    auto_bumped = int(resp.get("auto_bumped", 0))
    rejected = int(resp.get("rejected", 0))
    entry["entries_ingested"] = accepted + auto_bumped
    entry["auto_bumps"] = auto_bumped
    entry["ingest_rejected"] = rejected
    entry["ingest_total_submitted"] = int(resp.get("total_submitted", 0))
    if warnings:
        entry["warnings"] = warnings

    # 5. Engine brief warm-up (per Sauron-precedent retry handling)
    time.sleep(ENGINE_REQUEST_DELAY)
    brief = _engine_warmup_brief(name)
    if not brief:
        entry["status"] = "PARTIAL"
        entry["reason"] = "ingest_ok_but_brief_warmup_failed"
        entry["completed_at"] = _now_iso()
        return entry

    entry["color_identity"] = brief.get("color_identity") or []
    entry["corpus_deck_count_after"] = brief.get("corpus_deck_count")
    entry["status"] = "SUCCESS" if rejected == 0 else "PARTIAL"
    if rejected > 0 and not entry["reason"]:
        entry["reason"] = f"{rejected}_entries_rejected"
    entry["completed_at"] = _now_iso()
    return entry


# ============================================================
# Git auto-commit
# ============================================================

def _git_commit_and_push(message: str, *, no_git: bool) -> Tuple[bool, str]:
    """Returns (success, commit_hash_or_error). Retries once."""
    if no_git:
        return True, "skipped_no_git"
    cwd = str(REPO_ROOT)
    for attempt in range(2):
        try:
            subprocess.run(["git", "add", "-A"], cwd=cwd, check=True, capture_output=True, text=True, timeout=60)
            r = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd, capture_output=True, timeout=10)
            if r.returncode == 0:
                return True, "no_changes"
            subprocess.run(["git", "commit", "-m", message], cwd=cwd, check=True, capture_output=True, text=True, timeout=60)
            hash_r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=cwd, check=True, capture_output=True, text=True, timeout=10)
            commit_hash = hash_r.stdout.strip()
            subprocess.run(["git", "push", "origin", "HEAD"], cwd=cwd, check=True, capture_output=True, text=True, timeout=120)
            return True, commit_hash
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr or ""
            if attempt == 0:
                _log(f"  git op failed (attempt 1/2): {stderr[:200]}; retrying in 5s")
                time.sleep(5)
                continue
            return False, stderr[:300]
        except subprocess.TimeoutExpired:
            if attempt == 0:
                _log("  git op timeout (attempt 1/2); retrying in 5s")
                time.sleep(5)
                continue
            return False, "git_timeout"
    return False, "unreachable"


# ============================================================
# Main
# ============================================================

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Phase 5a bulk corpus ingest orchestrator")
    parser.add_argument("--count", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--commit-every", type=int, default=DEFAULT_COMMIT_EVERY)
    parser.add_argument("--start-rank", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--commander-slugs", type=str, default="")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--no-git", action="store_true")
    args = parser.parse_args(argv)

    _log(f"Engine version check (ping /corpus/batch_ingest_v1)...")
    version = _engine_version_check_via_ingest_ping()
    if version != EXPECTED_INGEST_VERSION:
        _log(f"ENGINE VERSION GATE FAILED: got {version!r}, expected {EXPECTED_INGEST_VERSION!r}")
        return 2
    _log(f"  engine version OK: {version}")

    progress = _load_progress()
    existing_commanders = _load_existing_corpus_commanders()
    _log(f"Existing corpus has {len(existing_commanders)} distinct commanders")
    _log(f"Progress file has {len(progress)} prior commander attempts")

    # Build planned commander list
    if args.commander_slugs.strip():
        # Explicit slug list mode — look up canonical name via EDHREC page
        slug_list = [s.strip() for s in args.commander_slugs.split(",") if s.strip()]
        planned: List[Dict[str, Any]] = []
        for i, slug in enumerate(slug_list):
            canonical = _fetch_canonical_commander_name(slug)
            time.sleep(EDHREC_REQUEST_DELAY)
            if not canonical:
                # Fall back to slug-derived guess; warn the user
                canonical = " ".join(p.capitalize() for p in slug.split("-"))
                _log(f"  WARN: could not resolve canonical name for {slug}; using guess {canonical!r}")
            planned.append({"name": canonical, "slug": slug, "rank": i + 1, "deck_count": 0})
        _log(f"Explicit commander slugs mode: {len(planned)} commanders")
    else:
        # `--count N` means "process N NET-NEW commanders" (not "fetch top-N raw").
        # Over-fetch from EDHREC then dedupe + slice; the corpus may already
        # contain many of the top-ranked commanders from prior manual sweeps.
        # EDHREC's `commanders/year.json` and equivalent HTML page both cap at
        # 100 commanders — no JSON pagination is exposed, so this is a hard
        # ceiling on the orchestrator's reach via this ranking source.
        EDHREC_RANKING_CAP = 100
        over_fetch = min(EDHREC_RANKING_CAP, max(args.count * 2, EDHREC_RANKING_CAP))
        _log(f"Fetching top {over_fetch} commanders from EDHREC year.json (over-fetch for --count={args.count} net-new)...")
        top_commanders = _fetch_top_commanders(over_fetch)
        if len(top_commanders) < 50:
            _log(f"HALT: top-2-years fetch returned only {len(top_commanders)} commanders (<50 floor — API health issue)")
            return 3
        if len(top_commanders) < EDHREC_RANKING_CAP:
            _log(f"  note: EDHREC returned {len(top_commanders)} (under {EDHREC_RANKING_CAP} expected cap)")
        if args.count > EDHREC_RANKING_CAP:
            _log(f"  note: requested --count {args.count} but EDHREC ranking endpoint caps at {EDHREC_RANKING_CAP}; this run will process at most {EDHREC_RANKING_CAP} net-new")
        planned = [c for c in top_commanders if c["rank"] >= args.start_rank]

    # Filter: skip already-in-corpus + already-SUCCESS in progress, then slice
    # to args.count net-new (only in top-N mode; explicit-slug mode processes
    # the exact list provided).
    to_process: List[Dict[str, Any]] = []
    skipped_corpus = 0
    skipped_progress = 0
    for c in planned:
        # In explicit-slug mode the name is a guess; we can't reliably match
        # against existing_commanders by name. Skip the corpus-membership
        # filter in explicit mode (let the engine do dedupe at ingest time).
        name_norm = _norm_commander_name(c["name"])
        if not args.commander_slugs.strip() and name_norm in existing_commanders:
            skipped_corpus += 1
            continue
        prior = progress.get(c["slug"])
        if prior and prior.get("status") == "SUCCESS" and not args.retry_failed:
            skipped_progress += 1
            continue
        if prior and prior.get("status") in ("PARTIAL", "FAILED") and not args.retry_failed:
            skipped_progress += 1
            continue
        to_process.append(c)
        if not args.commander_slugs.strip() and len(to_process) >= args.count:
            break

    _log(f"  skipped (in corpus): {skipped_corpus}")
    _log(f"  skipped (progress SUCCESS/PARTIAL/FAILED): {skipped_progress}")
    _log(f"\nPlanned: {len(to_process)} commanders to process")
    for c in to_process[:10]:
        _log(f"  rank {c['rank']:>3}: {c['name']} ({c['slug']})")
    if len(to_process) > 10:
        _log(f"  ... and {len(to_process) - 10} more")

    if args.dry_run:
        _log("\nDRY RUN — exiting without fetching/ingesting")
        return 0

    notes = _load_strategy_notes()
    processed_since_commit: List[str] = []
    total_processed = 0
    total_entries = 0
    total_bumps = 0

    for c in to_process:
        slug, name, rank = c["slug"], c["name"], c["rank"]
        _log(f"\n[{rank}] processing {slug} ({name})...")

        # Capture strategy notes first (non-fatal if missing)
        try:
            sn = _fetch_strategy_notes(slug)
            if sn and sn.get("strategy_text"):
                # Resolve canonical commander name from the page if possible
                notes["by_commander"][name] = {
                    **sn,
                    "commander_slug": slug,
                    "rank": rank,
                }
        except Exception as exc:
            _log(f"  strategy notes fetch failed: {exc!r}")

        # Run main workflow
        try:
            result = _process_commander(slug, name, rank)
        except Exception as exc:
            result = {
                "commander_slug": slug,
                "commander_name": name,
                "rank": rank,
                "started_at": _now_iso(),
                "completed_at": _now_iso(),
                "status": "FAILED",
                "reason": f"unhandled_exception:{exc.__class__.__name__}:{str(exc)[:200]}",
                "entries_ingested": 0,
                "auto_bumps": 0,
                "color_identity": [],
            }

        # If the engine returned a canonical commander name, use it for notes
        if result.get("status") in ("SUCCESS", "PARTIAL"):
            brief_name = name  # process function uses the name we passed
            if brief_name in notes["by_commander"]:
                notes["by_commander"][brief_name]["color_identity"] = result.get("color_identity") or []
                notes["by_commander"][brief_name]["corpus_deck_count_after"] = result.get("corpus_deck_count_after")

        _append_progress(result)
        if result.get("status") in ("SUCCESS", "PARTIAL"):
            try:
                _save_strategy_notes(notes)
            except Exception as exc:
                _log(f"  strategy notes save failed: {exc!r}")

        status = result.get("status", "UNKNOWN")
        ingested = result.get("entries_ingested", 0)
        bumps = result.get("auto_bumps", 0)
        reason = result.get("reason", "")
        if status == "SUCCESS":
            _log(f"  SUCCESS: {ingested} entries, {bumps} auto-bumps. Brief OK.")
        elif status == "PARTIAL":
            _log(f"  PARTIAL: {ingested} entries, {bumps} bumps. Reason: {reason}")
        elif status == "ENGINE_VERSION_MISMATCH":
            _log(f"  HALT: engine version mismatch. {reason}")
            _log("Exiting — fix uvicorn and re-run.")
            return 4
        else:
            _log(f"  FAILED: {reason}")

        total_processed += 1
        total_entries += ingested
        total_bumps += bumps
        if status in ("SUCCESS", "PARTIAL"):
            processed_since_commit.append(slug)

        # Auto-commit cadence
        if len(processed_since_commit) >= args.commit_every:
            msg = (
                f"Phase 5a bulk: commanders {processed_since_commit[0]} through "
                f"{processed_since_commit[-1]} ({len(processed_since_commit)} this batch; "
                f"running total: {total_entries} entries from {total_processed} commanders)"
            )
            ok, ref = _git_commit_and_push(msg, no_git=args.no_git)
            if ok:
                _log(f"  COMMIT {ref[:12]}: {len(processed_since_commit)} commanders")
                # Annotate progress with commit ref (separate JSONL line as metadata)
                _append_progress({
                    "_meta": "commit",
                    "commit_ref": ref,
                    "commanders_in_commit": processed_since_commit,
                    "timestamp": _now_iso(),
                })
                processed_since_commit = []
            else:
                _log(f"  HALT: git commit/push failed twice. Error: {ref}")
                return 5

    # Final commit for stragglers
    if processed_since_commit:
        msg = (
            f"Phase 5a bulk: final batch {processed_since_commit[0]} through "
            f"{processed_since_commit[-1]} ({len(processed_since_commit)} commanders; "
            f"running total: {total_entries} entries from {total_processed} commanders)"
        )
        ok, ref = _git_commit_and_push(msg, no_git=args.no_git)
        if ok:
            _log(f"  FINAL COMMIT {ref[:12]}: {len(processed_since_commit)} commanders")
        else:
            _log(f"  WARN: final git commit/push failed: {ref}")

    _log(f"\nDone. Processed {total_processed} commanders, {total_entries} corpus entries, {total_bumps} auto-bumps.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
