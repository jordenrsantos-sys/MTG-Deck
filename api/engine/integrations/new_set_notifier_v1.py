"""
new_set_notifier_v1 — Mega-task v3 Phase 8.

Optional desktop notification when a new set has been processed and
its discovery report published. Opt-in via the
`MTG_ENGINE_NOTIFICATIONS_ENABLED` environment variable; default off.

Two notification paths:

  1. **File path (always available)** — writes a notification record
     to `repo/api/engine/data/notifications/<timestamp>_<set_code>.json`.
     Always writes when enabled; this is the durable audit log.

  2. **Desktop toast (opportunistic)** — on Windows only, attempts a
     toast via PowerShell + `BurntToast`. If the module isn't
     installed, silently falls back to file-only. Mac/Linux paths are
     out of scope for v0.1.

Public API:
  - `is_enabled() -> bool`
  - `compose_notification(set_code, set_name, card_count, top_archetypes,
                           report_path) -> Notification`
  - `notify(notification, allow_desktop_toast=True) -> NotificationResult`
"""
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


NEW_SET_NOTIFIER_VERSION = "new_set_notifier_v1.0"

ENABLED_ENV_VAR = "MTG_ENGINE_NOTIFICATIONS_ENABLED"

NOTIFICATIONS_DIR = (
    Path(__file__).resolve().parents[1] / "data" / "notifications"
)


@dataclass
class Notification:
    set_code: str
    set_name: str
    card_count: int
    top_archetypes: List[str]
    report_path: str
    title: str
    body: str
    composed_at: str


@dataclass
class NotificationResult:
    status: str   # ok / disabled / file_only / failed
    actions: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    file_path: Optional[str] = None


def is_enabled() -> bool:
    val = os.environ.get(ENABLED_ENV_VAR, "").strip().lower()
    return val in ("1", "true", "yes", "on")


def compose_notification(
    set_code: str,
    set_name: str,
    card_count: int,
    top_archetypes: List[str],
    report_path: str,
) -> Notification:
    """Build the Notification payload."""
    top = top_archetypes[:3] if top_archetypes else []
    if top:
        archetype_str = ", ".join(top)
        body = (
            f"{card_count} new cards processed. Top archetype impacts: "
            f"{archetype_str}. Report at {report_path}"
        )
    else:
        body = f"{card_count} new cards processed. Report at {report_path}"
    return Notification(
        set_code=set_code, set_name=set_name, card_count=card_count,
        top_archetypes=list(top), report_path=report_path,
        title=f"MTG set processed: {set_name}",
        body=body,
        composed_at=datetime.now(timezone.utc).isoformat(),
    )


def _write_notification_file(n: Notification) -> str:
    """Write the notification record to the notifications dir. Returns
    the filepath."""
    NOTIFICATIONS_DIR.mkdir(parents=True, exist_ok=True)
    ts = n.composed_at.replace(":", "").replace("-", "").replace("T", "_")
    # Strip the timezone tail so filenames stay clean.
    ts = ts.split(".")[0].split("+")[0]
    filename = f"{ts}_{n.set_code}.json"
    path = NOTIFICATIONS_DIR / filename
    path.write_text(json.dumps(asdict(n), indent=2), encoding="utf-8")
    return str(path)


def _try_desktop_toast(n: Notification) -> Optional[str]:
    """Attempt a Windows 10/11 toast via PowerShell + BurntToast.

    Returns None on success; an error string on failure. Silent skip on
    non-Windows.
    """
    if os.name != "nt":
        return "non-windows platform"
    # Use BurntToast if available. Quote the title + body carefully —
    # avoid shell injection by using single-quoted strings + escaping
    # any embedded single quotes.
    safe_title = (n.title or "").replace("'", "''")
    safe_body = (n.body or "").replace("'", "''")
    ps_cmd = (
        "if (Get-Module -ListAvailable -Name BurntToast) { "
        "Import-Module BurntToast; "
        f"New-BurntToastNotification -Text '{safe_title}', '{safe_body}' "
        "} else { exit 2 }"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive",
             "-Command", ps_cmd],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return None
        if result.returncode == 2:
            return "BurntToast module not installed"
        return f"powershell exit {result.returncode}: {result.stderr[:200]}"
    except FileNotFoundError:
        return "powershell not found on PATH"
    except subprocess.TimeoutExpired:
        return "powershell timeout"
    except Exception as exc:
        return f"unexpected: {exc!r}"


def notify(
    notification: Notification,
    allow_desktop_toast: bool = True,
) -> NotificationResult:
    """Fire the notification per the configured policy.

    Always writes the notification file (audit log). Optionally
    attempts the Windows desktop toast.
    """
    if not is_enabled():
        return NotificationResult(
            status="disabled",
            warnings=[f"{ENABLED_ENV_VAR} not set; notification suppressed"],
        )

    actions: List[str] = []
    warnings: List[str] = []
    file_path: Optional[str] = None
    try:
        file_path = _write_notification_file(notification)
        actions.append(f"wrote notification file: {file_path}")
    except Exception as exc:
        warnings.append(f"file write failed: {exc!r}")

    desktop_status = "file_only"
    if allow_desktop_toast:
        err = _try_desktop_toast(notification)
        if err is None:
            actions.append("desktop toast displayed")
            desktop_status = "ok"
        else:
            warnings.append(f"desktop toast skipped: {err}")

    if not actions:
        status = "failed"
    elif desktop_status == "ok":
        status = "ok"
    else:
        status = "file_only"

    return NotificationResult(
        status=status, actions=actions, warnings=warnings,
        file_path=file_path,
    )
