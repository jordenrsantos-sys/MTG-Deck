from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, Sequence

BULK_INDEX_URL = "https://api.scryfall.com/bulk-data"
DEFAULT_BULK_FILENAME = "default-cards.json"


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def _resolve_path(raw_path: Any) -> Path:
    token = _nonempty_str(raw_path)
    if token == "":
        raise RuntimeError("Path is required")

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate.resolve()


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _download_default_cards_index_uri() -> str:
    with urllib.request.urlopen(BULK_INDEX_URL, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))

    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("Unexpected bulk-data payload: missing data list")

    for row in rows:
        if not isinstance(row, dict):
            continue
        if _nonempty_str(row.get("type")) != "default_cards":
            continue

        download_uri = _nonempty_str(row.get("download_uri"))
        if download_uri != "":
            return download_uri

    raise RuntimeError("Could not find download_uri for bulk type default_cards")


def _download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".tmp")

    with urllib.request.urlopen(url, timeout=300) as response:
        payload = response.read()

    with temp_path.open("wb") as file_obj:
        file_obj.write(payload)

    temp_path.replace(destination)


def _run_checked(command: Sequence[str], cwd: Path) -> None:
    subprocess.run(list(command), cwd=str(cwd), check=True)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Update Mode bulk/enrich/prefetch pipeline")
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    parser.add_argument("--snapshot-id", required=True, dest="snapshot_id", help="Snapshot ID used by prefetch")
    parser.add_argument("--cache-dir", default="./data/card_images", dest="cache_dir", help="Local card image cache directory")
    parser.add_argument("--bulk-dir", default="./data/scryfall/bulk", dest="bulk_dir", help="Directory for bulk downloads")
    parser.add_argument("--bulk-json", default="", dest="bulk_json", help="Path to default-cards bulk JSON")
    parser.add_argument("--workers", type=int, default=4, help="Prefetch worker count")
    parser.add_argument("--progress", type=int, default=100, help="Prefetch progress cadence")
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument("--resume", dest="resume", action="store_true", help="Skip cached images (default)")
    resume_group.add_argument("--no-resume", dest="resume", action="store_false", help="Redownload images even when cached")
    parser.set_defaults(resume=True)
    parser.add_argument("--download-bulk", action="store_true", dest="download_bulk", help="Download fresh default_cards bulk JSON from Scryfall")
    parser.add_argument("--skip-enrich", action="store_true", dest="skip_enrich", help="Skip enrichment step")
    parser.add_argument("--skip-prefetch", action="store_true", dest="skip_prefetch", help="Skip prefetch step")
    launch_group = parser.add_mutually_exclusive_group()
    launch_group.add_argument("--launch", action="store_true", dest="launch", help="Launch dev environment after pipeline")
    launch_group.add_argument("--no-launch", action="store_false", dest="launch", help="Do not launch dev environment")
    parser.set_defaults(launch=False)
    parser.add_argument("--limit", type=int, default=0, help="Optional enrichment scan limit for smoke tests")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    db_path = _resolve_path(args.db)
    if not db_path.is_file():
        print(f"ERROR: DB file not found: {db_path}")
        return 2

    cache_dir = _resolve_path(args.cache_dir)
    bulk_dir = _resolve_path(args.bulk_dir)
    bulk_json_path = _resolve_path(args.bulk_json) if _nonempty_str(args.bulk_json) != "" else (bulk_dir / DEFAULT_BULK_FILENAME).resolve()

    snapshot_id = _nonempty_str(args.snapshot_id)
    if snapshot_id == "":
        print("ERROR: --snapshot-id is required")
        return 2

    steps: Dict[str, str] = {
        "download_bulk": "SKIPPED",
        "enrich": "SKIPPED",
        "prefetch": "SKIPPED",
        "launch": "SKIPPED",
    }
    bulk_sha256 = ""
    current_step = "initialization"

    bulk_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    try:
        if args.download_bulk:
            print("STEP 1/3 Download bulk")
            current_step = "download_bulk"
            download_uri = _download_default_cards_index_uri()
            print(f"Downloading default_cards bulk to: {bulk_json_path}")
            _download_file(download_uri, bulk_json_path)
            steps["download_bulk"] = "RAN"

        if not bulk_json_path.is_file():
            raise RuntimeError("Bulk JSON missing. Re-run with --download-bulk or provide --bulk-json.")

        bulk_sha256 = _sha256_file(bulk_json_path)
        print(f"Bulk SHA256: {bulk_sha256}")

        if not args.skip_enrich:
            print("STEP 2/3 Enrich DB")
            current_step = "enrich"
            enrich_command = [
                sys.executable,
                "-m",
                "snapshot_build.enrich_images_from_scryfall_bulk",
                "--db",
                str(db_path),
                "--bulk-json",
                str(bulk_json_path),
                "--bulk-version",
                bulk_sha256,
            ]
            if int(args.limit) > 0:
                enrich_command.extend(["--limit", str(int(args.limit))])
            _run_checked(enrich_command, cwd=repo_root)
            steps["enrich"] = "RAN"
        else:
            print("Skipping enrichment step")

        if not args.skip_prefetch:
            print("STEP 3/3 Prefetch images")
            current_step = "prefetch"
            prefetch_command = [
                sys.executable,
                "-m",
                "snapshot_build.prefetch_card_images",
                "--db",
                str(db_path),
                "--snapshot_id",
                snapshot_id,
                "--source",
                "card_images",
                "--out",
                str(cache_dir),
                "--sizes",
                "normal,small",
                "--workers",
                str(int(args.workers)),
                "--progress",
                str(int(args.progress)),
            ]
            if bool(args.resume):
                prefetch_command.append("--resume")
            else:
                prefetch_command.append("--no-resume")
            _run_checked(prefetch_command, cwd=repo_root)
            steps["prefetch"] = "RAN"
        else:
            print("Skipping prefetch step")

        if args.launch:
            current_step = "launch"
            launch_command = [sys.executable, str((repo_root / "launch_dev.py").resolve())]
            _run_checked(launch_command, cwd=repo_root)
            steps["launch"] = "RAN"

    except subprocess.CalledProcessError as exc:
        print(f"ERROR: Step '{current_step}' failed with exit code {exc.returncode}: {exc.cmd}")
        return 3
    except Exception as exc:
        print(f"ERROR: Step '{current_step}' failed: {exc}")
        return 2

    print("\nUpdate pipeline summary")
    print(f"bulk path: {bulk_json_path}")
    print(f"sha256: {bulk_sha256}")
    print(f"db path: {db_path}")
    print(f"snapshot id: {snapshot_id}")
    print(f"cache dir: {cache_dir}")
    print(
        "steps: "
        f"download_bulk={steps['download_bulk']} "
        f"enrich={steps['enrich']} "
        f"prefetch={steps['prefetch']} "
        f"launch={steps['launch']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
