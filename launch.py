from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
import webbrowser

from engine.db import resolve_db_path, resolve_image_cache_dir

API_HOST = "127.0.0.1"
API_PORT = 8000
DEV_UI_HOST = "127.0.0.1"
DEV_UI_PORT = 5173
API_URL = f"http://{API_HOST}:{API_PORT}"
UI_URL = f"http://{DEV_UI_HOST}:{DEV_UI_PORT}"
NO_BROWSER_ENV = "MTG_ENGINE_NO_BROWSER"

# Mega-task v5 Phase 1: agent_build_deck_v1 is async def wrapping a 110-150s
# synchronous build; a single uvicorn worker blocks the event loop for the
# entire build, freezing /health and every other endpoint. Multiple workers
# fix this by giving non-build requests their own process. Default 2 workers
# (one for the in-flight build, one for sidebar/health/snapshot endpoints);
# override via MTG_ENGINE_API_WORKERS env var if more headroom is desired.
DEFAULT_API_WORKERS = 2
API_WORKERS_ENV = "MTG_ENGINE_API_WORKERS"


def _print_runtime_paths() -> bool:
    try:
        db_path = resolve_db_path()
    except Exception as exc:
        print(f"ERROR: Failed resolving DB_PATH: {exc}")
        return False

    try:
        image_cache_dir = resolve_image_cache_dir()
    except Exception as exc:
        print(f"ERROR: Failed resolving IMAGE_CACHE_DIR: {exc}")
        return False

    print(f"DB_PATH={db_path}")
    print(f"IMAGE_CACHE_DIR={image_cache_dir}")
    return True


def _popen_kwargs() -> dict[str, object]:
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def _start_process(command: list[str], cwd: str, name: str) -> subprocess.Popen:
    try:
        return subprocess.Popen(command, cwd=cwd, **_popen_kwargs())
    except Exception as exc:
        raise RuntimeError(f"Failed to start {name} process: {exc}") from exc


def _wait_for_exit(process: subprocess.Popen, timeout_seconds: int) -> bool:
    try:
        process.wait(timeout=timeout_seconds)
        return True
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return process.poll() is not None


def _get_pid_on_port(port: int) -> str | None:
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return None

    target_suffix = f":{int(port)}"
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue

        local_address = parts[1]
        state = parts[3].upper()
        pid = parts[-1]

        if state == "LISTENING" and local_address.endswith(target_suffix):
            return pid

    return None


def _kill_pid(pid: str) -> None:
    subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False)


def _ensure_port_available(port: int) -> bool:
    pid = _get_pid_on_port(port)
    if pid is None:
        return True

    print(f"Port {port} is in use by PID {pid}")
    decision = input("Kill this process? (y/n): ").strip().lower()
    if decision != "y":
        return False

    print(f"Killing PID {pid}...")
    _kill_pid(pid)
    time.sleep(1)

    remaining_pid = _get_pid_on_port(port)
    if remaining_pid is not None:
        print(f"ERROR: Port {port} is still in use by PID {remaining_pid}.")
        return False

    return True


def _shutdown_process(process: subprocess.Popen | None) -> None:
    if process is None:
        return

    if process.poll() is not None:
        return

    if os.name == "nt":
        try:
            process.send_signal(signal.CTRL_BREAK_EVENT)
        except Exception:
            pass

        if _wait_for_exit(process, 5):
            return

        try:
            process.terminate()
        except Exception:
            pass

        if _wait_for_exit(process, 5):
            return

        try:
            process.kill()
        except Exception:
            return

        _wait_for_exit(process, 5)
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except Exception:
        pass

    if _wait_for_exit(process, 5):
        return

    try:
        process.terminate()
    except Exception:
        pass

    if _wait_for_exit(process, 5):
        return

    try:
        process.kill()
    except Exception:
        return

    _wait_for_exit(process, 5)


def _ui_dist_exists(repo_root: str) -> bool:
    ui_dist_path = os.path.join(repo_root, "ui_harness", "dist")
    ui_index_path = os.path.join(ui_dist_path, "index.html")
    return os.path.isdir(ui_dist_path) and os.path.isfile(ui_index_path)


def _resolve_api_workers() -> int:
    raw = os.getenv(API_WORKERS_ENV, "").strip()
    if not raw:
        return DEFAULT_API_WORKERS
    try:
        n = int(raw)
    except ValueError:
        print(f"WARNING: {API_WORKERS_ENV}={raw!r} is not an int; using default {DEFAULT_API_WORKERS}")
        return DEFAULT_API_WORKERS
    if n < 1:
        print(f"WARNING: {API_WORKERS_ENV}={n} < 1; using 1")
        return 1
    return n


def _start_api_process(repo_root: str) -> subprocess.Popen:
    workers = _resolve_api_workers()
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "api.main:app",
        "--host",
        API_HOST,
        "--port",
        str(API_PORT),
    ]
    if workers > 1:
        cmd.extend(["--workers", str(workers)])
    return _start_process(cmd, cwd=repo_root, name="API")


def _start_ui_dev_process(repo_root: str) -> subprocess.Popen:
    ui_root = os.path.join(repo_root, "ui_harness")
    npm_exe = "npm.cmd" if os.name == "nt" else "npm"
    return _start_process(
        [npm_exe, "run", "dev", "--", "--host", DEV_UI_HOST, "--port", str(DEV_UI_PORT)],
        cwd=ui_root,
        name="UI",
    )


def _monitor_processes(api_process: subprocess.Popen, ui_process: subprocess.Popen | None = None) -> int:
    exit_code = 0
    try:
        while True:
            if api_process.poll() is not None:
                print("ERROR: API process exited unexpectedly.", file=sys.stderr)
                exit_code = 1
                break
            if ui_process is not None and ui_process.poll() is not None:
                print("ERROR: UI process exited unexpectedly.", file=sys.stderr)
                exit_code = 1
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        print("Shutting down...")
        _shutdown_process(ui_process)
        _shutdown_process(api_process)
        print("Shutdown complete.")

    return exit_code


def _run_dev(repo_root: str) -> int:
    print("Starting MTG Engine Dev Environment...")
    if not _print_runtime_paths():
        return 1

    if not _ensure_port_available(API_PORT):
        print(f"ERROR: Port {API_PORT} is unavailable.")
        return 1
    if not _ensure_port_available(DEV_UI_PORT):
        print(f"ERROR: Port {DEV_UI_PORT} is unavailable.")
        return 1

    api_process: subprocess.Popen | None = None
    ui_process: subprocess.Popen | None = None

    try:
        print(f"Starting API on {API_URL}")
        api_process = _start_api_process(repo_root)
        print(f"Starting UI on {UI_URL}")
        ui_process = _start_ui_dev_process(repo_root)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        _shutdown_process(ui_process)
        _shutdown_process(api_process)
        return 1

    print("Press Ctrl+C to stop everything.")
    time.sleep(2)
    webbrowser.open(f"{UI_URL}/")
    return _monitor_processes(api_process=api_process, ui_process=ui_process)


def _run_prod(repo_root: str) -> int:
    if not _ui_dist_exists(repo_root):
        print("UI not built. Run 'npm run build' in ui_harness first.")
        return 1

    print("Starting MTG Engine Production Server...")
    if not _print_runtime_paths():
        return 1
    if not _ensure_port_available(API_PORT):
        print(f"ERROR: Port {API_PORT} is unavailable.")
        return 1

    api_process: subprocess.Popen | None = None

    try:
        print(f"Starting API + UI static server on {API_URL}")
        api_process = _start_api_process(repo_root)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        _shutdown_process(api_process)
        return 1

    print("Press Ctrl+C to stop.")
    time.sleep(2)
    if os.getenv(NO_BROWSER_ENV, "0") != "1":
        webbrowser.open(f"{API_URL}/")
    return _monitor_processes(api_process=api_process, ui_process=None)


def main(argv: list[str] | None = None) -> int:
    signal.signal(signal.SIGINT, signal.default_int_handler)

    parser = argparse.ArgumentParser(description="Launch MTG Engine in dev or prod mode.")
    parser.add_argument(
        "--mode",
        choices=("dev", "prod"),
        default="dev",
        help="Launch mode. dev starts Vite + API, prod starts API static server.",
    )
    args = parser.parse_args(argv)

    repo_root = os.path.dirname(os.path.abspath(__file__))
    if args.mode == "prod":
        return _run_prod(repo_root)
    return _run_dev(repo_root)


if __name__ == "__main__":
    raise SystemExit(main())
