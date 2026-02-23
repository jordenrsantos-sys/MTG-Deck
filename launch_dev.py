import os
import signal
import subprocess
import sys
import time
import webbrowser

API_URL = "http://127.0.0.1:8000"
UI_URL = "http://127.0.0.1:5173"


def _popen_kwargs():
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def _start_process(command, cwd, name):
    try:
        return subprocess.Popen(command, cwd=cwd, **_popen_kwargs())
    except Exception as exc:
        raise RuntimeError(f"Failed to start {name} process: {exc}") from exc


def _wait_for_exit(process, timeout_seconds):
    try:
        process.wait(timeout=timeout_seconds)
        return True
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return process.poll() is not None


def _get_pid_on_port(port: int):
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


def _kill_pid(pid: str):
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


def _shutdown_process(process):
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


def main():
    signal.signal(signal.SIGINT, signal.default_int_handler)

    repo_root = os.path.dirname(os.path.abspath(__file__))
    ui_root = os.path.join(repo_root, "ui_harness")

    api_process = None
    ui_process = None

    print("Starting MTG Engine Dev Environment...")

    if not _ensure_port_available(8000):
        print("ERROR: Port 8000 is unavailable.")
        return 1
    if not _ensure_port_available(5173):
        print("ERROR: Port 5173 is unavailable.")
        return 1

    if os.name == "nt":
        npm_exe = "npm.cmd"
    else:
        npm_exe = "npm"

    print(f"Starting API on {API_URL}")

    try:
        api_process = _start_process(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "api.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
            ],
            cwd=repo_root,
            name="API",
        )

        print(f"Starting UI on {UI_URL}")
        ui_process = _start_process(
            [npm_exe, "run", "dev", "--", "--host", "127.0.0.1", "--port", "5173"],
            cwd=ui_root,
            name="UI",
        )

    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        _shutdown_process(ui_process)
        _shutdown_process(api_process)
        return 1

    print("Press Ctrl+C to stop everything.")

    time.sleep(2)
    webbrowser.open(f"{UI_URL}/")

    exit_code = 0

    try:
        while True:
            if api_process.poll() is not None:
                print("ERROR: API process exited unexpectedly.", file=sys.stderr)
                exit_code = 1
                break
            if ui_process.poll() is not None:
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


if __name__ == "__main__":
    raise SystemExit(main())
