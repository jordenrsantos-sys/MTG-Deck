from __future__ import annotations

from launch import main as launch_main


def main() -> int:
    return launch_main(["--mode", "prod"])


if __name__ == "__main__":
    raise SystemExit(main())
