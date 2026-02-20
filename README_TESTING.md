# Testing

This project keeps test tooling pinned for deterministic contributor setups.

## 1) Create a virtual environment

### Windows (PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### macOS / Linux
```bash
python -m venv .venv
source .venv/bin/activate
```

## 2) Install dev/test dependencies

```bash
python -m pip install -r requirements-dev.txt
```

## 3) Run tests

```bash
python -m pytest -q
```

## Why versions are pinned

Pinning exact test tool versions (`pytest`, `pytest-cov`) keeps local and CI behavior aligned and reduces non-deterministic failures caused by upstream toolchain changes.
