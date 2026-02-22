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

## Runtime dependencies

Install runtime dependencies when running the API locally outside tests:

```bash
python -m pip install -r requirements.txt
```

Run the API server with module invocation (do not use a local `uvicorn` file):

```bash
python -m uvicorn api.main:app --reload
```

## 3) Run tests

```bash
python -m pytest -q
```

## Why versions are pinned

Pinning exact test tool versions (`pytest`, `pytest-cov`) keeps local and CI behavior aligned and reduces non-deterministic failures caused by upstream toolchain changes. The dev requirements also include `httpx`, which is required by `fastapi.testclient` so endpoint integration tests run locally instead of being skipped.
