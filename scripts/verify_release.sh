#!/usr/bin/env bash
# verify_release.sh — Phase 6 Stage 9 pre-release verification.
#
# Runs the v1.0 ship-readiness gate checks:
#   1. pytest baseline (>=747 collected)
#   2. vitest baseline (>=244)
#   3. Vite production build (exit 0 + bundle size <500kB)
#   4. Manifest SHA-256 (a63ae1feec8b... v1.0 baseline)
#   5. Calibration corpus SHA-256 (6d5c6af51ff7b... v1.0 baseline)
#   6. Perf-timing audit (exit 0 — per-layer + pipeline thresholds hold)
#   7. npm top-level dep list captured for documentation
#
# Exits 0 only when every check passes. Any FAIL output is a halt-and-investigate signal.
#
# Usage (run from repo/):
#   bash scripts/verify_release.sh
#
# Requires: python (with the engine's .venv activated OR globally available),
# node + npm, bash (Git-Bash or WSL on Windows; native bash on macOS/Linux).
#
# See `99_META/ROLLBACK_PLAN.md` for the full ship-readiness gate doc.

set -u  # error on unset variables; do NOT set -e (we want to capture per-check status)

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR" || { echo "FATAL: cannot cd to repo dir $REPO_DIR"; exit 2; }

# Baselines (v1.0 — update with each release)
EXPECTED_PYTEST_MIN=747
EXPECTED_VITEST_MIN=244
EXPECTED_BUNDLE_MAX_BYTES=512000  # 500 kB threshold
EXPECTED_MANIFEST_SHA="a63ae1feec8b9e3a"
EXPECTED_CORPUS_SHA="6d5c6af51ff7b"

PASS_COUNT=0
FAIL_COUNT=0
RESULTS=()

_check() {
  local label="$1"
  local status="$2"
  local detail="$3"
  if [[ "$status" == "PASS" ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    RESULTS+=("PASS  $label  $detail")
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    RESULTS+=("FAIL  $label  $detail")
  fi
}

echo "===== verify_release.sh — Phase 6 Stage 9 v1.0 ship-readiness gate ====="
echo "Repo: $REPO_DIR"
echo

# ----- Check 1: pytest baseline -----
echo "[1/7] pytest baseline (expect >=${EXPECTED_PYTEST_MIN})..."
PYTEST_OUT=$(python -m pytest --tb=no -q 2>&1 | tail -3)
PYTEST_COUNT=$(echo "$PYTEST_OUT" | grep -oE '[0-9]+ passed' | head -1 | grep -oE '[0-9]+')
if [[ -z "$PYTEST_COUNT" ]]; then
  _check "pytest" "FAIL" "could not parse pass count from: $PYTEST_OUT"
elif (( PYTEST_COUNT >= EXPECTED_PYTEST_MIN )); then
  _check "pytest" "PASS" "$PYTEST_COUNT passed (>= $EXPECTED_PYTEST_MIN)"
else
  _check "pytest" "FAIL" "$PYTEST_COUNT passed (< $EXPECTED_PYTEST_MIN)"
fi

# ----- Check 2: vitest baseline -----
echo "[2/7] vitest baseline (expect >=${EXPECTED_VITEST_MIN})..."
if [[ -d "ui_harness/node_modules" ]]; then
  VITEST_OUT=$(cd ui_harness && npx vitest run 2>&1 | tail -5)
  VITEST_COUNT=$(echo "$VITEST_OUT" | grep -oE '[0-9]+ passed' | tail -1 | grep -oE '[0-9]+')
  if [[ -z "$VITEST_COUNT" ]]; then
    _check "vitest" "FAIL" "could not parse pass count"
  elif (( VITEST_COUNT >= EXPECTED_VITEST_MIN )); then
    _check "vitest" "PASS" "$VITEST_COUNT passed (>= $EXPECTED_VITEST_MIN)"
  else
    _check "vitest" "FAIL" "$VITEST_COUNT passed (< $EXPECTED_VITEST_MIN)"
  fi
else
  _check "vitest" "FAIL" "ui_harness/node_modules missing — run 'npm install' first"
fi

# ----- Check 3: Vite production build -----
echo "[3/7] Vite production build (expect exit 0 + bundle <${EXPECTED_BUNDLE_MAX_BYTES} bytes)..."
if [[ -d "ui_harness/node_modules" ]]; then
  (cd ui_harness && npm run build) > /tmp/vite_build_out.txt 2>&1
  VITE_EXIT=$?
  if [[ $VITE_EXIT -ne 0 ]]; then
    _check "vite-build" "FAIL" "exit code $VITE_EXIT — see /tmp/vite_build_out.txt"
  else
    LARGEST_JS=$(find ui_harness/dist/assets -name "index-*.js" -type f 2>/dev/null | head -1)
    if [[ -z "$LARGEST_JS" ]]; then
      _check "vite-build" "FAIL" "exit 0 but no dist/assets/index-*.js produced"
    else
      BUNDLE_SIZE=$(stat -c%s "$LARGEST_JS" 2>/dev/null || stat -f%z "$LARGEST_JS" 2>/dev/null)
      if [[ -z "$BUNDLE_SIZE" ]]; then
        _check "vite-build" "FAIL" "could not stat bundle file $LARGEST_JS"
      elif (( BUNDLE_SIZE <= EXPECTED_BUNDLE_MAX_BYTES )); then
        _check "vite-build" "PASS" "exit 0; bundle ${BUNDLE_SIZE} bytes (<= ${EXPECTED_BUNDLE_MAX_BYTES})"
      else
        _check "vite-build" "FAIL" "exit 0 but bundle ${BUNDLE_SIZE} bytes exceeds ${EXPECTED_BUNDLE_MAX_BYTES}"
      fi
    fi
  fi
else
  _check "vite-build" "FAIL" "ui_harness/node_modules missing"
fi

# ----- Check 4: Manifest SHA-256 -----
echo "[4/7] Manifest SHA-256 (expect prefix ${EXPECTED_MANIFEST_SHA})..."
MANIFEST_FILE="api/engine/data/packs/curated_pack_manifest_v1.json"
if [[ -f "$MANIFEST_FILE" ]]; then
  MANIFEST_SHA=$(python -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$MANIFEST_FILE")
  if [[ "$MANIFEST_SHA" == "${EXPECTED_MANIFEST_SHA}"* ]]; then
    _check "manifest-sha" "PASS" "prefix matches (${MANIFEST_SHA:0:16}...)"
  else
    _check "manifest-sha" "FAIL" "expected prefix ${EXPECTED_MANIFEST_SHA}, got ${MANIFEST_SHA:0:16}"
  fi
else
  _check "manifest-sha" "FAIL" "manifest file missing: $MANIFEST_FILE"
fi

# ----- Check 5: Calibration corpus SHA-256 -----
echo "[5/7] Calibration corpus SHA-256 (expect prefix ${EXPECTED_CORPUS_SHA})..."
CORPUS_FILE="api/engine/data/calibration/external_decks_v1.json"
if [[ -f "$CORPUS_FILE" ]]; then
  CORPUS_SHA=$(python -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$CORPUS_FILE")
  if [[ "$CORPUS_SHA" == "${EXPECTED_CORPUS_SHA}"* ]]; then
    _check "corpus-sha" "PASS" "prefix matches (${CORPUS_SHA:0:16}...)"
  else
    _check "corpus-sha" "FAIL" "expected prefix ${EXPECTED_CORPUS_SHA}, got ${CORPUS_SHA:0:16}"
  fi
else
  _check "corpus-sha" "FAIL" "corpus file missing: $CORPUS_FILE"
fi

# ----- Check 6: Perf-timing audit -----
echo "[6/7] Perf-timing audit (expect exit 0)..."
python -m tools.perf.run_perf_timing_audit --iterations 10 > /tmp/perf_audit_out.txt 2>&1
PERF_EXIT=$?
if [[ $PERF_EXIT -eq 0 ]]; then
  _check "perf-audit" "PASS" "exit 0 (per-layer + pipeline thresholds hold)"
else
  _check "perf-audit" "FAIL" "exit code $PERF_EXIT — see /tmp/perf_audit_out.txt"
fi

# ----- Check 7: npm top-level deps captured -----
echo "[7/7] Capture npm top-level deps (documentation only; no assertion)..."
if [[ -d "ui_harness/node_modules" ]]; then
  (cd ui_harness && npm ls --depth=0) > /tmp/npm_ls_top.txt 2>&1
  TOP_COUNT=$(grep -cE '^[├└]' /tmp/npm_ls_top.txt 2>/dev/null || echo 0)
  _check "npm-deps-capture" "PASS" "$TOP_COUNT top-level deps captured to /tmp/npm_ls_top.txt"
else
  _check "npm-deps-capture" "FAIL" "ui_harness/node_modules missing"
fi

# ----- Summary -----
echo
echo "===== Summary ====="
for line in "${RESULTS[@]}"; do
  echo "$line"
done
echo
echo "PASS=$PASS_COUNT  FAIL=$FAIL_COUNT"
if (( FAIL_COUNT > 0 )); then
  echo "RELEASE GATE: FAIL — do not ship"
  exit 1
else
  echo "RELEASE GATE: PASS — ready to ship"
  exit 0
fi
