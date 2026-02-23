import sys
import urllib.error
import urllib.request

BASE_URL = "http://127.0.0.1:8000"


def _fetch_status_and_headers(url: str):
    try:
        with urllib.request.urlopen(url, timeout=3.0) as response:
            return response.getcode(), response.headers
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers


def _run_check(name: str, url: str, expected_status: int, expected_content_type: str | None = None):
    status_code, headers = _fetch_status_and_headers(url)
    content_type = ""
    if headers is not None:
        content_type = headers.get("Content-Type", "")

    status_ok = status_code == expected_status
    content_type_ok = True
    if expected_content_type is not None:
        content_type_ok = expected_content_type.lower() in content_type.lower()

    passed = status_ok and content_type_ok
    detail_parts = [f"status={status_code}"]
    if expected_content_type is not None:
        detail_parts.append(f"content_type={content_type or '(missing)'}")
    detail = ", ".join(detail_parts)

    print(f"[{ 'PASS' if passed else 'FAIL' }] {name}: {detail}")
    return passed


def main() -> int:
    checks = [
        ("GET /", f"{BASE_URL}/", 200, "text/html"),
        ("GET /health", f"{BASE_URL}/health", 200, None),
        ("GET /api/does-not-exist", f"{BASE_URL}/api/does-not-exist", 404, None),
        ("GET /cards/does-not-exist", f"{BASE_URL}/cards/does-not-exist", 404, None),
    ]

    total = len(checks)
    passed = 0

    for check in checks:
        if _run_check(*check):
            passed += 1

    failed = total - passed
    print(f"Summary: passed={passed}, failed={failed}, total={total}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
