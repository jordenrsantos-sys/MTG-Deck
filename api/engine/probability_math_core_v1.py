from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Any


PROBABILITY_MATH_CORE_V1_VERSION = "probability_math_core_v1"


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_int(name: str, value: Any) -> int:
    if not _is_int(value):
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            f"{name} must be int",
        )
    return int(value)


def _validate_nonnegative(name: str, value: int) -> None:
    if value < 0:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            f"{name} must be >= 0",
        )


def _round6_half_up(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _clamp_probability(value: Decimal) -> Decimal:
    if value < Decimal("0"):
        return Decimal("0")
    if value > Decimal("1"):
        return Decimal("1")
    return value


def comb(n: Any, k: Any) -> int:
    n_int = _require_int("n", n)
    k_int = _require_int("k", k)

    _validate_nonnegative("n", n_int)
    _validate_nonnegative("k", k_int)

    if k_int > n_int:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            "k must be <= n",
        )

    if k_int == 0 or k_int == n_int:
        return 1

    k_small = min(k_int, n_int - k_int)
    result = 1

    for index in range(1, k_small + 1):
        numerator = n_int - k_small + index
        result *= numerator

        if result % index != 0:
            raise _runtime_error(
                "PROBABILITY_MATH_CORE_V1_INTERNAL_ERROR",
                "comb division remainder detected",
            )

        result //= index

    return int(result)


def _validate_hypergeom_inputs(N: Any, K_int: Any, n: Any, x: Any) -> tuple[int, int, int, int]:
    n_total = _require_int("N", N)
    k_success = _require_int("K_int", K_int)
    draw_count = _require_int("n", n)
    threshold = _require_int("x", x)

    _validate_nonnegative("N", n_total)
    _validate_nonnegative("K_int", k_success)
    _validate_nonnegative("n", draw_count)
    _validate_nonnegative("x", threshold)

    if k_success > n_total:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            "K_int must be <= N",
        )

    if draw_count > n_total:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            "n must be <= N",
        )

    if threshold > draw_count:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            "x must be <= n",
        )

    return n_total, k_success, draw_count, threshold


def hypergeom_p_ge_x(N: Any, K_int: Any, n: Any, x: Any) -> float:
    n_total, k_success, draw_count, threshold = _validate_hypergeom_inputs(N=N, K_int=K_int, n=n, x=x)

    if threshold == 0:
        return 1.0

    max_hits = min(k_success, draw_count)
    if threshold > max_hits:
        return 0.0

    try:
        denominator = comb(n_total, draw_count)
        if denominator <= 0:
            raise _runtime_error(
                "PROBABILITY_MATH_CORE_V1_INTERNAL_ERROR",
                "hypergeom denominator must be positive",
            )

        total_probability = Decimal("0")
        with localcontext() as context:
            context.prec = 80
            denominator_decimal = Decimal(denominator)
            for hits in range(threshold, max_hits + 1):
                misses_drawn = draw_count - hits
                misses_available = n_total - k_success
                if misses_drawn < 0 or misses_drawn > misses_available:
                    continue

                numerator = comb(k_success, hits) * comb(misses_available, misses_drawn)
                total_probability += Decimal(numerator) / denominator_decimal

        return _round6_half_up(_clamp_probability(total_probability))
    except RuntimeError:
        raise
    except Exception as exc:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INTERNAL_ERROR",
            "hypergeom_p_ge_x deterministic computation failed",
        ) from exc


def hypergeom_p_ge_1(N: Any, K_int: Any, n: Any) -> float:
    n_total = _require_int("N", N)
    k_success = _require_int("K_int", K_int)
    draw_count = _require_int("n", n)

    _validate_nonnegative("N", n_total)
    _validate_nonnegative("K_int", k_success)
    _validate_nonnegative("n", draw_count)

    if k_success > n_total:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            "K_int must be <= N",
        )

    if draw_count > n_total:
        raise _runtime_error(
            "PROBABILITY_MATH_CORE_V1_INVALID_INPUT",
            "n must be <= N",
        )

    if draw_count == 0:
        return 0.0

    return hypergeom_p_ge_x(N=N, K_int=K_int, n=n, x=1)
