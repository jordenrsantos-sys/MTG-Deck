"""Synthetic-fixture unit tests + AST forbidden-construct verification for
``score_formula_v1``.

Sub-task 2A.1: themes_v1_5 smoke corpus is deferred to sub-task 2A.3 (pending
the identifier-curation decision in 2A.2). All fixtures here use small
synthetic identifier sets like ``{"X", "Y", "Z"}`` and integer bindings — the
evaluator's correctness is content-agnostic.
"""

from __future__ import annotations

import ast
import pathlib
from typing import Set

import pytest

from api.engine.scoring.score_formula_v1 import (
    Assignment,
    BinOp,
    Comparison,
    Identifier,
    Number,
    SCORE_FORMULA_V1_VERSION,
    ScoreFormulaError,
    ScoreFormulaEvaluationError,
    ScoreFormulaParseError,
    ScoreFormulaUnknownIdentifierError,
    evaluate,
    parse,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


XYZ_IDENTIFIERS: Set[str] = {"X", "Y", "Z", "score"}


# ---------------------------------------------------------------------------
# Tokenizer (exercised through parse)
# ---------------------------------------------------------------------------


class TestTokenizer:
    def test_unrecognized_character_raises_with_position(self) -> None:
        with pytest.raises(ScoreFormulaParseError) as info:
            parse("X = Y & Z", XYZ_IDENTIFIERS)
        assert "'&'" in str(info.value)
        assert "position 6" in str(info.value)

    def test_whitespace_is_ignored(self) -> None:
        node = parse("X = 1   +    2", XYZ_IDENTIFIERS)
        assert isinstance(node, Assignment)
        assert isinstance(node.value, BinOp) and node.value.op == "+"

    def test_multidigit_integer_literal(self) -> None:
        node = parse("X = 42", XYZ_IDENTIFIERS)
        assert isinstance(node, Assignment)
        assert node.value == Number(42)

    def test_gte_consumed_as_single_token(self) -> None:
        # If GTE were tokenized as GT then EQ, the parser would error.
        node = parse("X >= 5", XYZ_IDENTIFIERS)
        assert isinstance(node, Comparison) and node.op == ">="

    def test_underscore_identifier_accepted(self) -> None:
        ident_set = XYZ_IDENTIFIERS | {"FOO_BAR_BAZ"}
        node = parse("X = FOO_BAR_BAZ", ident_set)
        assert isinstance(node, Assignment)
        assert node.value == Identifier("FOO_BAR_BAZ")


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class TestParser:
    def test_simple_assignment(self) -> None:
        node = parse("X = 1", XYZ_IDENTIFIERS)
        assert node == Assignment(target=Identifier("X"), value=Number(1))

    def test_addition_left_associative(self) -> None:
        node = parse("X = 1 + 2 + 3", XYZ_IDENTIFIERS)
        # ((1 + 2) + 3)
        expected = Assignment(
            target=Identifier("X"),
            value=BinOp(
                op="+",
                left=BinOp(op="+", left=Number(1), right=Number(2)),
                right=Number(3),
            ),
        )
        assert node == expected

    def test_multiplication_binds_tighter_than_addition(self) -> None:
        node = parse("X = 2 + 3 * 4", XYZ_IDENTIFIERS)
        # (2 + (3 * 4))
        assert isinstance(node, Assignment)
        outer = node.value
        assert isinstance(outer, BinOp) and outer.op == "+"
        assert outer.left == Number(2)
        assert isinstance(outer.right, BinOp) and outer.right.op == "*"

    def test_parens_override_precedence(self) -> None:
        node = parse("X = (2 + 3) * 4", XYZ_IDENTIFIERS)
        # ((2 + 3) * 4)
        assert isinstance(node, Assignment)
        outer = node.value
        assert isinstance(outer, BinOp) and outer.op == "*"
        assert outer.right == Number(4)
        assert isinstance(outer.left, BinOp) and outer.left.op == "+"

    def test_nested_parens(self) -> None:
        node = parse("X = ((Y))", XYZ_IDENTIFIERS)
        assert isinstance(node, Assignment)
        assert node.value == Identifier("Y")

    def test_threshold_gte(self) -> None:
        node = parse("score >= 18", XYZ_IDENTIFIERS)
        assert node == Comparison(
            op=">=", left=Identifier("score"), right=Number(18)
        )

    def test_threshold_gt(self) -> None:
        node = parse("X > 0", XYZ_IDENTIFIERS)
        assert node == Comparison(
            op=">", left=Identifier("X"), right=Number(0)
        )

    def test_threshold_lt(self) -> None:
        node = parse("X < 10", XYZ_IDENTIFIERS)
        assert node == Comparison(
            op="<", left=Identifier("X"), right=Number(10)
        )

    def test_unbalanced_paren_raises(self) -> None:
        with pytest.raises(ScoreFormulaParseError):
            parse("X = (1 + 2", XYZ_IDENTIFIERS)

    def test_trailing_input_raises(self) -> None:
        with pytest.raises(ScoreFormulaParseError) as info:
            parse("X = 1 2", XYZ_IDENTIFIERS)
        assert "trailing" in str(info.value)

    def test_empty_input_raises(self) -> None:
        with pytest.raises(ScoreFormulaParseError):
            parse("", XYZ_IDENTIFIERS)

    def test_missing_op_after_ident_raises(self) -> None:
        with pytest.raises(ScoreFormulaParseError):
            parse("X", XYZ_IDENTIFIERS)

    def test_threshold_rhs_must_be_number(self) -> None:
        with pytest.raises(ScoreFormulaParseError):
            parse("X >= Y", XYZ_IDENTIFIERS)


# ---------------------------------------------------------------------------
# Closed-world identifier validation (parse-time)
# ---------------------------------------------------------------------------


class TestClosedWorldValidation:
    def test_unknown_rhs_identifier_raises_at_parse_time(self) -> None:
        with pytest.raises(ScoreFormulaUnknownIdentifierError) as info:
            parse("X = NOT_KNOWN", XYZ_IDENTIFIERS)
        assert info.value.offenders == ("NOT_KNOWN",)

    def test_unknown_lhs_identifier_also_raises(self) -> None:
        # LHS gating: closed-world applies to both positions.
        with pytest.raises(ScoreFormulaUnknownIdentifierError) as info:
            parse("UNKNOWN_LHS = 1", XYZ_IDENTIFIERS)
        assert info.value.offenders == ("UNKNOWN_LHS",)

    def test_unknown_lhs_in_threshold_raises(self) -> None:
        with pytest.raises(ScoreFormulaUnknownIdentifierError) as info:
            parse("UNKNOWN_THRESHOLD >= 5", XYZ_IDENTIFIERS)
        assert info.value.offenders == ("UNKNOWN_THRESHOLD",)

    def test_multiple_offenders_listed_sorted_and_deduped(self) -> None:
        with pytest.raises(ScoreFormulaUnknownIdentifierError) as info:
            parse(
                "X = AAA + BBB + AAA + BBB + CCC",
                XYZ_IDENTIFIERS,
            )
        assert info.value.offenders == ("AAA", "BBB", "CCC")

    def test_unknown_identifier_error_subclasses_parse_error(self) -> None:
        # Callers catching ScoreFormulaParseError also catch closed-world hits.
        with pytest.raises(ScoreFormulaParseError):
            parse("X = NOPE", XYZ_IDENTIFIERS)

    def test_known_identifiers_accepted(self) -> None:
        node = parse("X = Y * Z + Y", XYZ_IDENTIFIERS)
        assert isinstance(node, Assignment)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------


class TestEvaluator:
    def test_assignment_evaluates_constant_rhs(self) -> None:
        result = evaluate("X = 7", {}, XYZ_IDENTIFIERS)
        assert result == 7.0
        assert isinstance(result, float)

    def test_assignment_with_precedence(self) -> None:
        # X = 2 + 3 * Y, Y=4 -> 2 + 12 = 14
        result = evaluate("X = 2 + 3 * Y", {"Y": 4}, XYZ_IDENTIFIERS)
        assert result == 14.0

    def test_assignment_with_parens(self) -> None:
        # X = (2 + 3) * Y, Y=4 -> 5 * 4 = 20
        result = evaluate("X = (2 + 3) * Y", {"Y": 4}, XYZ_IDENTIFIERS)
        assert result == 20.0

    def test_assignment_with_zero_bindings(self) -> None:
        result = evaluate(
            "X = 2 * Y + 3 * Z", {"Y": 0, "Z": 0}, XYZ_IDENTIFIERS
        )
        assert result == 0.0

    def test_threshold_returns_bool_at_boundary_above(self) -> None:
        # score = 6, threshold score >= 5 -> True
        result = evaluate("score >= 5", {"score": 6}, XYZ_IDENTIFIERS)
        assert result is True

    def test_threshold_returns_bool_at_boundary_exact(self) -> None:
        # score = 5, threshold score >= 5 -> True
        result = evaluate("score >= 5", {"score": 5}, XYZ_IDENTIFIERS)
        assert result is True

    def test_threshold_returns_bool_at_boundary_below(self) -> None:
        # score = 4, threshold score >= 5 -> False
        result = evaluate("score >= 5", {"score": 4}, XYZ_IDENTIFIERS)
        assert result is False

    def test_threshold_strict_gt(self) -> None:
        assert evaluate("X > 0", {"X": 0}, XYZ_IDENTIFIERS) is False
        assert evaluate("X > 0", {"X": 1}, XYZ_IDENTIFIERS) is True

    def test_threshold_lt(self) -> None:
        assert evaluate("X < 10", {"X": 9}, XYZ_IDENTIFIERS) is True
        assert evaluate("X < 10", {"X": 10}, XYZ_IDENTIFIERS) is False

    def test_missing_binding_raises_evaluation_error(self) -> None:
        with pytest.raises(ScoreFormulaEvaluationError) as info:
            evaluate("X = Y", {}, XYZ_IDENTIFIERS)
        assert "Y" in str(info.value)

    def test_bool_binding_rejected(self) -> None:
        # bool subclasses int; refuse it so True does not silently become 1.
        with pytest.raises(ScoreFormulaEvaluationError):
            evaluate("X = Y", {"Y": True}, XYZ_IDENTIFIERS)

    def test_string_binding_rejected(self) -> None:
        with pytest.raises(ScoreFormulaEvaluationError):
            evaluate("X = Y", {"Y": "5"}, XYZ_IDENTIFIERS)

    def test_float_binding_accepted(self) -> None:
        result = evaluate("X = Y * 2", {"Y": 1.5}, XYZ_IDENTIFIERS)
        assert result == 3.0

    def test_evaluate_is_deterministic_across_repeat_calls(self) -> None:
        # Identical inputs -> identical output bytes.
        kwargs = {
            "expression": "X = 2 * Y + 3 * Z",
            "bindings": {"Y": 5, "Z": 7},
            "known_identifiers": XYZ_IDENTIFIERS,
        }
        first = evaluate(**kwargs)
        second = evaluate(**kwargs)
        assert first == second


# ---------------------------------------------------------------------------
# Public API + module surface
# ---------------------------------------------------------------------------


class TestModuleSurface:
    def test_version_constant(self) -> None:
        assert SCORE_FORMULA_V1_VERSION == "score_formula_v1"

    def test_exception_hierarchy(self) -> None:
        # Subclass relations specified in the module docstring.
        assert issubclass(ScoreFormulaParseError, ScoreFormulaError)
        assert issubclass(ScoreFormulaUnknownIdentifierError, ScoreFormulaParseError)
        assert issubclass(ScoreFormulaEvaluationError, ScoreFormulaError)

    def test_ast_node_dataclasses_are_frozen(self) -> None:
        # Frozen so AST nodes are hashable / immutable / safely shared.
        with pytest.raises(Exception):
            n = Number(1)
            n.value = 2  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Forbidden-construct AST walk over the module source
# ---------------------------------------------------------------------------


_FORBIDDEN_NAMES = frozenset({"eval", "exec", "compile", "__import__"})
_FORBIDDEN_ATTR_PATHS = frozenset({"ast.literal_eval"})
_FORBIDDEN_IMPORT_ROOTS = frozenset({"importlib", "ast"})


def _attribute_path(node: ast.Attribute) -> str:
    parts = [node.attr]
    current: ast.AST = node.value
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    parts.reverse()
    return ".".join(parts)


def _module_source_path() -> pathlib.Path:
    here = pathlib.Path(__file__).resolve()
    return here.parent.parent / "api" / "engine" / "scoring" / "score_formula_v1.py"


class TestForbiddenConstructsAreAbsentFromModuleSource:
    def test_module_source_file_resolves(self) -> None:
        path = _module_source_path()
        assert path.is_file(), f"module source not found at {path}"

    def test_no_forbidden_name_references(self) -> None:
        path = _module_source_path()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id in _FORBIDDEN_NAMES:
                offenders.append(f"{node.id} (line {node.lineno})")
        assert offenders == [], (
            f"forbidden name references found in module source: {offenders}"
        )

    def test_no_forbidden_attribute_paths(self) -> None:
        path = _module_source_path()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                full_path = _attribute_path(node)
                # Catch importlib.* in any attribute access path.
                head = full_path.split(".", 1)[0]
                if head == "importlib":
                    offenders.append(f"{full_path} (line {node.lineno})")
                if full_path in _FORBIDDEN_ATTR_PATHS:
                    offenders.append(f"{full_path} (line {node.lineno})")
        assert offenders == [], (
            f"forbidden attribute paths found in module source: {offenders}"
        )

    def test_no_forbidden_imports(self) -> None:
        path = _module_source_path()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    head = alias.name.split(".", 1)[0]
                    if head in _FORBIDDEN_IMPORT_ROOTS:
                        offenders.append(
                            f"import {alias.name} (line {node.lineno})"
                        )
            if isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                head = module_name.split(".", 1)[0]
                if head in _FORBIDDEN_IMPORT_ROOTS:
                    offenders.append(
                        f"from {module_name} import ... (line {node.lineno})"
                    )
        assert offenders == [], (
            f"forbidden imports found in module source: {offenders}"
        )

    def test_no_forbidden_call_expressions(self) -> None:
        # Belt-and-suspenders: a call like compile(...)() would still leave a
        # Name node, but explicit Call inspection makes the intent clear.
        path = _module_source_path()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id in _FORBIDDEN_NAMES:
                    offenders.append(f"{func.id}() (line {node.lineno})")
                if isinstance(func, ast.Attribute):
                    full_path = _attribute_path(func)
                    head = full_path.split(".", 1)[0]
                    if head == "importlib" or full_path in _FORBIDDEN_ATTR_PATHS:
                        offenders.append(f"{full_path}() (line {node.lineno})")
        assert offenders == [], (
            f"forbidden call expressions found in module source: {offenders}"
        )
