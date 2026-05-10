"""Score-formula expression DSL — tokenizer + parser + evaluator.

Sub-task 2A.1 utility: a deterministic, content-agnostic expression evaluator for the
arithmetic-plus-comparison slice of the DSL surfaced in ``themes_v1_5.json`` during
sub-task 2's Reconnoiter pass. Pure tokenize -> parse -> AST-walk; identifier
resolution is closed-world (caller supplies ``known_identifiers``; the parser raises
on any identifier outside the set, in either LHS or RHS position).

Grammar (in scope this pass)::

    formula_statement     := IDENT EQ expression EOF
    threshold_expression  := IDENT comparison_op NUMBER EOF
    comparison_op         := GTE | GT | LT
    expression            := term (PLUS term)*
    term                  := factor (STAR factor)*
    factor                := NUMBER | IDENT | LPAREN expression RPAREN

Out of scope (architected for additive extension): boolean operators (AND/OR),
statement separators (``;``), float literals, negation, function calls.

Forbidden by construction: ``eval``, ``exec``, ``compile``, ``__import__``,
``importlib.*``, ``ast.literal_eval``. The accompanying test in
``repo/tests/test_score_formula_v1.py`` parses this module's own source via the
stdlib ``ast`` module and walks the resulting tree to assert none of those names
or attribute paths appear.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Mapping, Sequence, Set, Tuple, Union


SCORE_FORMULA_V1_VERSION = "score_formula_v1"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ScoreFormulaError(Exception):
    """Common base for every error this module raises."""


class ScoreFormulaParseError(ScoreFormulaError):
    """Tokenizer or parser failure (malformed input, unrecognized character)."""


class ScoreFormulaUnknownIdentifierError(ScoreFormulaParseError):
    """One or more identifiers in the AST are not in ``known_identifiers``.

    Subclass of :class:`ScoreFormulaParseError` so callers that catch parse
    errors also catch closed-world violations. The ``offenders`` attribute is a
    sorted tuple of unknown identifier names.
    """

    def __init__(self, offenders: Sequence[str]) -> None:
        self.offenders: Tuple[str, ...] = tuple(sorted(set(offenders)))
        joined = ", ".join(self.offenders)
        super().__init__(
            f"unknown identifier(s) not in closed-world allowlist: {joined}"
        )


class ScoreFormulaEvaluationError(ScoreFormulaError):
    """Evaluation-time failure (missing binding, internal inconsistency).

    Defense in depth: parse-time identifier validation should catch unknown
    identifiers before the evaluator runs, but the evaluator still raises this
    when a binding is absent so a misuse cannot silently return zero.
    """


# ---------------------------------------------------------------------------
# Token kinds + AST node types
# ---------------------------------------------------------------------------


TOK_NUMBER = "NUMBER"
TOK_IDENT = "IDENT"
TOK_PLUS = "PLUS"
TOK_STAR = "STAR"
TOK_LPAREN = "LPAREN"
TOK_RPAREN = "RPAREN"
TOK_EQ = "EQ"
TOK_GTE = "GTE"
TOK_GT = "GT"
TOK_LT = "LT"
TOK_EOF = "EOF"


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str
    position: int


@dataclass(frozen=True)
class Number:
    value: int


@dataclass(frozen=True)
class Identifier:
    name: str


@dataclass(frozen=True)
class BinOp:
    op: str  # "+" or "*"
    left: "_ExpressionNode"
    right: "_ExpressionNode"


@dataclass(frozen=True)
class Assignment:
    target: Identifier
    value: "_ExpressionNode"


@dataclass(frozen=True)
class Comparison:
    op: str  # ">=", ">", "<"
    left: Identifier
    right: Number


_ExpressionNode = Union[Number, Identifier, BinOp]
_StatementNode = Union[Assignment, Comparison]


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------


def _tokenize(source: str) -> List[_Token]:
    tokens: List[_Token] = []
    i = 0
    length = len(source)

    while i < length:
        ch = source[i]

        if ch in (" ", "\t", "\n", "\r"):
            i += 1
            continue

        if ch == "+":
            tokens.append(_Token(TOK_PLUS, "+", i))
            i += 1
            continue
        if ch == "*":
            tokens.append(_Token(TOK_STAR, "*", i))
            i += 1
            continue
        if ch == "(":
            tokens.append(_Token(TOK_LPAREN, "(", i))
            i += 1
            continue
        if ch == ")":
            tokens.append(_Token(TOK_RPAREN, ")", i))
            i += 1
            continue
        if ch == "=":
            tokens.append(_Token(TOK_EQ, "=", i))
            i += 1
            continue
        if ch == ">":
            if i + 1 < length and source[i + 1] == "=":
                tokens.append(_Token(TOK_GTE, ">=", i))
                i += 2
            else:
                tokens.append(_Token(TOK_GT, ">", i))
                i += 1
            continue
        if ch == "<":
            tokens.append(_Token(TOK_LT, "<", i))
            i += 1
            continue

        if ch.isdigit():
            start = i
            while i < length and source[i].isdigit():
                i += 1
            tokens.append(_Token(TOK_NUMBER, source[start:i], start))
            continue

        if ch.isalpha() or ch == "_":
            start = i
            while i < length and (source[i].isalnum() or source[i] == "_"):
                i += 1
            tokens.append(_Token(TOK_IDENT, source[start:i], start))
            continue

        raise ScoreFormulaParseError(
            f"unrecognized character {ch!r} at position {i}"
        )

    tokens.append(_Token(TOK_EOF, "", length))
    return tokens


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class _Parser:
    def __init__(self, tokens: Sequence[_Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    # ----- helpers --------------------------------------------------------

    def _peek(self, offset: int = 0) -> _Token:
        return self._tokens[self._pos + offset]

    def _advance(self) -> _Token:
        token = self._tokens[self._pos]
        self._pos += 1
        return token

    def _expect(self, kind: str) -> _Token:
        token = self._peek()
        if token.kind != kind:
            raise ScoreFormulaParseError(
                f"expected {kind} at position {token.position} but got "
                f"{token.kind} ({token.value!r})"
            )
        return self._advance()

    # ----- top-level ------------------------------------------------------

    def parse(self) -> _StatementNode:
        first = self._peek(0)
        if first.kind != TOK_IDENT:
            raise ScoreFormulaParseError(
                f"expected IDENT at position {first.position} but got "
                f"{first.kind} ({first.value!r})"
            )

        second = self._peek(1)
        if second.kind == TOK_EQ:
            statement: _StatementNode = self._parse_assignment()
        elif second.kind in (TOK_GTE, TOK_GT, TOK_LT):
            statement = self._parse_comparison()
        else:
            raise ScoreFormulaParseError(
                f"expected '=', '>=', '>' or '<' after IDENT at position "
                f"{second.position} but got {second.kind} ({second.value!r})"
            )

        eof = self._peek(0)
        if eof.kind != TOK_EOF:
            raise ScoreFormulaParseError(
                f"unexpected trailing input at position {eof.position}: "
                f"{eof.kind} ({eof.value!r})"
            )
        return statement

    # ----- statements -----------------------------------------------------

    def _parse_assignment(self) -> Assignment:
        target_tok = self._expect(TOK_IDENT)
        self._expect(TOK_EQ)
        value = self._parse_expression()
        return Assignment(target=Identifier(target_tok.value), value=value)

    def _parse_comparison(self) -> Comparison:
        left_tok = self._expect(TOK_IDENT)
        op_tok = self._advance()
        right_tok = self._expect(TOK_NUMBER)
        return Comparison(
            op=op_tok.value,
            left=Identifier(left_tok.value),
            right=Number(int(right_tok.value)),
        )

    # ----- expression grammar --------------------------------------------

    def _parse_expression(self) -> _ExpressionNode:
        node = self._parse_term()
        while self._peek().kind == TOK_PLUS:
            self._advance()
            right = self._parse_term()
            node = BinOp(op="+", left=node, right=right)
        return node

    def _parse_term(self) -> _ExpressionNode:
        node = self._parse_factor()
        while self._peek().kind == TOK_STAR:
            self._advance()
            right = self._parse_factor()
            node = BinOp(op="*", left=node, right=right)
        return node

    def _parse_factor(self) -> _ExpressionNode:
        token = self._peek()
        if token.kind == TOK_NUMBER:
            self._advance()
            return Number(int(token.value))
        if token.kind == TOK_IDENT:
            self._advance()
            return Identifier(token.value)
        if token.kind == TOK_LPAREN:
            self._advance()
            inner = self._parse_expression()
            self._expect(TOK_RPAREN)
            return inner
        raise ScoreFormulaParseError(
            f"expected NUMBER, IDENT or '(' at position {token.position} but got "
            f"{token.kind} ({token.value!r})"
        )


# ---------------------------------------------------------------------------
# Closed-world identifier validation
# ---------------------------------------------------------------------------


def _collect_identifiers(node: object) -> List[Identifier]:
    if isinstance(node, Identifier):
        return [node]
    if isinstance(node, Number):
        return []
    if isinstance(node, BinOp):
        return _collect_identifiers(node.left) + _collect_identifiers(node.right)
    if isinstance(node, Assignment):
        return [node.target] + _collect_identifiers(node.value)
    if isinstance(node, Comparison):
        return [node.left]
    raise ScoreFormulaEvaluationError(
        f"unexpected AST node type during identifier walk: {type(node).__name__}"
    )


def _enforce_closed_world(
    statement: _StatementNode, known_identifiers: Iterable[str]
) -> None:
    allowlist: Set[str] = set(known_identifiers)
    offenders: List[str] = []
    seen: Set[str] = set()
    for ident in _collect_identifiers(statement):
        name = ident.name
        if name in allowlist or name in seen:
            continue
        if name not in allowlist:
            offenders.append(name)
            seen.add(name)
    if offenders:
        raise ScoreFormulaUnknownIdentifierError(offenders)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------


def _resolve_binding(name: str, bindings: Mapping[str, object]) -> float:
    if name not in bindings:
        raise ScoreFormulaEvaluationError(
            f"missing binding for identifier {name!r}"
        )
    raw = bindings[name]
    if isinstance(raw, bool):
        # bool is a subclass of int in Python; refuse it explicitly so a True
        # binding cannot quietly become 1 in arithmetic context.
        raise ScoreFormulaEvaluationError(
            f"binding for {name!r} is bool; expected int or float"
        )
    if isinstance(raw, (int, float)):
        return float(raw)
    raise ScoreFormulaEvaluationError(
        f"binding for {name!r} has unsupported type {type(raw).__name__}"
    )


def _evaluate_expression(
    node: _ExpressionNode, bindings: Mapping[str, object]
) -> float:
    if isinstance(node, Number):
        return float(node.value)
    if isinstance(node, Identifier):
        return _resolve_binding(node.name, bindings)
    if isinstance(node, BinOp):
        left_value = _evaluate_expression(node.left, bindings)
        right_value = _evaluate_expression(node.right, bindings)
        if node.op == "+":
            return left_value + right_value
        if node.op == "*":
            return left_value * right_value
        raise ScoreFormulaEvaluationError(
            f"unsupported binary operator {node.op!r}"
        )
    raise ScoreFormulaEvaluationError(
        f"unexpected expression node type: {type(node).__name__}"
    )


def _apply_comparison(op: str, left_value: float, right_value: float) -> bool:
    if op == ">=":
        return left_value >= right_value
    if op == ">":
        return left_value > right_value
    if op == "<":
        return left_value < right_value
    raise ScoreFormulaEvaluationError(
        f"unsupported comparison operator {op!r}"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse(
    expression: str, known_identifiers: Iterable[str]
) -> _StatementNode:
    """Tokenize, parse, and closed-world-validate ``expression``.

    Returns the AST root (an :class:`Assignment` or :class:`Comparison`).
    Raises :class:`ScoreFormulaParseError` (or its
    :class:`ScoreFormulaUnknownIdentifierError` subclass) on malformed input or
    closed-world violations.
    """

    tokens = _tokenize(expression)
    statement = _Parser(tokens).parse()
    _enforce_closed_world(statement, known_identifiers)
    return statement


def evaluate(
    expression: str,
    bindings: Mapping[str, object],
    known_identifiers: Iterable[str],
) -> Union[float, bool]:
    """Parse + evaluate ``expression``.

    Returns ``float`` for an :class:`Assignment` (the RHS value) and ``bool``
    for a :class:`Comparison`. Caller is responsible for binding the
    assignment target (typically ``score``) before evaluating a subsequent
    threshold against the same ``bindings`` mapping.
    """

    statement = parse(expression, known_identifiers)
    if isinstance(statement, Assignment):
        return _evaluate_expression(statement.value, bindings)
    if isinstance(statement, Comparison):
        left_value = _resolve_binding(statement.left.name, bindings)
        right_value = float(statement.right.value)
        return _apply_comparison(statement.op, left_value, right_value)
    raise ScoreFormulaEvaluationError(
        f"unexpected statement node type: {type(statement).__name__}"
    )
