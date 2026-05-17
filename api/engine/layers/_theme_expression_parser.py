"""
_theme_expression_parser — bounded expression evaluator for theme classification.

Used by deck_theme_classifier_v1.py to evaluate expressions like:
  required_signals: "(STACK_COUNTERSPELL+TARGETED_REMOVAL_CREATURE)>=12 AND CARD_DRAW_REPEATABLE>=3"
  score_formula:    "score=1*(STACK_COUNTERSPELL+TARGETED_REMOVAL_CREATURE) + 2*BOARDWIPE_CREATURES"
  classify_threshold: "score>=22"
  anti_signals:     "AGGRO_CONVERSION>=1 AND INTERACTION_TOTAL<10"
  optional_boosters: "SILENCE_EFFECT>=1; TAX_STACK_OR_SPELLS>=1"

The grammar is bounded — confirmed by Phase 2.1 Stage 0 audit (2026-05-16):
  - Operators: + * >= > <= < = AND OR ( ) ;
  - Operands: integer/float literals + UPPERCASE_IDENTIFIER lookups
  - Score formulas use 'score = EXPR' (assignment binding; we evaluate the RHS only)
  - Semicolon separates clauses (combined with implicit AND)
  - No nested function calls in main themes; concrete typal themes substitute the
    placeholder subtype at data-pack-generation time, so the runtime parser sees
    plain identifiers like TRIBAL_PAYOFFS.

Custom mini-parser deliberately chosen over jinja2/sympy/asteval to avoid a new
runtime dependency. The grammar is small enough (~10 token types) to hand-roll.

Public surface:
  - tokenize(text: str) -> List[Token]
  - parse_expression(text: str) -> AstNode
  - evaluate(node: AstNode, lookup: Callable[[str], float]) -> float | bool
  - evaluate_predicate(text: str, lookup: Callable[[str], float]) -> bool
  - evaluate_score(text: str, lookup: Callable[[str], float]) -> float

`lookup` is called for each identifier; should return a numeric count for primitives,
signals, or the special name 'score' (caller-managed). Unknown identifiers should
either return 0 (lenient) or raise UnknownIdentifier (strict) — caller chooses.

Calibration-honest: any caller MUST NOT fabricate identifier values. If a primitive
isn't present in the deck's primitive_index, return 0 (the natural absence count).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Union


class ThemeExpressionError(ValueError):
    """Raised when an expression can't be parsed or evaluated."""


class UnknownIdentifier(ThemeExpressionError):
    """Raised when lookup is invoked for an identifier not in the variable namespace."""


# ====== Tokenizer ======

TOK_NUM = "NUM"
TOK_IDENT = "IDENT"
TOK_OP = "OP"
TOK_LPAREN = "LPAREN"
TOK_RPAREN = "RPAREN"
TOK_AND = "AND"
TOK_OR = "OR"
TOK_SEMI = "SEMI"
TOK_EOF = "EOF"

# Multi-char operators must be tried before single-char.
_OPERATORS_BY_LEN = [
    (">=", "GE"),
    ("<=", "LE"),
    ("==", "EQ"),
    ("!=", "NE"),
    (">", "GT"),
    ("<", "LT"),
    ("=", "EQ"),
    ("+", "ADD"),
    ("-", "SUB"),
    ("*", "MUL"),
    ("/", "DIV"),
]


@dataclass
class Token:
    kind: str
    value: Any
    pos: int


def tokenize(text: str) -> List[Token]:
    """Lex `text` into a flat token stream. Whitespace and commas are ignored."""
    out: List[Token] = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c.isspace() or c == ",":
            i += 1
            continue
        if c == "(":
            out.append(Token(TOK_LPAREN, "(", i))
            i += 1
            continue
        if c == ")":
            out.append(Token(TOK_RPAREN, ")", i))
            i += 1
            continue
        if c == ";":
            out.append(Token(TOK_SEMI, ";", i))
            i += 1
            continue
        # Multi-char operator pass
        matched_op = False
        for sym, name in _OPERATORS_BY_LEN:
            if text[i:i + len(sym)] == sym:
                out.append(Token(TOK_OP, name, i))
                i += len(sym)
                matched_op = True
                break
        if matched_op:
            continue
        # Number (int or float)
        m = re.match(r"\d+(?:\.\d+)?", text[i:])
        if m:
            tok_text = m.group(0)
            val = float(tok_text) if "." in tok_text else int(tok_text)
            out.append(Token(TOK_NUM, val, i))
            i += len(tok_text)
            continue
        # Identifier or keyword (UPPERCASE or lowercase; keywords are AND/OR/score)
        m = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[i:])
        if m:
            tok_text = m.group(0)
            upper = tok_text.upper()
            if upper == "AND":
                out.append(Token(TOK_AND, "AND", i))
            elif upper == "OR":
                out.append(Token(TOK_OR, "OR", i))
            else:
                out.append(Token(TOK_IDENT, tok_text, i))
            i += len(tok_text)
            continue
        raise ThemeExpressionError(f"Unexpected character {c!r} at position {i} in {text!r}")
    out.append(Token(TOK_EOF, None, n))
    return out


# ====== AST ======

@dataclass
class NumNode:
    value: Union[int, float]


@dataclass
class IdentNode:
    name: str


@dataclass
class BinOpNode:
    op: str   # "ADD", "SUB", "MUL", "DIV", "GE", "GT", "LE", "LT", "EQ", "NE", "AND", "OR"
    left: Any
    right: Any


@dataclass
class AssignNode:
    """`score = EXPR` — we ignore the LHS and return the RHS evaluation."""
    name: str
    rhs: Any


AstNode = Union[NumNode, IdentNode, BinOpNode, AssignNode]


# ====== Parser (recursive descent) ======

class _Parser:
    def __init__(self, tokens: List[Token]) -> None:
        self._toks = tokens
        self._pos = 0

    def _peek(self, k: int = 0) -> Token:
        return self._toks[self._pos + k] if self._pos + k < len(self._toks) else self._toks[-1]

    def _advance(self) -> Token:
        t = self._toks[self._pos]
        self._pos += 1
        return t

    def _accept(self, kind: str, value: Optional[Any] = None) -> Optional[Token]:
        t = self._peek()
        if t.kind != kind:
            return None
        if value is not None and t.value != value:
            return None
        return self._advance()

    def _expect(self, kind: str, value: Optional[Any] = None) -> Token:
        t = self._accept(kind, value)
        if t is None:
            actual = self._peek()
            raise ThemeExpressionError(
                f"Expected {kind}{f'={value}' if value else ''}, got {actual.kind}={actual.value!r} at pos {actual.pos}"
            )
        return t

    # statement := assignment | or_expression
    # assignment := IDENT '=' or_expression
    def parse_statement(self) -> AstNode:
        # Lookahead for assignment: IDENT '='
        if self._peek().kind == TOK_IDENT and self._peek(1).kind == TOK_OP and self._peek(1).value == "EQ":
            name_tok = self._advance()
            self._advance()  # consume '='
            rhs = self.parse_or()
            return AssignNode(name=name_tok.value, rhs=rhs)
        return self.parse_or()

    # or_expression := and_expression ( 'OR' and_expression )*
    def parse_or(self) -> AstNode:
        left = self.parse_and()
        while self._peek().kind == TOK_OR:
            self._advance()
            right = self.parse_and()
            left = BinOpNode(op="OR", left=left, right=right)
        return left

    # and_expression := comparison ( 'AND' comparison )*
    def parse_and(self) -> AstNode:
        left = self.parse_comparison()
        while self._peek().kind == TOK_AND:
            self._advance()
            right = self.parse_comparison()
            left = BinOpNode(op="AND", left=left, right=right)
        return left

    # comparison := additive ( COMP_OP additive )?
    def parse_comparison(self) -> AstNode:
        left = self.parse_additive()
        if self._peek().kind == TOK_OP and self._peek().value in ("GE", "GT", "LE", "LT", "EQ", "NE"):
            op_tok = self._advance()
            right = self.parse_additive()
            return BinOpNode(op=op_tok.value, left=left, right=right)
        return left

    # additive := multiplicative ( ('+' | '-') multiplicative )*
    def parse_additive(self) -> AstNode:
        left = self.parse_multiplicative()
        while self._peek().kind == TOK_OP and self._peek().value in ("ADD", "SUB"):
            op_tok = self._advance()
            right = self.parse_multiplicative()
            left = BinOpNode(op=op_tok.value, left=left, right=right)
        return left

    # multiplicative := primary ( ('*' | '/') primary )*
    def parse_multiplicative(self) -> AstNode:
        left = self.parse_primary()
        while self._peek().kind == TOK_OP and self._peek().value in ("MUL", "DIV"):
            op_tok = self._advance()
            right = self.parse_primary()
            left = BinOpNode(op=op_tok.value, left=left, right=right)
        return left

    # primary := NUM | IDENT | '(' or_expression ')' | IDENT '(' or_expression ')' [function call]
    def parse_primary(self) -> AstNode:
        t = self._peek()
        if t.kind == TOK_NUM:
            self._advance()
            return NumNode(value=t.value)
        if t.kind == TOK_LPAREN:
            self._advance()
            inner = self.parse_or()
            self._expect(TOK_RPAREN)
            return inner
        if t.kind == TOK_IDENT:
            self._advance()
            # Function-call form: IDENT '(' IDENT ')' — present in template typal_themes
            # but concrete typal entries don't use it. We synthesize a derived name
            # "IDENT_OF_ARG" for lookup, which the classifier can handle as a derived
            # metric. Concrete callers shouldn't hit this branch.
            if self._peek().kind == TOK_LPAREN:
                self._advance()
                arg_tok = self._expect(TOK_IDENT)
                self._expect(TOK_RPAREN)
                return IdentNode(name=f"{t.value}__OF__{arg_tok.value}")
            return IdentNode(name=t.value)
        raise ThemeExpressionError(f"Unexpected token {t.kind}={t.value!r} at pos {t.pos}")


def parse_expression(text: str) -> AstNode:
    """Parse a single-clause expression (no semicolons). Use parse_clauses for multi-clause."""
    tokens = tokenize(text)
    p = _Parser(tokens)
    node = p.parse_statement()
    if p._peek().kind != TOK_EOF:
        raise ThemeExpressionError(
            f"Trailing tokens after expression: {p._peek().kind}={p._peek().value!r} in {text!r}"
        )
    return node


def parse_clauses(text: str) -> List[AstNode]:
    """Parse semicolon-separated clauses. Each clause is a full statement."""
    if not text or not text.strip():
        return []
    parts = [p.strip() for p in text.split(";") if p.strip()]
    return [parse_expression(p) for p in parts]


# ====== Evaluator ======

LookupFn = Callable[[str], Union[int, float]]


def _to_number(value: Any) -> float:
    """Coerce a value to a float for arithmetic. Booleans → 1.0/0.0."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    raise ThemeExpressionError(f"Cannot use non-numeric value {value!r} in arithmetic")


def evaluate(node: AstNode, lookup: LookupFn) -> Union[int, float, bool]:
    """Evaluate `node` using `lookup` for identifier resolution."""
    if isinstance(node, NumNode):
        return node.value
    if isinstance(node, IdentNode):
        return lookup(node.name)
    if isinstance(node, AssignNode):
        # `score = EXPR` — discard binding name (it's a label, not state), return RHS value
        return evaluate(node.rhs, lookup)
    if isinstance(node, BinOpNode):
        if node.op in ("AND", "OR"):
            left = evaluate(node.left, lookup)
            right = evaluate(node.right, lookup)
            # Coerce to truthy: non-zero numbers are truthy
            left_truthy = (left if isinstance(left, bool) else bool(_to_number(left) != 0))
            right_truthy = (right if isinstance(right, bool) else bool(_to_number(right) != 0))
            if node.op == "AND":
                return left_truthy and right_truthy
            return left_truthy or right_truthy
        left = _to_number(evaluate(node.left, lookup))
        right = _to_number(evaluate(node.right, lookup))
        if node.op == "ADD":
            return left + right
        if node.op == "SUB":
            return left - right
        if node.op == "MUL":
            return left * right
        if node.op == "DIV":
            return left / right if right != 0 else 0.0
        if node.op == "GE":
            return left >= right
        if node.op == "GT":
            return left > right
        if node.op == "LE":
            return left <= right
        if node.op == "LT":
            return left < right
        if node.op == "EQ":
            return left == right
        if node.op == "NE":
            return left != right
        raise ThemeExpressionError(f"Unknown operator {node.op!r}")
    raise ThemeExpressionError(f"Unknown AST node type {type(node).__name__}")


def evaluate_predicate(text: str, lookup: LookupFn) -> bool:
    """Evaluate a boolean expression. Empty text → True (no constraints)."""
    if not text or not text.strip():
        return True
    clauses = parse_clauses(text)
    # Multiple clauses combined with implicit AND
    for clause in clauses:
        result = evaluate(clause, lookup)
        if isinstance(result, bool):
            if not result:
                return False
        elif _to_number(result) == 0:
            return False
    return True


def evaluate_score(text: str, lookup: LookupFn) -> float:
    """Evaluate a score formula. Returns 0.0 on empty text."""
    if not text or not text.strip():
        return 0.0
    node = parse_expression(text)
    val = evaluate(node, lookup)
    return _to_number(val)


# ====== Defensive lookup helper ======

def make_lenient_lookup(values: dict) -> LookupFn:
    """Return a lookup function that returns values[name] or 0.0 for unknowns.

    Use this when classifying real decks — primitives that aren't present in the
    deck are naturally absent (count 0), not errors.
    """
    def _lookup(name: str) -> float:
        if name in values:
            return float(values[name])
        return 0.0
    return _lookup


def make_strict_lookup(values: dict) -> LookupFn:
    """Return a lookup function that raises UnknownIdentifier for unknowns.

    Use this when validating data files — every identifier should resolve to a
    known primitive/signal/derived metric.
    """
    def _lookup(name: str) -> float:
        if name in values:
            return float(values[name])
        raise UnknownIdentifier(name)
    return _lookup
