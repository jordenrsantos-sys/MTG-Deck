import hashlib
import re
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree


RULESET_ID = "cr_2026-01-16"
SOURCE_TITLE = "Magic Comprehensive Rules"
EFFECTIVE_DATE = "2026-01-16"
SOURCE_REL_PATH = Path("data/rules/source/MagicCompRules_20260116.docx")
DB_REL_PATH = Path("data/rules/rules.sqlite")
RULE_LINE_RE = re.compile(r"^(\d{3})\.(\d+)([a-z])?\s+(.*)$")


def normalize_paragraph_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def compute_sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_sort_key(section_str: str, main_str: str, letter: str | None) -> str:
    section = int(section_str)
    main = int(main_str)
    if letter:
        return f"{section:04d}.{main:06d}.{letter}"
    return f"{section:04d}.{main:06d}"


def read_docx_paragraphs(source_docx_path: Path) -> list[str]:
    try:
        from docx import Document

        doc = Document(str(source_docx_path))
        return [p.text for p in doc.paragraphs]
    except Exception:
        # Deterministic stdlib fallback if python-docx is unavailable.
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        with zipfile.ZipFile(source_docx_path, "r") as zf:
            xml_bytes = zf.read("word/document.xml")
        root = ElementTree.fromstring(xml_bytes)
        paragraphs: list[str] = []
        for para in root.findall(".//w:p", ns):
            text_parts: list[str] = []
            for text_node in para.findall(".//w:t", ns):
                if text_node.text:
                    text_parts.append(text_node.text)
            paragraphs.append("".join(text_parts))
        return paragraphs


def create_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        PRAGMA journal_mode = DELETE;
        PRAGMA synchronous = FULL;

        CREATE TABLE IF NOT EXISTS ruleset_source (
            ruleset_id TEXT PRIMARY KEY,
            source_title TEXT NOT NULL,
            effective_date TEXT NOT NULL,
            source_path TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            imported_at_utc TEXT NOT NULL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS rules_rule (
            ruleset_id TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            parent_rule_id TEXT NULL,
            section_id TEXT NULL,
            sort_key TEXT NOT NULL,
            rule_text TEXT NOT NULL,
            PRIMARY KEY (ruleset_id, rule_id)
        );

        CREATE INDEX IF NOT EXISTS idx_rules_rule_ruleset_section
            ON rules_rule (ruleset_id, section_id);

        CREATE INDEX IF NOT EXISTS idx_rules_rule_ruleset_parent
            ON rules_rule (ruleset_id, parent_rule_id);

        CREATE TABLE IF NOT EXISTS rules_glossary (
            ruleset_id TEXT NOT NULL,
            term TEXT NOT NULL,
            definition_text TEXT NOT NULL,
            PRIMARY KEY (ruleset_id, term)
        );

        CREATE TABLE IF NOT EXISTS rules_term_xref (
            ruleset_id TEXT NOT NULL,
            term TEXT NOT NULL,
            display_term TEXT NOT NULL,
            kind TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            PRIMARY KEY (ruleset_id, term, kind, rule_id)
        );

        CREATE INDEX IF NOT EXISTS idx_rules_term_xref_ruleset_term
            ON rules_term_xref (ruleset_id, term);

        CREATE VIRTUAL TABLE IF NOT EXISTS rules_rule_fts USING fts5(
            ruleset_id UNINDEXED,
            rule_id UNINDEXED,
            section_id UNINDEXED,
            rule_text
        );
        """
    )


def parse_rules_from_docx(source_docx_path: Path) -> list[tuple[str, str | None, str, str, str]]:
    paragraph_texts = read_docx_paragraphs(source_docx_path)
    parsed_rules: list[tuple[str, str | None, str, str, str]] = []

    for para_text in paragraph_texts:
        normalized = normalize_paragraph_text(para_text)
        if not normalized:
            continue

        m = RULE_LINE_RE.match(normalized)
        if not m:
            continue

        section_id = m.group(1)
        main_num = m.group(2)
        letter = m.group(3)
        rule_id = f"{section_id}.{main_num}{letter or ''}"
        parent_rule_id = f"{section_id}.{main_num}" if letter else None
        sort_key = make_sort_key(section_id, main_num, letter)

        parsed_rules.append((rule_id, parent_rule_id, section_id, sort_key, normalized))

    return parsed_rules


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    source_docx_path = (repo_root / SOURCE_REL_PATH).resolve()
    db_path = (repo_root / DB_REL_PATH).resolve()

    if not source_docx_path.exists():
        print(f"ERROR: source docx not found: {source_docx_path}", file=sys.stderr)
        return 1

    db_path.parent.mkdir(parents=True, exist_ok=True)

    source_bytes = source_docx_path.read_bytes()
    source_sha256 = compute_sha256_bytes(source_bytes)

    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(str(db_path))
    try:
        create_schema(con)

        imported_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        con.execute(
            """
            INSERT OR REPLACE INTO ruleset_source (
                ruleset_id, source_title, effective_date, source_path,
                source_sha256, imported_at_utc, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                RULESET_ID,
                SOURCE_TITLE,
                EFFECTIVE_DATE,
                SOURCE_REL_PATH.as_posix(),
                source_sha256,
                imported_at_utc,
                None,
            ),
        )

        parsed_rules = parse_rules_from_docx(source_docx_path)

        parsed_rules_sorted = sorted(parsed_rules, key=lambda r: (r[2], r[3], r[0]))
        con.executemany(
            """
            INSERT OR REPLACE INTO rules_rule (
                ruleset_id, rule_id, parent_rule_id, section_id, sort_key, rule_text
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (RULESET_ID, rule_id, parent_rule_id, section_id, sort_key, rule_text)
                for (rule_id, parent_rule_id, section_id, sort_key, rule_text) in parsed_rules_sorted
            ],
        )

        con.execute("DELETE FROM rules_rule_fts")
        con.execute(
            """
            INSERT INTO rules_rule_fts (ruleset_id, rule_id, section_id, rule_text)
            SELECT ruleset_id, rule_id, section_id, rule_text
            FROM rules_rule
            WHERE ruleset_id = ?
            ORDER BY section_id ASC, sort_key ASC, rule_id ASC
            """,
            (RULESET_ID,),
        )

        con.commit()

        ruleset_source_count = con.execute("SELECT COUNT(*) FROM ruleset_source").fetchone()[0]
        rules_rule_count = con.execute("SELECT COUNT(*) FROM rules_rule WHERE ruleset_id = ?", (RULESET_ID,)).fetchone()[0]
        rules_glossary_count = con.execute("SELECT COUNT(*) FROM rules_glossary WHERE ruleset_id = ?", (RULESET_ID,)).fetchone()[0]
        rules_term_xref_count = con.execute("SELECT COUNT(*) FROM rules_term_xref WHERE ruleset_id = ?", (RULESET_ID,)).fetchone()[0]
        rules_rule_fts_count = con.execute("SELECT COUNT(*) FROM rules_rule_fts WHERE ruleset_id = ?", (RULESET_ID,)).fetchone()[0]

        print(f"ruleset_source_count {ruleset_source_count}")
        print(f"rules_rule_count {rules_rule_count}")
        print(f"rules_glossary_count {rules_glossary_count}")
        print(f"rules_term_xref_count {rules_term_xref_count}")
        print(f"rules_rule_fts_count {rules_rule_fts_count}")
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
