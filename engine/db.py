import json
import os
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_RELATIVE_PATH = Path("data") / "mtg.sqlite"
DB_PATH = (REPO_ROOT / DEFAULT_DB_RELATIVE_PATH).resolve()


def resolve_db_path() -> Path:
    env_db_path = os.getenv("MTG_ENGINE_DB_PATH")
    if isinstance(env_db_path, str) and env_db_path.strip() != "":
        candidate = Path(env_db_path.strip()).expanduser()
        if not candidate.is_absolute():
            candidate = (REPO_ROOT / candidate).resolve()
    else:
        candidate = DB_PATH

    if not candidate.is_file():
        raise RuntimeError(
            "MTG engine database file not found at "
            f"'{candidate}'. Set MTG_ENGINE_DB_PATH or ensure ./data/mtg.sqlite exists."
        )
    return candidate


class CommanderEligibilityUnknownError(RuntimeError):
    code = "COMMANDER_ELIGIBILITY_UNKNOWN"

    def __init__(
        self,
        snapshot_id: str | None,
        oracle_id: str | None,
        commander_name: str | None,
        taxonomy_version: str | None,
        missing_facet_keys: List[str],
    ):
        self.snapshot_id = snapshot_id if isinstance(snapshot_id, str) and snapshot_id else None
        self.oracle_id = oracle_id if isinstance(oracle_id, str) and oracle_id else None
        self.commander_name = commander_name if isinstance(commander_name, str) and commander_name else None
        self.taxonomy_version = taxonomy_version if isinstance(taxonomy_version, str) and taxonomy_version else None
        self.missing_facet_keys = [k for k in missing_facet_keys if isinstance(k, str) and k]
        super().__init__(
            "Commander eligibility cannot be determined from compiled card facets. "
            f"oracle_id={self.oracle_id} taxonomy_version={self.taxonomy_version} "
            f"missing_facet_keys={self.missing_facet_keys}"
        )

    def to_unknown(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "snapshot_id": self.snapshot_id,
            "oracle_id": self.oracle_id,
            "commander_name": self.commander_name,
            "taxonomy_version": self.taxonomy_version,
            "missing_facet_keys": self.missing_facet_keys,
            "message": (
                "Commander eligibility facet is missing/ambiguous in compiled tags. "
                "Do not infer from card text at runtime."
            ),
        }


def _parse_json_object(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(raw: Any) -> List[Any]:
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, str):
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _resolve_runtime_taxonomy_version(con: sqlite3.Connection, snapshot_id: str) -> str | None:
    try:
        row = con.execute(
            """
            SELECT taxonomy_version
            FROM card_tags
            WHERE snapshot_id = ?
            GROUP BY taxonomy_version
            ORDER BY taxonomy_version DESC
            LIMIT 1
            """,
            (snapshot_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None

    if row is not None and isinstance(row[0], str) and row[0] != "":
        return row[0]
    return None


def _lookup_card_tag_facets(
    con: sqlite3.Connection,
    snapshot_id: str,
    oracle_id: str,
    taxonomy_version: str,
) -> Dict[str, Any]:
    try:
        row = con.execute(
            """
            SELECT facets_json
            FROM card_tags
            WHERE snapshot_id = ?
              AND taxonomy_version = ?
              AND oracle_id = ?
            LIMIT 1
            """,
            (snapshot_id, taxonomy_version, oracle_id),
        ).fetchone()
    except sqlite3.OperationalError:
        return {}

    if row is None:
        return {}
    return _parse_json_object(row[0])


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"true", "yes", "y", "1", "legal", "eligible", "allowed", "ok"}:
            return True
        if token in {"false", "no", "n", "0", "illegal", "ineligible", "not_legal", "forbidden", "banned"}:
            return False
    return None


def _coerce_bool_from_collection(value: Any) -> bool | None:
    if isinstance(value, list):
        normalized = [str(item).strip().lower() for item in value if isinstance(item, (str, int, float, bool))]
        if any(token in {"legal", "eligible", "allowed", "true", "1", "yes"} for token in normalized):
            return True
        if any(token in {"illegal", "ineligible", "not_legal", "false", "0", "no", "forbidden"} for token in normalized):
            return False
        return None
    if isinstance(value, dict):
        for nested_key in ("legal", "eligible", "can_be_commander", "value", "status"):
            if nested_key in value:
                nested = _coerce_bool(value.get(nested_key))
                if nested is not None:
                    return nested
    return None


def _extract_commander_eligibility_from_facets(facets: Dict[str, Any]) -> Tuple[bool | None, str | None, List[str]]:
    candidate_keys = [
        "commander_legal",
        "commander_eligible",
        "can_be_commander",
        "legal_commander",
        "commander_status",
        "commander",
    ]

    for key in candidate_keys:
        if key not in facets:
            continue
        raw_value = facets.get(key)
        parsed = _coerce_bool(raw_value)
        if parsed is None:
            parsed = _coerce_bool_from_collection(raw_value)
        if parsed is not None:
            return parsed, key, []

    nested = facets.get("eligibility")
    if isinstance(nested, dict):
        nested_commander = nested.get("commander")
        parsed_nested = _coerce_bool(nested_commander)
        if parsed_nested is None:
            parsed_nested = _coerce_bool_from_collection(nested_commander)
        if parsed_nested is not None:
            return parsed_nested, "eligibility.commander", []

    return None, None, candidate_keys

def connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(resolve_db_path()))
    con.row_factory = sqlite3.Row
    return con

def snapshot_exists(snapshot_id: str) -> bool:
    with connect() as con:
        row = con.execute(
            "SELECT 1 FROM snapshots WHERE snapshot_id = ? LIMIT 1",
            (snapshot_id,)
        ).fetchone()
        return row is not None

def find_card_by_name(snapshot_id: str, name: str) -> Optional[Dict[str, Any]]:
    with connect() as con:
        row = con.execute(
            "SELECT snapshot_id, oracle_id, name, mana_cost, cmc, type_line, colors, color_identity, legalities_json, primitives_json "
            "FROM cards WHERE snapshot_id = ? AND LOWER(name) = LOWER(?) LIMIT 1",
            (snapshot_id, name)
        ).fetchone()
        card = dict(row) if row else None
        if card is not None:
            card["legalities"] = _parse_json_object(card.get("legalities_json"))
            card["primitives"] = _parse_json_list(card.get("primitives_json"))

            taxonomy_version = _resolve_runtime_taxonomy_version(con=con, snapshot_id=snapshot_id)
            oracle_id = card.get("oracle_id")
            if isinstance(oracle_id, str) and isinstance(taxonomy_version, str):
                card["tag_facets"] = _lookup_card_tag_facets(
                    con=con,
                    snapshot_id=snapshot_id,
                    oracle_id=oracle_id,
                    taxonomy_version=taxonomy_version,
                )
            else:
                card["tag_facets"] = {}
            card["taxonomy_version"] = taxonomy_version
        return card

def list_snapshots(limit: int = 20):
    with connect() as con:
        rows = con.execute(
            "SELECT snapshot_id, created_at, source, scryfall_bulk_updated_at "
            "FROM snapshots ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

def suggest_card_names(snapshot_id: str, query: str, limit: int = 5) -> list[str]:
    q = (query or "").strip().lower()
    if not q:
        return []

    # Deterministic: prefix first, then contains
    with connect() as con:
        prefix = con.execute(
            "SELECT name FROM cards "
            "WHERE snapshot_id = ? AND LOWER(name) LIKE ? "
            "ORDER BY name ASC LIMIT ?",
            (snapshot_id, q + "%", limit),
        ).fetchall()

        names = [r["name"] for r in prefix]

        if len(names) < limit:
            remaining = limit - len(names)
            contains = con.execute(
                "SELECT name FROM cards "
                "WHERE snapshot_id = ? AND LOWER(name) LIKE ? AND LOWER(name) NOT LIKE ? "
                "ORDER BY name ASC LIMIT ?",
                (snapshot_id, "%" + q + "%", q + "%", remaining),
            ).fetchall()
            names.extend([r["name"] for r in contains])

        return names

def is_legal_in_format(card: dict, fmt: str) -> tuple[bool, str]:
    legalities = card.get("legalities") or {}
    status = legalities.get(fmt)

    if status == "legal":
        return True, "legal"

    if status in ("banned", "not_legal"):
        return False, f"{card.get('name', 'Card')} is {status} in {fmt}"

    if status == "restricted":
        return False, f"{card.get('name', 'Card')} is restricted in {fmt}"

    return False, f"{card.get('name', 'Card')} legality unknown in {fmt}"

def is_legal_commander_card(card: Dict[str, Any]) -> tuple[bool, str]:
    facets = card.get("tag_facets") if isinstance(card.get("tag_facets"), dict) else {}
    eligible, source_key, missing_keys = _extract_commander_eligibility_from_facets(facets)

    if eligible is True:
        source = source_key if isinstance(source_key, str) else "commander_facet"
        return True, f"OK_{source.upper()}"

    if eligible is False:
        source = source_key if isinstance(source_key, str) else "commander_facet"
        return False, f"{source.upper()}_NOT_LEGAL"

    raise CommanderEligibilityUnknownError(
        snapshot_id=card.get("snapshot_id") if isinstance(card.get("snapshot_id"), str) else None,
        oracle_id=card.get("oracle_id") if isinstance(card.get("oracle_id"), str) else None,
        commander_name=card.get("name") if isinstance(card.get("name"), str) else None,
        taxonomy_version=card.get("taxonomy_version") if isinstance(card.get("taxonomy_version"), str) else None,
        missing_facet_keys=missing_keys,
    )


def commander_legality(snapshot_id: str, commander_name: str) -> tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Returns:
      (is_legal, reason_code, resolved_card_dict_or_none)
    """
    card = find_card_by_name(snapshot_id, commander_name)
    if card is None:
        return False, "UNKNOWN_COMMANDER", None

    try:
        ok, reason = is_legal_commander_card(card)
    except CommanderEligibilityUnknownError:
        return False, "COMMANDER_ELIGIBILITY_UNKNOWN", card
    if not ok:
        return False, "ILLEGAL_COMMANDER", card

    return True, reason, card

