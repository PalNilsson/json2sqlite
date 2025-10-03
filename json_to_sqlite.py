import argparse
import sqlite3
import json
from typing import Any, Dict, Iterable, Tuple, Optional
from pathlib import Path

# --------- Schema & typing ---------
SQLITE_TYPE_ORDER = ["INTEGER", "REAL", "TEXT"]  # escalation order


# --------- Public API ---------
def dicts_to_sqlite(
    db_path: str,
    table_name: str,
    dictionary1: Dict[str, Dict[str, Any]],
    dictionary2: Dict[str, str] = None,
) -> None:
    """
    Create/extend an SQLite table from `dictionary1` and store `dictionary2` as column docs.
    - dictionary1: { row_id: {col: value, ...}, ... }
    - dictionary2: { col: "description", ... } (optional)
    """
    if not dictionary1:
        raise ValueError("dictionary1 is empty.")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")

        # 1) Analyze data and infer schema
        col_types, col_samples = analyze_schema(dictionary1.values())

        # 2) Create table and helper tables
        create_main_table(conn, table_name, col_types)
        create_or_update_column_map(conn, table_name, col_types.keys())
        create_or_update_column_docs(conn, table_name)
        if dictionary2:
            upsert_column_docs(conn, table_name, dictionary2)

        # 3) Insert/Upsert rows
        upsert_rows(conn, table_name, dictionary1, col_types.keys())

        conn.commit()
    finally:
        conn.close()


# Infer SQLite type from a Python value
def infer_sqlite_type(value: Any) -> str:
    """Infer a safe SQLite type for a Python value (with JSON fallback)."""
    if value is None:
        # Unknown yet—will be resolved by escalation during union
        return "INTEGER"  # start narrow; can escalate later
    if isinstance(value, bool):
        # Store booleans as INTEGER 0/1 for SQLite friendliness
        return "INTEGER"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, (str, bytes)):
        return "TEXT"
    # Lists, dicts, tuples, sets, custom objects → JSON TEXT
    return "TEXT"

# Merge two SQLite affinities, escalating as needed
def merge_affinity(a: str, b: str) -> str:
    """Return a type that can hold both a and b, escalating as needed."""
    if a == b:
        return a
    # If either is TEXT, winner is TEXT
    if "TEXT" in (a, b):
        return "TEXT"
    # If one is REAL, winner is REAL
    if "REAL" in (a, b):
        return "REAL"
    # Otherwise INTEGER
    return "INTEGER"

#--------- Schema analysis ---------
def analyze_schema(rows: Iterable[Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """
    Walk all rows and infer a column type per column (union over rows).
    Returns (col_types, col_samples_for_docs)
    """
    col_types: Dict[str, str] = {}
    col_samples: Dict[str, Any] = {}

    for row in rows:
        for col, val in row.items():
            t = infer_sqlite_type(val)
            if col not in col_types:
                col_types[col] = t
                if val is not None:
                    col_samples[col] = val
            else:
                col_types[col] = merge_affinity(col_types[col], t)
                if col not in col_samples and val is not None:
                    col_samples[col] = val

    return col_types, col_samples

# --------- DDL ---------
def create_main_table(conn: sqlite3.Connection, table: str, col_types: Dict[str, str]) -> None:
    """
    Creates the main table if it doesn't exist, with:
      - record_id TEXT PRIMARY KEY
      - one column per inner key (nullable)
      - raw_json TEXT (full original row for traceability)
    """
    cols_sql = []
    for col, typ in col_types.items():
        cols_sql.append(f'"{col}" {typ}')

    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{table}" (
        record_id TEXT PRIMARY KEY,
        {", ".join(cols_sql)},
        raw_json TEXT
    );
    """
    conn.execute(ddl)

# --------- Column map ---------
def create_or_update_column_map(conn: sqlite3.Connection, table: str, cols: Iterable[str]) -> None:
    """
    Stores a mapping of original column names to themselves (and reserved for future renames).
    """
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table}__column_map" (
        original_name TEXT PRIMARY KEY,
        stored_name   TEXT NOT NULL
    );
    """)
    for c in cols:
        conn.execute(
            f'INSERT OR IGNORE INTO "{table}__column_map"(original_name, stored_name) VALUES (?, ?);',
            (c, c),
        )

# --------- Column documentation ---------
def create_or_update_column_docs(conn: sqlite3.Connection, table: str) -> None:
    """
    Table for per-column documentation that your LLM can read when generating SQL.
    """
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table}__column_docs" (
        column_name TEXT PRIMARY KEY,
        description TEXT
    );
    """)

# --------- Upsert column docs ---------
def upsert_column_docs(conn: sqlite3.Connection, table: str, docs: Dict[str, str]) -> None:
    for col, desc in docs.items():
        conn.execute(
            f'INSERT INTO "{table}__column_docs"(column_name, description) VALUES (?, ?) '
            f'ON CONFLICT(column_name) DO UPDATE SET description=excluded.description;',
            (col, desc),
        )


# --------- DML ---------
def to_db_scalar(value: Any) -> Any:
    """Convert Python value to something SQLite can store as per inferred type rules."""
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float, str, bytes)):
        return value
    # Fallback: JSON-serialize
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


# --------- Upsert rows ---------
def upsert_rows(
    conn: sqlite3.Connection,
    table: str,
    data: Dict[str, Dict[str, Any]],
    columns: Iterable[str],
) -> None:
    """
    Upsert rows:
      - record_id = outer key
      - set all known columns (others NULL)
      - store raw_json
    """
    cols = list(columns)
    placeholders = ", ".join(["?"] * (len(cols) + 2))  # + record_id + raw_json
    col_list = ", ".join([f'"{c}"' for c in cols])
    update_set = ", ".join([f'"{c}"=excluded."{c}"' for c in cols] + ['raw_json=excluded.raw_json'])

    sql = f"""
    INSERT INTO "{table}" (record_id, {col_list}, raw_json)
    VALUES ({placeholders})
    ON CONFLICT(record_id) DO UPDATE SET
        {update_set};
    """

    for record_id, row in data.items():
        values = [to_db_scalar(row.get(c)) for c in cols]
        raw = json.dumps(row, separators=(",", ":"), ensure_ascii=False)
        conn.execute(sql, [record_id, *values, raw])


# --------- Optional helpers for LLM prompting ---------
def get_column_docs_bundle(conn: sqlite3.Connection, table: str) -> str:
    """
    Returns a compact, LLM-friendly description of table columns for prompting.
    """
    cur = conn.execute(f'SELECT column_name, COALESCE(description, "") FROM "{table}__column_docs" ORDER BY column_name;')
    pairs = [f"{name}: {desc}".strip() for name, desc in cur.fetchall()]
    return f"Table {table} columns:\n" + "\n".join(pairs)


# --------- JSON file helper ---------
def _read_json(path: str | Path) -> Any:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


# --------- Annotations normalization helper ---------
def _normalize_annotations(d: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """
    Ensure all values are strings; coerce None / missing / falsy to "".
    Keys are stringified to be safe.
    """
    if not d:
        return {}
    out: Dict[str, str] = {}
    for k, v in d.items():
        if v is None or v is False:
            out[str(k)] = ""
        else:
            s = str(v)
            out[str(k)] = s if s is not None else ""
    return out


# --------- High-level function to load from files ---------
def dicts_to_sqlite_from_files(
    db_path: str,
    table_name: str,
    queuedata_path: str = "queuedata.json",
    annotations_path: Optional[str] = "annotated_queuedata.json",
) -> None:
    """
    Load dictionary1 from `queuedata.json` and dictionary2 from `annotated_queuedata.json`,
    normalize annotations (empty → ""), and write into SQLite via dicts_to_sqlite().
    """
    dictionary1 = _read_json(queuedata_path)
    if not isinstance(dictionary1, dict):
        raise ValueError(
            f"{queuedata_path} must be a JSON object of the form "
            "{ outer_key: { col: value, ... }, ... }"
        )

    dictionary2 = _read_json(annotations_path) if annotations_path and Path(annotations_path).exists() else {}
    if dictionary2 and not isinstance(dictionary2, dict):
        raise ValueError(
            f"{annotations_path} must be a JSON object of the form "
            "{ col: 'description', ... }"
        )

    dictionary2 = _normalize_annotations(dictionary2)

    # Optional: warn if some columns in dictionary1 have no annotation present
    # (not required, but often useful)
    # known_cols = set().union(*[set(v.keys()) for v in dictionary1.values()])
    # missing = sorted([c for c in known_cols if c not in dictionary2])
    # if missing:
    #     print(f"[info] {len(missing)} columns without annotations (stored as empty strings): {missing[:10]}{'...' if len(missing) > 10 else ''}")

    # Call your earlier function
    dicts_to_sqlite(db_path, table_name, dictionary1, dictionary2)


# --------- Example usage ---------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Load queuedata + annotations into SQLite.")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file (will be created if missing).")
    parser.add_argument("--table", required=True, help="Target table name, e.g., 'queuedata'.")
    parser.add_argument("--queuedata", default="queuedata.json", help="Path to queuedata.json (dictionary1).")
    parser.add_argument("--annotations", default="annotated_queuedata.json",
                        help="Path to annotated_queuedata.json (dictionary2). If absent, no docs are stored.")
    args = parser.parse_args()

    dicts_to_sqlite_from_files(args.db, args.table, args.queuedata, args.annotations)
    print(f"Loaded '{args.queuedata}' (+ annotations) into {args.db}:{args.table}")
