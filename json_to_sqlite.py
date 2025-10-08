import sqlite3
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

# --------- Public API ---------
def dicts_to_sqlite(
    db_path: str,
    table_name: str,
    dictionary1: Dict[str, Dict[str, Any]],
    dictionary2: Optional[Dict[str, str]] = None,  # kept optional; if None/empty => no docs table
) -> None:
    """
    Create/extend an SQLite table from `dictionary1`.
    - dictionary1: { row_id: {col: value, ...}, ... }
    - dictionary2: (optional) { col: "description", ... } — if provided and non-empty,
      a <table>__column_docs table will be created/updated. Otherwise it's skipped.
    """
    if not dictionary1:
        raise ValueError("dictionary1 is empty.")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")

        # 1) Infer schema
        col_types, _ = analyze_schema(dictionary1.values())

        # 2) Create main table + column map
        create_main_table(conn, table_name, col_types)
        create_or_update_column_map(conn, table_name, col_types.keys())

        # 3) Only create/populate column docs if provided (removed by default)
        if dictionary2:
            create_or_update_column_docs(conn, table_name)
            upsert_column_docs(conn, table_name, dictionary2)

        # 4) Upsert rows
        upsert_rows(conn, table_name, dictionary1, col_types.keys())

        conn.commit()
    finally:
        conn.close()

# --------- File loader (single JSON only) ---------
def dict_to_sqlite_from_file(
    db_path: str,
    table_name: str,
    queuedata_path: str = "queuedata.json",
) -> None:
    """
    Load `dictionary1` from `queuedata.json` and write into SQLite via dicts_to_sqlite().
    No annotations are read or stored.
    """
    dictionary1 = _read_json(queuedata_path)
    if not isinstance(dictionary1, dict) or not all(isinstance(v, dict) for v in dictionary1.values()):
        raise ValueError(
            f"{queuedata_path} must be a JSON object of the form "
            "{ outer_key: { col: value, ... }, ... }"
        )

    dicts_to_sqlite(db_path, table_name, dictionary1, dictionary2=None)

# --------- Helpers ---------
def _read_json(path: str | Path) -> Any:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

SQLITE_TYPE_ORDER = ["INTEGER", "REAL", "TEXT"]

def infer_sqlite_type(value: Any) -> str:
    if value is None:
        return "INTEGER"
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, (str, bytes)):
        return "TEXT"
    return "TEXT"  # JSON fallback for lists/dicts/etc.

def merge_affinity(a: str, b: str) -> str:
    if a == b:
        return a
    if "TEXT" in (a, b):
        return "TEXT"
    if "REAL" in (a, b):
        return "REAL"
    return "INTEGER"

def analyze_schema(rows: Iterable[Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, Any]]:
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
    cols_sql = [f'"{col}" {typ}' for col, typ in col_types.items()]
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{table}" (
        record_id TEXT PRIMARY KEY,
        {", ".join(cols_sql)},
        raw_json TEXT
    );
    """
    conn.execute(ddl)

def create_or_update_column_map(conn: sqlite3.Connection, table: str, cols: Iterable[str]) -> None:
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

# (Only used if dictionary2 is provided; not called otherwise)
def create_or_update_column_docs(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table}__column_docs" (
        column_name TEXT PRIMARY KEY,
        description TEXT
    );
    """)

def upsert_column_docs(conn: sqlite3.Connection, table: str, docs: Dict[str, str]) -> None:
    for col, desc in docs.items():
        conn.execute(
            f'INSERT INTO "{table}__column_docs"(column_name, description) VALUES (?, ?) '
            f'ON CONFLICT(column_name) DO UPDATE SET description=excluded.description;',
            (col, desc if desc is not None else ""),
        )

# --------- DML ---------
def to_db_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float, str, bytes)):
        return value
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

def upsert_rows(
    conn: sqlite3.Connection,
    table: str,
    data: Dict[str, Dict[str, Any]],
    columns: Iterable[str],
) -> None:
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
        conn.execute(sql, [str(record_id), *values, raw])

# --------- Optional CLI ---------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load queuedata (no annotations) into SQLite.")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file.")
    parser.add_argument("--table", required=True, help="Target table name, e.g., 'queuedata'.")
    parser.add_argument("--queuedata", default="queuedata.json", help="Path to queuedata.json (dictionary1).")
    args = parser.parse_args()

    dict_to_sqlite_from_file(args.db, args.table, args.queuedata)
    print(f"Loaded '{args.queuedata}' into {args.db}:{args.table}")
