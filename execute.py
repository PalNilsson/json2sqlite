#!/usr/bin/env python3
import argparse
import sqlite3
import json
from typing import Any, Dict, Iterable, List, Optional

def run_sql_to_dicts(
    db_path: str,
    sql: str,
    params: Optional[Iterable[Any]] = None,
    *,
    json_columns: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Execute an SQL statement on a SQLite DB and return rows as dictionaries.
    JSON-looking strings are parsed into native Python objects by default.
    """

    def _maybe_json_parse(val: Any, col: str) -> Any:
        if not isinstance(val, str):
            return val
        s = val.strip()
        if not s:
            return val

        # If user specified explicit JSON columns, only parse those
        if json_columns is not None and col not in json_columns:
            return val

        # Try to parse JSON if it looks like JSON
        if s[0] in "{[" or s in ("true", "false", "null") or s.replace('.', '', 1).isdigit():
            try:
                return json.loads(s)
            except Exception:
                return val
        return val

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params or []))
        rows = cur.fetchall()

        result: List[Dict[str, Any]] = []
        for r in rows:
            as_dict: Dict[str, Any] = {}
            for col in r.keys():
                parsed = _maybe_json_parse(r[col], col)
                as_dict[col] = parsed
            result.append(as_dict)
        return result
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run SQL on a SQLite DB and return JSON-like dicts."
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database file.")
    parser.add_argument("--sql", required=True, help="SQL query to execute.")
    parser.add_argument(
        "--json-cols",
        nargs="*",
        help="Optional list of column names to parse as JSON (defaults to auto-detect).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Optional LIMIT to add to the SQL query if not already present."
    )
    parser.add_argument(
        "--pretty", action="store_true",
        help="Pretty-print JSON output."
    )
    args = parser.parse_args()

    sql = args.sql
    if args.limit and "limit" not in sql.lower():
        sql = f"{sql.strip().rstrip(';')} LIMIT {args.limit};"

    rows = run_sql_to_dicts(
        args.db,
        sql,
        json_columns=args.json_cols
    )

    if args.pretty:
        print(json.dumps(rows, indent=2))
    else:
        print(json.dumps(rows))


if __name__ == "__main__":
    main()
