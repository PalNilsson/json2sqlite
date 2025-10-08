import sqlite3, json
from typing import Dict, Any, List, Optional, Tuple

def export_schema(db_path: str,
                  sample_json_rows: int = 50,
                  include_counts: bool = True) -> Dict[str, Any]:
    """
    Introspect a SQLite DB and return a dict with:
      - tables -> { name, create_sql, row_count?, columns[], primary_key[], foreign_keys[], indexes[], column_docs?, json_hints{} }
    JSON detection: for TEXT columns, samples up to `sample_json_rows` non-null values and tries json.loads.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # list all user tables
    cur.execute("""
        SELECT name, sql
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    tables = cur.fetchall()
    table_names = [r["name"] for r in tables]
    schema: Dict[str, Any] = {"tables": []}

    # helper: does a table exist?
    def has_table(name: str) -> bool:
        return name in table_names

    for t in tables:
        tname = t["name"]
        create_sql = t["sql"]

        # columns + pk info
        cur.execute(f"PRAGMA table_info('{tname}')")
        cols = [dict(r) for r in cur.fetchall()]
        pk_cols = [c["name"] for c in sorted(cols, key=lambda x: x["pk"]) if c["pk"] > 0]

        # foreign keys
        cur.execute(f"PRAGMA foreign_key_list('{tname}')")
        fks = [dict(r) for r in cur.fetchall()]

        # indexes
        cur.execute(f"PRAGMA index_list('{tname}')")
        idx_list = []
        for r in cur.fetchall():
            idx_name = r["name"]
            unique = bool(r["unique"])
            # columns in index
            cur.execute(f"PRAGMA index_info('{idx_name}')")
            idx_cols = [rr["name"] for rr in cur.fetchall()]
            idx_list.append({"name": idx_name, "unique": unique, "columns": idx_cols})

        # optional column docs (if you ever create <table>__column_docs)
        docs = {}
        docs_table = f"{tname}__column_docs"
        if has_table(docs_table):
            cur.execute(f"SELECT column_name, COALESCE(description,'') AS description FROM '{docs_table}'")
            docs = {r["column_name"]: r["description"] for r in cur.fetchall()}

        # row count (optional)
        row_count: Optional[int] = None
        if include_counts:
            try:
                cur.execute(f"SELECT COUNT(*) AS n FROM '{tname}'")
                row_count = cur.fetchone()["n"]
            except sqlite3.Error:
                row_count = None

        # JSON hints: detect JSON-y TEXT columns and extract top-level keys/types by sampling
        json_hints: Dict[str, Any] = {}
        for c in cols:
            col_name = c["name"]
            col_type = (c["type"] or "").upper()
            # Only try to detect JSON in TEXT/unknown columns
            if "TEXT" in col_type or col_type == "":
                try:
                    cur.execute(f"SELECT \"{col_name}\" AS v FROM '{tname}' WHERE \"{col_name}\" IS NOT NULL LIMIT ?", (sample_json_rows,))
                    values = [row["v"] for row in cur.fetchall()]
                except sqlite3.Error:
                    values = []

                parsed = []
                for v in values:
                    if isinstance(v, (bytes, bytearray)):
                        try:
                            v = v.decode("utf-8", "ignore")
                        except Exception:
                            continue
                    if isinstance(v, str) and v and v[0] in "[{":
                        try:
                            parsed.append(json.loads(v))
                        except Exception:
                            pass

                if parsed:
                    # collect top-level keys & value kinds
                    key_info: Dict[str, Dict[str, int]] = {}
                    list_item_kinds: Dict[str, int] = {}
                    for p in parsed:
                        if isinstance(p, dict):
                            for k, vv in p.items():
                                kind = _py_kind(vv)
                                key_info.setdefault(k, {}).setdefault(kind, 0)
                                key_info[k][kind] += 1
                        elif isinstance(p, list):
                            for it in p[:10]:
                                list_item_kinds.setdefault(_py_kind(it), 0)
                                list_item_kinds[_py_kind(it)] += 1

                    hint: Dict[str, Any] = {"detected": True}
                    if key_info:
                        # summarize top-level keys and dominant kinds
                        summarized = {
                            k: max(kinds.items(), key=lambda kv: kv[1])[0]  # pick most frequent kind
                            for k, kinds in key_info.items()
                        }
                        hint["top_level_keys"] = summarized
                    if list_item_kinds:
                        hint["list_item_kinds"] = list_item_kinds

                    json_hints[col_name] = hint

        schema["tables"].append({
            "name": tname,
            "create_sql": create_sql,
            "row_count": row_count,
            "columns": [{
                "name": c["name"],
                "type": c["type"],
                "notnull": bool(c["notnull"]),
                "default": c["dflt_value"],
                "pk_position": c["pk"],  # 0 if not PK; >0 indicates position in composite PK
                "doc": docs.get(c["name"])
            } for c in cols],
            "primary_key": pk_cols,
            "foreign_keys": fks,
            "indexes": idx_list,
            "json_hints": json_hints or None
        })

    conn.close()
    return schema

def _py_kind(v: Any) -> str:
    if v is None: return "null"
    if isinstance(v, bool): return "boolean"
    if isinstance(v, int): return "integer"
    if isinstance(v, float): return "real"
    if isinstance(v, str): return "string"
    if isinstance(v, dict): return "object"
    if isinstance(v, list): return "array"
    return "other"

def format_schema_for_llm(schema: Dict[str, Any]) -> str:
    """
    Produce a concise, LLM-friendly text description of the DB.
    """
    lines: List[str] = []
    for t in schema["tables"]:
        header = f"Table {t['name']}"
        if t.get("row_count") is not None:
            header += f" (rows: {t['row_count']})"
        lines.append(header + ":")
        # columns
        for c in t["columns"]:
            col = f"  - {c['name']} {c['type'] or ''}".rstrip()
            if c["pk_position"]:
                col += " PRIMARY KEY" + (f" (pos {c['pk_position']})" if c["pk_position"] > 1 else "")
            if c["notnull"]:
                col += " NOT NULL"
            if c["default"] is not None:
                col += f" DEFAULT {c['default']}"
            if c.get("doc"):
                col += f"  // {c['doc']}"
            # JSON hint
            jh = (t.get("json_hints") or {}).get(c["name"])
            if jh and jh.get("detected"):
                extras = []
                if jh.get("top_level_keys"):
                    tlk = ", ".join([f"{k}:{v}" for k, v in sorted(jh["top_level_keys"].items())])
                    extras.append(f"keys[{tlk}]")
                if jh.get("list_item_kinds"):
                    lik = ", ".join([f"{k}:{v}" for k, v in jh["list_item_kinds"].items()])
                    extras.append(f"list[{lik}]")
                if extras:
                    col += f"  (JSON: {', '.join(extras)})"
            lines.append(col)
        # foreign keys
        if t["foreign_keys"]:
            lines.append("  Foreign keys:")
            for fk in t["foreign_keys"]:
                lines.append(f"    - {fk['from']} -> {fk['table']}.{fk['to']} (on_update={fk['on_update']}, on_delete={fk['on_delete']})")
        # indexes
        if t["indexes"]:
            lines.append("  Indexes:")
            for idx in t["indexes"]:
                uniq = " UNIQUE" if idx["unique"] else ""
                lines.append(f"    -{uniq} {idx['name']} ({', '.join(idx['columns'])})")
        lines.append("")  # blank line
    # note about JSON1
    lines.append("Note: JSON columns can be queried via SQLite JSON1 (e.g., json_extract(col, '$.path')).")
    return "\n".join(lines)

# --- Example CLI usage ---
if __name__ == "__main__":
    import argparse, pathlib
    parser = argparse.ArgumentParser(description="Export SQLite schema (JSON + LLM text).")
    parser.add_argument("--db", required=True, help="Path to SQLite DB.")
    parser.add_argument("--json-out", help="Where to write schema JSON (optional).")
    parser.add_argument("--txt-out", help="Where to write LLM-friendly text (optional).")
    args = parser.parse_args()

    s = export_schema(args.db)
    if args.json_out:
        pathlib.Path(args.json_out).write_text(json.dumps(s, indent=2), encoding="utf-8")
        print(f"Wrote {args.json_out}")
    txt = format_schema_for_llm(s)
    if args.txt_out:
        pathlib.Path(args.txt_out).write_text(txt, encoding="utf-8")
        print(f"Wrote {args.txt_out}")
    else:
        print(txt)
