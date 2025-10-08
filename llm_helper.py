from typing import Dict, Any, Iterable, Optional, Mapping
import fnmatch

from export_schema import export_schema

def emit_minimal_schema_txt(
    schema: Dict[str, Any],
    include_tables: Optional[Iterable[str]] = None,          # e.g. ["queuedata", "jobs*"]
    columns_by_table: Optional[Mapping[str, Iterable[str]]] = None,  # e.g. {"queuedata": ["record_id","acopytools"]}
    include_json_hints: bool = True,
    include_counts: bool = False,
) -> str:
    """
    Produce a concise, LLM-friendly schema description, limited to selected tables/columns.

    Args:
      schema: dict returned by export_schema(db_path)
      include_tables: table names or glob patterns; None = all tables
      columns_by_table: optional {table: [col,...]} to further restrict columns
      include_json_hints: append JSON key hints when detected by export_schema
      include_counts: show row counts (if computed by export_schema)

    Returns:
      str with lines like:
        Table queuedata (rows: 123):
          - record_id TEXT PRIMARY KEY
          - acopytools TEXT  (JSON: keys[pr,array; pw,array; ...])
    """
    def table_selected(name: str) -> bool:
        if not include_tables:
            return True
        return any(fnmatch.fnmatch(name, pat) for pat in include_tables)

    lines = []
    any_json = False

    for t in schema.get("tables", []):
        tname = t["name"]
        if not table_selected(tname):
            continue

        header = f"Table {tname}"
        if include_counts and t.get("row_count") is not None:
            header += f" (rows: {t['row_count']})"
        lines.append(header + ":")

        allowed_cols = None
        if columns_by_table and tname in columns_by_table:
            allowed_cols = set(columns_by_table[tname])

        json_hints = (t.get("json_hints") or {}) if include_json_hints else {}

        for c in t.get("columns", []):
            cname = c["name"]
            if allowed_cols is not None and cname not in allowed_cols:
                continue

            ctype = (c.get("type") or "").strip()
            line = f"  - {cname}" + (f" {ctype}" if ctype else "")

            if c.get("pk_position", 0):
                line += " PRIMARY KEY"
            if c.get("notnull"):
                line += " NOT NULL"
            if c.get("default") is not None:
                line += f" DEFAULT {c['default']}"

            jh = json_hints.get(cname)
            if jh and jh.get("detected"):
                any_json = True
                # summarize keys (dict) or list item kinds (array)
                extras = []
                tlk = jh.get("top_level_keys")
                if tlk:
                    # format like "key:kind; key:kind"
                    extras.append("keys[" + "; ".join(f"{k},{v}" for k, v in sorted(tlk.items())) + "]")
                lik = jh.get("list_item_kinds")
                if lik:
                    extras.append("list[" + "; ".join(f"{k},{v}" for k, v in sorted(lik.items())) + "]")
                if extras:
                    line += "  (JSON: " + " ".join(extras) + ")"

            lines.append(line)

        lines.append("")  # blank line between tables

    if any_json:
        lines.append("Note: Use SQLite JSON1 functions, e.g., json_extract(col, '$.path').")

    return "\n".join(lines).strip()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <queuedata.db>")
        sys.exit(1)

    db_path = sys.argv[1]
    schema = export_schema(db_path)

    # Only the 'queuedata' table, and only two columns:
    txt = emit_minimal_schema_txt(
        schema,
        include_tables=["queuedata"],
        columns_by_table={"queuedata": ["record_id", "acopytools"]},
        include_json_hints=True,
        include_counts=True,
    )
    print(txt)
