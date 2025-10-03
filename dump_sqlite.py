import sqlite3
from typing import Optional

def dump_sqlite(db_path: str, table: Optional[str] = None, limit: Optional[int] = None) -> None:
    """
    Print the content of the SQLite database (all tables or a single table).
    
    Args:
        db_path (str): Path to SQLite database file.
        table (str, optional): If given, only dump this table.
        limit (int, optional): If given, limit number of rows per table.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    def dump_table(table_name: str):
        print(f"\n=== {table_name} ===")
        try:
            sql = f"SELECT * FROM '{table_name}'"
            if limit:
                sql += f" LIMIT {limit}"
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                print("(empty)")
                return

            # Print header
            headers = rows[0].keys()
            print(" | ".join(headers))
            print("-" * (len(" | ".join(headers))))

            # Print rows
            for row in rows:
                print(" | ".join(str(row[h]) if row[h] is not None else "NULL" for h in headers))

        except sqlite3.Error as e:
            print(f"Error reading {table_name}: {e}")

    try:
        if table:
            dump_table(table)
        else:
            # List all tables
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = [r[0] for r in cur.fetchall()]
            if not tables:
                print("(no tables found)")
            for t in tables:
                dump_table(t)
    finally:
        conn.close()


if __name__ == "__main__":
    dump_sqlite("example.db")                # dump everything
#    dump_sqlite("example.db", "jobs")        # dump only the "jobs" table
#    dump_sqlite("example.db", limit=5)       # dump first 5 rows of each table
