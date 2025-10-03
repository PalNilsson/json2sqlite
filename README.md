# json2sqlite

A simple tool that creates and populates an SQLite database from a JSON file with format:

`{ outer_key: { col: value, ... }, ... }`

**Examples**

`usage: python3 json_to_sqlite.py [-h] --db DB --table TABLE [--queuedata QUEUEDATA] [--annotations ANNOTATIONS]`

where

* --db: Path to SQLite DB file (will be created if missing).
* --table: Target table name, e.g., 'queuedata'.
* --queuedata: Path to queuedata.json (dictionary1).
* --annotations: Path to annotated_queuedata.json (dictionary2). If absent, no docs are stored.

