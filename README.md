# json2sqlite

A simple tool that creates and populates an SQLite database from a JSON file with assumed format:

`{ outer_key: { col: value, ... }, ... }`

**Usage**

`python3 json_to_sqlite.py [-h] --db DB --table TABLE [--queuedata QUEUEDATA]`

where

* --db: Path to SQLite DB file (will be created if missing).
* --table: Target table name, e.g., 'queuedata'.
* --queuedata: Path to queuedata.json.
=
**Examples with queuedata from CRIC**

The created database can easily be inspected using the `sqlite3` command line tool, e.g.,

Which queues are not using rucio for data transfers:

`sqlite3 queuedata.db "SELECT * FROM queuedata WHERE copytools NOT LIKE '%rucio%';"`

Which queues have a timefloor larger than zero?

`sqlite3 queuedata.db "SELECT * FROM queuedata WHERE timefloor>0;"`

Which queue is the first queue that has a timefloor larger than zero?

`sqlite3 queuedata.db "SELECT * FROM queuedata WHERE timefloor>0 LIMIT 1;"`

You can also use the provided `execute.py` script to run arbitrary SQL queries on the database, e.g.,

`python3 execute.py --db queuedata.db --sql "SELECT * FROM queuedata WHERE timefloor>0 LIMIT 1;" --pretty`

(this will return a list of proper python dictionaries, pretty printed).

Select the acopytools field from queue AGLT2:

`python3 execute.py --db queuedata.db --sql "SELECT acopytools FROM queuedata WHERE record_id = 'AGLT2';" --pretty`

**LLM integration**

The database can be used with LLMs to answer questions about the data. To alleviate this, the DB schema should be given 
to the LLM. It can be exported with the command (for a database named `queuedata.db`):

`python3 export_schema.py --db queuedata.db --json-out schema.json --txt-out schema.txt`

The output can be used like:

* TXT is already phrased for models: compact, readable, and includes hints (row counts, JSON keys). Drop it straight 
into the prompt right before you ask for SQL.
* JSON is for your tooling: programmatic checks/validation, auto-updating the TXT, or building a UI that filters to 
only the relevant tables before prompting.

Practical setup

* At query time, insert the TXT snippet into the prompt (system or preface) and add strict instructions like:
  * “Only use the tables/columns listed. Quote identifiers. Use json_extract for JSON fields.”
* Keep the JSON in your app so you can:
  * filter down to only the tables/columns relevant to the user’s question,
  * regenerate a smaller TXT snippet via your format_schema_for_llm(...),
  * optionally validate/repair the SQL the model returns.




