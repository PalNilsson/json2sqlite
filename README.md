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

**Notes**
The created database can easily be inspected using the `sqlite3` command line tool, e.g.,

Which queues are not using rucio for data transfers:

`sqlite3 queuedata.db "SELECT * FROM queuedata WHERE copytools NOT LIKE '%rucio%';"`

Which queues have a timefloor larger than zero?

`sqlite3 queuedata.db "SELECT * FROM queuedata WHERE timefloor>0;"`

Which queue is the first queue that has a timefloor larger than zero?

`sqlite3 queuedata.db "SELECT * FROM queuedata WHERE timefloor>0 LIMIT 1;"`

You can also use the provided `execute.py` script to run arbitrary SQL queries on the database, e.g.,

`python3 execute.py --db queuedata.db --sql "SELECT * FROM queuedata WHERE timefloor>0 LIMIT 1;"`

(this will return a proper python dictionary).

TODO: Create the annotations table from annotated_queuedata.json.





