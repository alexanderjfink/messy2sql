messy2sql
=========

[![Build Status](https://travis-ci.org/alexanderjfink/messy2sql.png)](https://travis-ci.org/alexanderjfink/messy2sql)

Uses okfn/messytables to generate SQL CREATE database/table and INSERTs from PDFs, text, CSV, and HTML file tables

## Installation

messy2sql is currently in pre alpha. You need to have [python](http://www.python.org/getit/) and [pip](http://www.pip-installer.org/en/latest/installing.html) installed.

Once these are installed, run from the command line: `pip install --pre messy2sql`

## Usage

messy2sql depends on messytables. Once you've got messytables, you can use messy2sql to create SQL statements.

For example.


`
import messy2sql # automatically imports messytables as dependency

rows = CSVTableSet(csv_file).tables[0] # create a MessyTables rowset from a CSV file.

# spin up object
m2s = Messy2SQL(csv_file, db_type="mysql", table_name="test_table")

create_db_query = m2s.create_sql_db("test_db")
create_table_query = m2s.create_sql_table(rows)
create_insert_query = m2s.create_sql_insert(self.rows)

# Run some code to actually do the database inserts
`

