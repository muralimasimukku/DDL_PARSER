# DDL_PARSER
This code is a SQL view lineage extractor written in Python using the sqlglot library.

It parses a SQL CREATE VIEW statement and produces a JSON mapping of:

view_name → { base_table_name → [list_of_columns_used] }

