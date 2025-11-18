Define DDL for analytics schema with a subset of Analytics views referenced in CM.

Populate Analytics table data from Snowflake to corresponding tables in Postgres.


One time / Recurring, incremental - primary key + watermark

copy (insert/merge) all records matching a condition.
a subset of columns is copied so define column mapping from source to target
qualify the primary key and the watermark columns, handle case where watermark is not present
