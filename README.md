# Creating Clickhouse Schema from Parquet

This is a small utility for creating Clickhouse database schema from a Parquet file. 

This is useful especially for Parquet files with nested schema where it is more natural 
to expose them as `Nested` types in the Clickhouse schema.

# Usage

```
$ cargo run -- --help

Usage: schemagen --parquet-path <PARQUET_PATH> --clickhouse-schema-path <CLICKHOUSE_SCHEMA_PATH> \
--table-name <TABLE_NAME> --primary-key <PRIMARY_KEY>

Options:
      --parquet-path <PARQUET_PATH>
          Absolute path of the Parquet file to read the Parquet schema from
      --clickhouse-schema-path <CLICKHOUSE_SCHEMA_PATH>
          Absolute path where the Clickhouse schema will be written to
      --table-name <TABLE_NAME>
          The Clickhouse table name
      --primary-key <PRIMARY_KEY>
          The Clickhouse table primary key
  -h, --help
          Print help
```

For example, if we have a Parquet file "/tmp/p.parquet" and want to generate the Clickhouse schema
to a file named "/tmp/clickhouse_schema.sql" for a table named "Sales" with primary key "id", 
run as follows:

```
cargo run -- --parquet-path /tmp/p.parquet --clickhouse-schema-path /tmp/clickhouse_schema.sql \
--table-name Sales --primary-key id
```
