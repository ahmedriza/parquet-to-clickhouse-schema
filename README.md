# Creating Clickhouse Schema from Parquet

This is a small utility for creating Clickhouse database schema from a Parquet file. 

This is useful especially for Parquet files with nested schema where it is more natural 
to expose them as `Nested` types in the Clickhouse schema.

# Usage

```
$ cargo run -- --help

Usage: schemagen --parquet-path <PARQUET_PATH> --clickhouse-schema-path <CLICKHOUSE_SCHEMA_PATH> --table-name <TABLE_NAME> --primary-key <PRIMARY_KEY>

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
