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

# Example

For example, if we have a Parquet file `/tmp/p.parquet` with the following data:

```
+---+----+----------+-------+
|id |b   |c         |d      |
+---+----+----------+-------+
|42 |null|{foo, bar}|[{foo}]|
+---+----+----------+-------+
```

The schema of the data is (this is shown in the Apache Spark schema format since its much easier 
to read than the full blow Parquet schema):
```
root
 |-- id: integer (nullable = true)
 |--  b: string (nullable = true)
 |--  c: struct (nullable = true)
 |     |-- a: string (nullable = true)
 |     |-- b: string (nullable = true)
 |--  d: array (nullable = true)
 |     |-- element: struct (containsNull = true)
 |     |    |-- a: string (nullable = true)
```

If we want to generate the Clickhouse schema to a file named `/tmp/clickhouse_schema.sql` for 
a table named `T` with primary key `id`, run as follows:

```
cargo run -- --parquet-path /tmp/p.parquet --clickhouse-schema-path /tmp/clickhouse_schema.sql \
--table-name T --primary-key id
```

The Clickhouse schema that's generated will be as follows:
```
drop table if exists T;
create table T (
    id Nullable(Int32)
    , b Nullable(String)
    , c Tuple(
        a Nullable(String)
        , b Nullable(String)
    )
    , d Nested (
        a Nullable(String)
    )
) engine = MergeTree() primary key (foo);
```

The resulting Clickhouse schema can be used to create the table in a number of ways.  The simplest 
is to use the Clickhouse command line client.  For example:

```
cat /tmp/clickhouse_schema.sql | clickhouse-client --multiquery
```

Note that the `clickhouse-client` may need additional parameters such as as username and 
password, depending on the way Clickhouse is configured.
