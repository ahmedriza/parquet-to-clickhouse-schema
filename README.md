# Creating Clickhouse Schema from Parquet

This is a small utility for creating Clickhouse database schema from a Parquet file. 

This is useful especially for Parquet files with nested schema where it is more natural 
to expose them as `Nested` types in the Clickhouse schema.

