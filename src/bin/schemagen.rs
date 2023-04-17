use clap::Parser;
use parquet_to_clickhouse_schema::parquetutils::ParquetUtils;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    /// Absolute path of the Parquet file to read the Parquet schema from
    parquet_path: String,

    #[arg(long)]
    /// Absolute path where the Clickhouse schema will be written to
    clickhouse_schema_path: String,

    /// The Clickhouse table name
    #[arg(long)]
    table_name: String,

    /// The Clickhouse table primary key
    #[arg(long)]
    primary_key: String,
}

pub fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    ParquetUtils::parquet_schema_to_clickhouse(
        args.parquet_path,
        args.clickhouse_schema_path,
        args.table_name,
        args.primary_key
    )?;
    
    Ok(())
}
