use clap::Parser;
use parquet_to_clickhouse_schema::parquetutils::ParquetUtils;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    parquet_path: String,

    #[arg(long)]
    clickhouse_schema_path: String,

    #[arg(long)]
    table_name: String,

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
