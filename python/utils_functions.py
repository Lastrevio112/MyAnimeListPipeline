import polars as pl
from loguru import logger

# ── Helper to extract id+name from a list[struct] column ──────────────────────
def extract_bridge_table(df, col, id_field, name_field):
    return (
        df.select(['anime_id', col])
        .explode(col)
        .select([
            pl.col('anime_id'),
            pl.col(col).struct.field(id_field).alias(id_field),
            pl.col(col).struct.field(name_field).alias(name_field),
        ])
        .drop_nulls()
    )

def overwrite_table(con, df, merge_table: str) -> None:
    #Splitting table name and schema name from variable:
    table_name = merge_table.split('.')[-1]
    schema_name = merge_table.split('.')[0]

    # Get the columns of the existing merge table in DuckDB
    existing_cols = [
        row[0] for row in 
        con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'").fetchall()
    ]
    
    df_cols = df.columns
    
    # Check for mismatches
    missing_in_df = set(existing_cols) - set(df_cols)
    extra_in_df = set(df_cols) - set(existing_cols)
    
    if missing_in_df or extra_in_df:
        raise ValueError(
            f"Column mismatch for {merge_table}!\n"
            f"  In DuckDB but not in DataFrame: {missing_in_df or 'none'}\n"
            f"  In DataFrame but not in DuckDB: {extra_in_df or 'none'}"
        )
    
    # Overwrite the merge table
    con.execute(f"DELETE FROM {merge_table}")
    con.execute(f"INSERT INTO {merge_table} SELECT * FROM df")
    
    count = con.execute(f"SELECT COUNT(*) FROM {merge_table}").fetchone()[0]
    logger.info(f"Overwrote {merge_table}: {count} rows written")