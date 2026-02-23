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