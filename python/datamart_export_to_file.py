# This script copies the datamart (gold) layer into a new duckdb file that can be pushed on GitHub for Streamlit cloud to pick up.
# We copy only the gold layer so it always ends up being less than 100MB, the github limit.

import duckdb

SRC = "/workspace/data/pipeline.duckdb"
DST = "/workspace/datamart_export.duckdb"
SCHEMA = "datamart"

src = duckdb.connect(SRC, read_only=True)
dst = duckdb.connect(DST)

# Get all tables in the target schema
tables = src.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = ?
      AND table_type = 'BASE TABLE'
""", [SCHEMA]).fetchall()

dst.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

for (table_name,) in tables:
    print(f"Copying {SCHEMA}.{table_name}...")
    df = src.execute(f'SELECT * FROM "{SCHEMA}"."{table_name}"').df()
    dst.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table_name}"')
    dst.execute(f'CREATE TABLE "{SCHEMA}"."{table_name}" AS SELECT * FROM df')

print(f"Done. {len(tables)} table(s) copied to {DST}")

src.close()
dst.close()