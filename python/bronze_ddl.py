import duckdb

def create_raw_api_data_table():
    con = duckdb.connect('/workspace/data/pipeline.duckdb')

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    con.execute("CREATE SEQUENCE IF NOT EXISTS id_sequence START 1 INCREMENT 1")

    con.execute(
        """
        CREATE OR REPLACE TABLE raw.api_data (
            id          INTEGER PRIMARY KEY DEFAULT nextval('id_sequence'),
            batch_id    INTEGER NOT NULL,
            page        INTEGER NOT NULL,
            loaded_at   TIMESTAMP NOT NULL,
            raw_json    JSON
        )
        """
        )
    con.close()

if __name__ == "__main__":
    create_raw_api_data_table()