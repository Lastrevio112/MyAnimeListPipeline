# My Anime List API end-to-end ELT pipeline | (Polars, DBT, DuckDB, Docker)

This is an end-to-end ELT pipeline that fethes data from the public "JikanAPI" (the API of MyAnimeList website) about the most popular ~10 000 animes and creates a medallion-like architecture in a DuckDB warehouse. 
This project handles API rate limits, de-duplication, testing and upsert logic with next to no human intervention.

Python (with Polars dataframes) was used for API requests as well as loading data into the bronze (raw) and silver (curated) layers of the architecture. 

DBT was used for creating and handling the gold (datamart) layer of the architecture. DuckDB was used as a warehouse for this project and the entire project was containarized in Docker.

## Repo structure

DBT configuration yaml files can be found in the dbt folder of this project with the models under the models sub-folder.
Jupyter notebooks can be found under the notebooks folder and Python scripts under the Python folder.
.devcontainer folder was used for configuring Docker "dev containers" in VS Code. The Dockerfile is at the root of the repo.
Raw data is stored in importbatches and the DuckDB database under data.

## Stage 1: API to Bronze (raw)

In the first stage of this pipeline, the fetch_raw_json.py script under the /python folder generates a new batch sub-folder in the importbatches folder and fills it with somewhere between 250 and 300 .json files, each containing information about 25 animes.

Next, a function is called to import in raw.api_data (in DuckDB) only the batches which do not already exist in the table.
This was done so that the data can be refreshed at will, because data about an anime (rating, score, etc.) can change over time.
The DDL for the bronze layer is defined in python/bronze_ddl.py

## Stage 2: Bronze (raw) to Silver (curated)

First, two sets of tables were defined in notebooks/silver_curated_ddl.ipynb - the merge tables and the 'regular' tables.

The data flow goes like this: each time load_curated_tables.ipynb is called, the merge tables get overwritten with the latest batch of data from the raw table (for debugging purposes, the batch number can be changed temporarily). Then, the data from the merge tables (that is, the data from the latest batch) gets upserted into the actual tables in the curated/silver layer. In this way, we avoid duplicates and data in the curated schema stays fresh with new runs of the pipeline.

The curated schema has a main "anime" table which is a flattened version of the raw JSONs from the raw table, with array-type columns removed. It also contains "links" tables to demograhics, genres, licensors, producers, studios and themes data. This is because the relation between an anime and any of those beforementioned data points is many-to-many (one anime has more genres, one genre is had by multiple animes), therefore needing a linking table so that the anime_id column remains unique in the anime table.

## Stage 3: Silver (curated) to Bronze (datamart)

This is a star schema, ready to be used by BI models, created in DBT under dbt/models/datamart. The materialization type of those models is table, for faster reads and up-to-date data. 

This model has a main fact table (f_anime) in a one-to-many relationship with three dimension tables (d_sources, d_statuses and d_types) as well as in a many to many relationship with six other dimension tables (d_demographics, d_genres, d_licensors, d_producers, d_studios, d_themes). 
The many to many relations were handled by linkage tables ("links_genres", etc.).
