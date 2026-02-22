#This script fetches raw JSON data from Jikan API.
#It will generate a new batch folder for each run. 
#This is because want to refresh the data from time to time but also not delete or modify historical data in the 'bronze' layer of the architecture.

import httpx
import json
import time
from pathlib import Path
from loguru import logger
import duckdb

# Configuring global variables:
IMPORT_BATCHES_PATH = Path("/workspace/importbatches")
BASE_URL = "https://api.jikan.moe/v4/anime"
MIN_MEMBERS = 10000  #we only want popular animes so we don't get obscure ones no one cares about
SLEEP_SECONDS = 1  #API rate limit is 60 requests per minute

# This function will create a new batch folder for each run of the script based on the number of the last one
def create_next_batch_folder() -> Path:
    existing = [
        int(f.name.split("_")[1])  #Extract the batch number from the folder name
        for f in IMPORT_BATCHES_PATH.iterdir()
        if f.is_dir() and f.name.startswith("batch_")
    ]
    next_batch = max(existing, default=0) + 1
    batch_folder = IMPORT_BATCHES_PATH / f"batch_{next_batch}"
    batch_folder.mkdir(parents=True)
    logger.info(f"Created batch folder: {batch_folder}")
    return batch_folder


# Fetch a single page from the Jikan API.
def fetch_page(page: int) -> dict | None:
    params = {
        "order_by": "members",
        "sort": "desc",
        "limit": 25,
        "page": page
    }
    try:
        response = httpx.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        logger.error(f"HTTP error on page {page}: {e}")
        return None
    

# Check if the last anime on this page still meets our member threshold
def has_enough_members(page_data: dict) -> bool:
    anime_list = page_data.get("data", [])
    if not anime_list:
        return False
    return anime_list[-1].get("members", 0) >= MIN_MEMBERS


# Main function to fetch and save pages until we hit the member threshold or run out of pages. 
def fetch_new_importbatch() -> None:
    batch_folder = create_next_batch_folder()
    page = 1

    cur_anime = fetch_page(page)
    tries = 0

    while cur_anime is None and tries < 20:
        logger.warning(f"Skipping page {page} due to fetch error.")
        time.sleep(SLEEP_SECONDS)
        cur_anime = fetch_page(page)
        tries += 1
        
    if cur_anime is None:
        logger.error(f"Failed to fetch page {page} after 20 tries. Stopping.")
        return
        
    while has_enough_members(cur_anime):
        with open(batch_folder / f"page_{page}.json", "w") as f:
            json.dump(cur_anime, f, indent=2)
        logger.info(f"Saved page {page} to {batch_folder}")
        page += 1
        time.sleep(SLEEP_SECONDS)
        cur_anime = fetch_page(page)

        tries = 0
        while cur_anime is None and tries < 20:
            logger.warning(f"Skipping page {page} due to fetch error.")
            time.sleep(SLEEP_SECONDS)
            cur_anime = fetch_page(page)
            tries += 1
        
    
#Loads a specific batch in the importbatches folder into the raw.api_data table in DuckDB.
def load_specific_batch_in_duckdb(batch_number: int) -> None:
    batch_folder = IMPORT_BATCHES_PATH / f"batch_{batch_number}"
    if not batch_folder.exists():
        logger.error(f"Batch folder {batch_folder} does not exist.")
        return
    
    json_files = list(batch_folder.glob("page_*.json"))
    if not json_files:
        logger.warning(f"No JSON files found in {batch_folder}.")
        return
    
    con = duckdb.connect('/workspace/data/pipeline.duckdb')

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            raw = json.load(f)

        con.execute("""
                    INSERT INTO raw.api_data(
                    batch_id, page, loaded_at, raw_json
                    )
                    VALUES (?, ?, NOW(), ?)
                    """, 
                    [batch_number, int(json_file.stem.split('_')[1]), json.dumps(raw)]
        )

        logger.info(f"Loaded {json_file} into DuckDB as anime_page_{json_file.stem}")
    
    con.close()


#This function loads the batches in importbatches folder that do not already exist in DuckDB.
def load_missing_batches():
    loaded_at_least_one_new_batch = False

    batches_in_importbatches = []
    for f in IMPORT_BATCHES_PATH.iterdir():
        if f.is_dir() and f.name.startswith("batch_"):
            batches_in_importbatches.append(int(f.name.split("_")[1]))
    
    batches_in_duckdb = []
    con = duckdb.connect('/workspace/data/pipeline.duckdb')
    result = con.execute("SELECT DISTINCT batch_id FROM raw.api_data").fetchall()
    batches_in_duckdb = [row[0] for row in result]
    con.close()

    missing_batches = [x for x in batches_in_importbatches if x not in batches_in_duckdb]
    
    for batch in missing_batches:
        logger.info(f"Batch {batch} is missing in DuckDB. Loading it now.")
        load_specific_batch_in_duckdb(batch)
        loaded_at_least_one_new_batch = True
    
    if (not loaded_at_least_one_new_batch):
        logger.info("No new batches to load. All batches in importbatches are already in DuckDB.")

    
if __name__ == "__main__":
    fetch_new_importbatch()
    load_missing_batches()