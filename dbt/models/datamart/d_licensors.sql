WITH CTE AS (
    SELECT DISTINCT
        licensor_id,
        licensor_name
    FROM {{ source('curated','links_anime_licensors') }} AS lal
)
SELECT * FROM CTE