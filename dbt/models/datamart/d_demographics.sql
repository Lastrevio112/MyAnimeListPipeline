WITH CTE AS (
    SELECT DISTINCT 
        demographic_id,
        demographic_name
    FROM {{ source('curated', 'links_anime_demographics') }} AS lad
)
SELECT * FROM CTE