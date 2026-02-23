WITH CTE AS (
    SELECT DISTINCT 
        producer_id,
        producer_name
    FROM {{ source('curated','links_anime_producers') }} AS lap
)
SELECT * FROM CTE