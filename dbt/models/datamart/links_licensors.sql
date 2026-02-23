WITH CTE AS (
    SELECT anime_id, producer_id
    FROM {{ source('curated', 'links_anime_producers') }}
)
SELECT * FROM CTE
ORDER BY anime_id, producer_id