WITH CTE AS (
    SELECT
        anime_id,
        demographic_id
    FROM {{ source('curated', 'links_anime_demographics') }}
)
SELECT * FROM CTE
ORDER BY anime_id, demographic_id