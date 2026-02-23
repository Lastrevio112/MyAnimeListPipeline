WITH CTE AS (
    SELECT
        anime_id,
        studio_id
    FROM {{ source('curated', 'links_anime_studios') }}
)
SELECT * FROM CTE
ORDER BY anime_id, studio_id