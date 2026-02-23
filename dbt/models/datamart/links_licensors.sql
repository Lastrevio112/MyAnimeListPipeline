WITH CTE AS (
    SELECT anime_id, licensor_id
    FROM {{ source('curated', 'links_anime_licensors') }}
)
SELECT * FROM CTE
ORDER BY anime_id, licensor_id