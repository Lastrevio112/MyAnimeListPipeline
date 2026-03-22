WITH CTE AS (
    SELECT DISTINCT 
        studio_id,
        studio_name
    FROM {{ source('curated', 'links_anime_studios') }} AS las
)
SELECT * FROM CTE