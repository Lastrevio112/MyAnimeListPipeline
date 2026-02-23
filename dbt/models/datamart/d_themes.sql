WITH CTE AS (
    SELECT DISTINCT 
        theme_id,
        theme_name
    FROM {{ source('curated', 'links_anime_themes') }} AS lat
)
SELECT * FROM CTE