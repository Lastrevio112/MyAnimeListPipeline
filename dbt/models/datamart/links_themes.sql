WITH CTE AS (
    SELECT
        anime_id,
        theme_id
    FROM {{ source('curated', 'links_anime_themes') }}
)
SELECT * FROM CTE
ORDER BY anime_id, theme_id