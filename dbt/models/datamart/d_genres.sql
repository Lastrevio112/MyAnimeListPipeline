WITH CTE AS (
    SELECT DISTINCT
        genre_id,
        genre_name
    FROM {{ source('curated', 'links_anime_genres') }} AS lag
)
SELECT * FROM CTE