WITH CTE AS (
    SELECT
        anime_id,
        genre_id
    FROM {{ source('curated', 'links_anime_genres') }}
)
SELECT * FROM CTE
ORDER BY anime_id, genre_id