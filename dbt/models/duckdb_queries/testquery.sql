with final as (
    SELECT * FROM datamart.f_anime f
    LEFT JOIN datamart.links_genres l ON f.anime_id = l.anime_id
    LEFT JOIN datamart.d_genres g ON l.genre_id = g.genre_id
)
SELECT * FROM final