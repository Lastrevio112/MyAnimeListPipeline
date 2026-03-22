WITH CTE AS (
    SELECT
        a.anime_id,
        a.title,
        a.title_english,
        a.no_of_episodes,
        CASE
            WHEN a.duration LIKE '% min per ep%' AND a.duration NOT LIKE '%hr%min%'
                THEN CAST(SPLIT_PART(a.duration, ' min', 1) AS INTEGER)
            WHEN a.duration LIKE '%hr%min%'
                THEN CAST(SUBSTRING(a.duration, 1, POSITION(' hr' IN a.duration) - 1) AS INTEGER) * 60
                    + CAST(TRIM(SUBSTRING(
                        a.duration, POSITION('hr ' IN a.duration) + 3, POSITION(' min' IN a.duration) - POSITION('hr ' IN a.duration) - 3
                            )) AS INTEGER)
            WHEN a.duration LIKE '% min%'
                THEN CAST(SPLIT_PART(a.duration, ' min', 1) AS INTEGER)
            WHEN a.duration LIKE '%sec%'
                THEN CAST(SPLIT_PART(a.duration, ' sec', 1) AS FLOAT) / 60.0
            WHEN a.duration LIKE '%hr%'
                THEN CAST(SPLIT_PART(a.duration, ' hr', 1) AS INTEGER) * 60
            WHEN a.duration LIKE '%Unknown%' 
                THEN NULL
            ELSE NULL
        END AS duration_minutes_per_episode,   --only God knows how this column is created
        a.score,
        a.scored_by,
        a.rank,
        a.popularity,
        a.members,
        a.favorites,
        a.year,
        ds.status_id,       --foreign key to dim table
        dty.type_id,         --foreign key to dim table
        dsrc.source_id,     --foreign key to dim table
    FROM curated.anime AS a
    LEFT JOIN {{ ref('d_statuses') }} ds ON a.status = ds.status        --we join here on strings since the IDs were generated with ROW_NUMBER() based on the strings themselves
    LEFT JOIN {{ ref('d_types') }} dty ON a.type = dty.type
    LEFT JOIN {{ ref('d_sources') }} dsrc ON a.source = dsrc.source
    WHERE a.batch_id = (SELECT MAX(batch_id) FROM curated.anime)  --only take the latest batch of data
)
SELECT * FROM CTE