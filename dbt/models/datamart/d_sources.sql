WITH unique_sources AS (
    SELECT DISTINCT source
    FROM {{ source('curated', 'anime') }}
    WHERE source IS NOT NULL
),
final AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY source) AS source_id,
        source
    FROM unique_sources
)
SELECT * FROM final