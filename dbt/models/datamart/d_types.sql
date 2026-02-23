WITH unique_types AS (
    SELECT DISTINCT type
    FROM {{ source('curated', 'anime') }}
    WHERE type IS NOT NULL
),
final AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY type) AS type_id,
        type
    FROM unique_types
)
SELECT * FROM final