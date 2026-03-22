WITH unique_statuses As (
    SELECT DISTINCT status
    FROM {{ source('curated', 'anime') }}
    WHERE status IS NOT NULL
),
final AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY status) AS status_id,
        status
    FROM unique_statuses
)
SELECT * FROM final