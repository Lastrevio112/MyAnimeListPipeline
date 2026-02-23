with final as (
--SELECT * FROM curated.anime LIMIT 1
--SELECT DISTINCT status FROM curated.anime
SELECT * FROM datamart.d_licensors
)
SELECT * FROM final