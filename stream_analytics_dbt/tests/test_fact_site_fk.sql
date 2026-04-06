-- Test foreign key relationship: fact.site_key should exist in dim_site.site_key
SELECT f.*
FROM {{ ref('fact_air_quality_readings') }} f
LEFT JOIN {{ ref('dim_site') }} s ON f.site_key = s.site_key
WHERE s.site_key IS NULL