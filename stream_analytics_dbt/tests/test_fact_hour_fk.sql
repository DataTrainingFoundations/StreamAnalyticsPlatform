-- Test foreign key relationship: fact.hour_key should exist in dim_hour.hour_key
SELECT f.*
FROM {{ ref('fact_air_quality_readings') }} f
LEFT JOIN {{ ref('dim_hour') }} h ON f.hour_key = h.hour_key
WHERE h.hour_key IS NULL