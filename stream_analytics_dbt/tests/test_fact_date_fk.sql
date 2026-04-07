-- Test foreign key relationship: fact.date_key should exist in dim_date.date_key
SELECT f.*
FROM {{ ref('fact_air_quality_readings') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
WHERE d.date_key IS NULL