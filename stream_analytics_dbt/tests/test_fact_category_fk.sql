-- Test foreign key relationship: fact.category_key should exist in dim_category.category_key
SELECT f.*
FROM {{ ref('fact_air_quality_readings') }} f
LEFT JOIN {{ ref('dim_category') }} c ON f.category_key = c.category_key
WHERE c.category_key IS NULL