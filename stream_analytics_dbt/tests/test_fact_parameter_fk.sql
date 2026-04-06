-- Test foreign key relationship: fact.parameter_key should exist in dim_parameter.parameter_key
SELECT f.*
FROM {{ ref('fact_air_quality_readings') }} f
LEFT JOIN {{ ref('dim_parameter') }} p ON f.parameter_key = p.parameter_key
WHERE p.parameter_key IS NULL