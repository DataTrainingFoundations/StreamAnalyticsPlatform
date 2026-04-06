-- Test for valid AQI value ranges (AQI should not be negative)
SELECT *
FROM {{ ref('cleaned_aqi') }}
WHERE aqi < 0