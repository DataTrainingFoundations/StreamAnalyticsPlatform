-- Test for valid category values (1-6 as per AirNow)
SELECT *
FROM {{ ref('cleaned_aqi') }}
WHERE category NOT IN (1, 2, 3, 4, 5, 6)