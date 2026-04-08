-- Test for not null columns in cleaned_aqi model
SELECT *
FROM {{ ref('cleaned_aqi') }}
WHERE
    date IS NULL OR
    hour IS NULL OR
    fullaqscode IS NULL OR
    parameter IS NULL OR
    unit IS NULL OR
    aqi IS NULL OR
    category IS NULL OR
    latitude IS NULL OR
    longitude IS NULL