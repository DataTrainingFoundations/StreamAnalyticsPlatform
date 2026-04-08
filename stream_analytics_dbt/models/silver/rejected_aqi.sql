{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['utc', 'fullaqscode', 'parameter']) }}

WITH source_rejected AS (
    SELECT
        parameter,
        unit,
        aqi,
        category,
        sitename,
        agencyname,
        latitude,
        longitude,
        fullaqscode,
        intlaqscode,
        utc,
        original_file,
        -- Concern level mapping (from AirNow)
        CASE 
            WHEN aqi = -999 THEN 'INVALID_AQI'
            WHEN aqi IS NULL THEN 'NULL_AQI'
            WHEN category = -999 THEN 'INVALID_CATEGORY'
            WHEN category IS NULL THEN 'NULL_CATEGORY'
            WHEN sitename IS NULL THEN 'NULL_SITE'
            WHEN parameter IS NULL THEN 'NULL_PARAMETER'
            WHEN fullaqscode IS NULL THEN 'NULL_FULLAQSCODE'
            WHEN intlaqscode IS NULL THEN 'NULL_INTLAQSCODE'
            WHEN agencyname IS NULL THEN 'NULL_AGENCY'
            WHEN latitude IS NULL THEN 'NULL_LATITUDE'
            WHEN longitude IS NULL THEN 'NULL_LONGITUDE'
            WHEN utc IS NULL THEN 'NULL_DATETIME'
            ELSE NULL
            END AS rejection_reason,
            CURRENT_TIMESTAMP() AS rejected_at
    FROM {{ source('bronze', 'raw_aqi') }}
    -- Remove invalid rows
            WHERE aqi = -999
            OR category = -999
            OR aqi IS NULL
            OR category IS NULL
            OR fullaqscode IS NULL
            OR intlaqscode IS NULL
            OR parameter IS NULL
            OR latitude IS NULL
            OR longitude IS NULL
            OR utc IS NULL

            {% if is_incremental() %}
            AND CAST(utc AS TIMESTAMP) > COALESCE(
                (SELECT DATEADD(hour, -1, MAX(utc))
                FROM {{ this }}),
                '1900-01-01'::timestamp
            )
            {% endif %}
            
), source_duplicates AS (
    SELECT
        parameter,
        unit,
        aqi,
        category,
        sitename,
        agencyname,
        latitude,
        longitude,
        fullaqscode,
        intlaqscode,
        utc,
        original_file,
        'DUPLICATE' as rejection_reason,
        CURRENT_TIMESTAMP() AS rejected_at
        -- Concern level mapping (from AirNow)
    FROM {{ source('bronze', 'raw_aqi') }}
    -- Remove invalid rows
            WHERE aqi != -999
            AND category != -999
            AND aqi IS NOT NULL
            AND category IS NOT NULL
            AND fullaqscode IS NOT NULL
            AND intlaqscode IS NOT NULL
            AND parameter IS NOT NULL
            AND latitude IS NOT NULL
            AND longitude IS NOT NULL
            AND utc IS NOT NULL

            {% if is_incremental() %}
            AND CAST(utc AS TIMESTAMP) > COALESCE(
                (SELECT DATEADD(hour, -1, MAX(utc))
                FROM {{ this }}),
                '1900-01-01'::timestamp
            )
            {% endif %}
),
duplicates AS (
    SELECT *
    FROM source_duplicates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY utc, fullaqscode, parameter
        ORDER BY aqi DESC
    ) > 1
),
all_rejected AS (
    SELECT * FROM duplicates
    UNION ALL
    SELECT * FROM source_rejected
),
deduped AS (
    SELECT *
    FROM all_rejected
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY utc, fullaqscode, parameter
        ORDER BY rejected_at DESC
    ) = 1
) 

SELECT * FROM deduped
