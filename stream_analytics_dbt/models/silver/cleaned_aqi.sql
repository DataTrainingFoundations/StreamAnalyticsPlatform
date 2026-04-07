{{ config(
    materialized='incremental',
    unique_key=['date', 'hour', 'fullaqscode', 'intlaqscode', 'parameter']) }}


WITH source AS (
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
        DATE(utc::TIMESTAMP) AS date,
        HOUR(utc::TIMESTAMP) AS hour,
        NULL AS local_hour,
        -- Concern level mapping (from AirNow)
        CASE category
            WHEN 1 THEN 'Good'
            WHEN 2 THEN 'Moderate'
            WHEN 3 THEN 'Unhealthy for Sensitive Groups'
            WHEN 4 THEN 'Unhealthy'
            WHEN 5 THEN 'Very Unhealthy'
            WHEN 6 THEN 'Hazardous'
        END AS concern_level
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
            AND DATEADD(hour, HOUR(utc::timestamp), DATE(utc::timestamp)) > COALESCE(
                (SELECT MAX(DATEADD(hour, hour, date))
                FROM {{ this }}),
                '1900-01-01'::timestamp
            )
            {% endif %}
), deduped AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date, hour, fullaqscode, intlaqscode, parameter
        ORDER BY aqi DESC
    ) = 1
)

SELECT * FROM deduped