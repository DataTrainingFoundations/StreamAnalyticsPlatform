{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='fullaqscode') }}
WITH base AS (
    SELECT 
        c.fullaqscode,
        c.sitename, 
        c.agencyname, 
        c.latitude, 
        c.longitude, 
        {{ dbt_utils.generate_surrogate_key(['fullaqscode'])}} as site_key,
        g.region_key
    FROM {{ ref('cleaned_aqi') }} c LEFT JOIN {{ source('gold', 'dim_region') }} g
        ON c.latitude  >= g.lat_min AND c.latitude  <= g.lat_max
            AND c.longitude >= g.lon_min AND c.longitude <= g.lon_max
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} t
        WHERE t.fullaqscode = c.fullaqscode
            AND t.latitude = c.latitude
            AND t.longitude = c.longitude
    )

    {% endif %}
),
deduped AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fullaqscode
        ORDER BY region_key DESC
    ) = 1
)

SELECT * FROM deduped