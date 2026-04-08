{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_key', 'hour_key', 'site_key', 'parameter_key', 'category_key']) }}

SELECT
    dd.date_key,
    dh.hour_key,
    ds.site_key,
    dp.parameter_key,
    dc.category_key,
    {{ dbt_utils.generate_surrogate_key(['date_key', 'hour_key', 'site_key', 'parameter_key', 'category_key'])}} AS fact_id,
    s.aqi 
FROM {{ ref('cleaned_aqi') }} s 
    JOIN {{ ref('dim_site') }} ds 
    ON s.fullaqscode = ds.fullaqscode
    JOIN {{ ref('dim_parameter') }} dp
    ON s.parameter = dp.parameter 
    AND s.unit = dp.unit
    JOIN {{ ref('dim_category') }} dc 
    ON s.category = dc.category 
    JOIN {{ ref('dim_date') }} dd 
    ON s.date = dd.date 
    JOIN {{ ref('dim_hour') }} dh 
    ON s.hour = dh.hour
    
    
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} f
    WHERE f.date_key = dd.date_key
        AND f.hour_key = dh.hour_key
        AND f.site_key = ds.site_key
        AND f.parameter_key = dp.parameter_key
        AND f.category_key = dc.category_key
)    
{% endif %}