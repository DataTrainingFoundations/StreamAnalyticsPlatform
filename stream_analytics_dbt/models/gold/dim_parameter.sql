{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['parameter', 'unit']) }}

SELECT DISTINCT 
    c.parameter, 
    c.unit,
    {{ dbt_utils.generate_surrogate_key(['parameter', 'unit'])}} as parameter_key
FROM {{ ref('cleaned_aqi') }} c

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.parameter = c.parameter
        AND t.unit = c.unit
)
{% endif %}