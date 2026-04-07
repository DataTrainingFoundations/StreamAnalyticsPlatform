{{ config(
    materialized='incremental',
    unique_key=['parameter', 'unit']) }}

SELECT DISTINCT 
    parameter, 
    unit,
    {{ dbt_utils.generate_surrogate_key(['parameter', 'unit'])}} as parameter_key
FROM {{ ref('cleaned_aqi') }}

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.parameter = {{ ref('cleaned_aqi') }}.parameter
        AND t.unit = {{ ref('cleaned_aqi') }}.unit
)
{% endif %}