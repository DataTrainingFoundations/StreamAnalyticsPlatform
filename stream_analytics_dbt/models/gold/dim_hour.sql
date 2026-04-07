{{ config(
    materialized='incremental',
    unique_key='hour') }}

SELECT DISTINCT hour,
    {{ dbt_utils.generate_surrogate_key(['hour'])}} as hour_key
FROM {{ ref('cleaned_aqi') }}

{% if is_incremental() %}
WHERE hour NOT IN (SELECT hour FROM {{ this }})
{% endif %}