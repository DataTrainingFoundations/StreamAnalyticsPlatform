{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='category') }}

SELECT DISTINCT 
    category, 
    concern_level,
    {{ dbt_utils.generate_surrogate_key(['category'])}} as category_key
FROM {{ ref('cleaned_aqi') }}

{% if is_incremental() %}
WHERE category NOT IN (SELECT category FROM {{ this }})
{% endif %}