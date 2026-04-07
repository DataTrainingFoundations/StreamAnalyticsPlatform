{{ config(
    materialized='incremental',
    unique_key='date') }}

SELECT DISTINCT date,
    {{ dbt_utils.generate_surrogate_key(['date'])}} as date_key
FROM {{ ref('cleaned_aqi') }}

{% if is_incremental() %}
WHERE date NOT IN (SELECT date FROM {{ this }})
{% endif %}