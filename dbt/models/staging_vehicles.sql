{{ config(
    materialized='table',
    schema='staging',
    alias='vehicles',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT
        DISTINCT VIN,
        OEM,
        model_year
    FROM
        {{ source('raw_source', 'raw_batch_data') }}
)

SELECT
    *
FROM
    source_data