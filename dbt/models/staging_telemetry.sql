{{ config(
    materialized='table',
    schema='staging',
    alias='telemetry',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT
        VIN,
        OEM,
        model_year,
        time,
        speed,
        odometer,
        geopos
    FROM
        {{ source('raw_source', 'raw_batch_data') }}
)

SELECT
    *
FROM
    source_data
    