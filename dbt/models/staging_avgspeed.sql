{{ config(
    materialized='table',
    schema='staging',
    alias='veh_speeds',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT
        VIN,
        AVG(speed) AS avg_speed,
        MAX(speed) AS max_speed,
        MIN(speed) AS min_speed
    FROM
        {{ source('raw_source', 'raw_batch_data') }}
    GROUP BY VIN
)

SELECT
    *
FROM
    source_data