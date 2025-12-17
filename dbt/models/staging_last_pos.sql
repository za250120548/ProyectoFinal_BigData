{{ config(
    materialized='table',
    schema='staging',
    alias='veh_last_pos',
    tags=['staging']
) }}

WITH ranked AS (
    SELECT
        VIN,
        OEM,
        geopos,
        time,
        ROW_NUMBER() OVER (
            PARTITION BY VIN ORDER BY time DESC) AS rn
    FROM
        {{ source('raw_source', 'raw_batch_data') }}
)

SELECT
    VIN,
    OEM,
    time,
    geopos
FROM ranked
WHERE rn = 1
