{{ config(
    materialized='table',
    schema='trusted',
    alias='veh_telemetry',
    tags=['trusted']
) }}

WITH source_data AS (
    SELECT
        v.VIN,
        REPEAT('*', LENGTH(v.VIN) - 4) || RIGHT(v.VIN, 4) AS masked_VIN,
        v.OEM,
        v.model_year,
        t.speed,
        t.odometer,
        t.time,
        t.geopos,
        REPEAT('*', LENGTH(t.geopos) - 4) AS masked_geopos
    FROM {{ ref('staging_vehicles') }} v
    JOIN {{ ref('staging_telemetry') }} t
    ON v.VIN = t.VIN
)

SELECT
    masked_VIN,
    OEM,
    model_year,
    speed,
    odometer,
    time,
    masked_geopos
FROM
    source_data