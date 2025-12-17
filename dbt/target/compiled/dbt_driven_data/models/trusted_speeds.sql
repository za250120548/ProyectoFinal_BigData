

WITH source_data AS (
    SELECT
        VIN,
        REPEAT('*', LENGTH(VIN) - 4) || RIGHT(VIN, 4) AS masked_VIN,
        AVG(speed) AS avg_speed,
        MAX(speed) AS max_speed,
        MIN(speed) AS min_speed
    FROM
        "airflow"."driven_raw"."raw_batch_data"
    GROUP BY VIN
)

SELECT
    *
FROM
    source_data