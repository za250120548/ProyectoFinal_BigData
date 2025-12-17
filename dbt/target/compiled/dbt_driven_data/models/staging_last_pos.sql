

WITH ranked AS (
    SELECT
        VIN,
        OEM,
        geopos,
        time,
        ROW_NUMBER() OVER (
            PARTITION BY VIN ORDER BY time DESC) AS rn
    FROM
        "airflow"."driven_raw"."raw_batch_data"
)

SELECT
    VIN,
    OEM,
    time,
    geopos
FROM ranked
WHERE rn = 1