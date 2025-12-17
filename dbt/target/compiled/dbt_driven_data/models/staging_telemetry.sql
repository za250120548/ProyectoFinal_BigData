

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
        "airflow"."driven_raw"."raw_batch_data"
)

SELECT
    *
FROM
    source_data