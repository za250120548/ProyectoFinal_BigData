
  
    

  create  table "airflow"."driven_staging"."dim_last_pos__dbt_tmp"
  
  
    as
  
  (
    

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
  );
  