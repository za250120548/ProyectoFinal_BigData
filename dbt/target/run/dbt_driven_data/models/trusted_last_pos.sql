
  
    

  create  table "airflow"."driven_trusted"."veh_last_pos__dbt_tmp"
  
  
    as
  
  (
    

WITH ranked AS (
    SELECT
        VIN,
        REPEAT('*', LENGTH(VIN) - 4) || RIGHT(VIN, 4) AS masked_VIN,
        OEM,
        geopos,
        REPEAT('*', LENGTH(geopos) - 4) AS masked_geopos,
        time,
        ROW_NUMBER() OVER (
            PARTITION BY VIN ORDER BY time DESC) AS rn
    FROM
        "airflow"."driven_raw"."raw_batch_data"
)

SELECT
    masked_VIN,
    OEM,
    time,
    masked_geopos
FROM ranked
WHERE rn = 1
  );
  