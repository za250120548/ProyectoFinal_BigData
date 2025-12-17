
  
    

  create  table "airflow"."driven_staging"."veh_speeds__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT
        VIN,
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
  );
  