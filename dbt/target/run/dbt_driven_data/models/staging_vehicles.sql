
  
    

  create  table "airflow"."driven_staging"."vehicles__dbt_tmp"
  
  
    as
  
  (
    

WITH source_data AS (
    SELECT
        DISTINCT VIN,
        OEM,
        model_year
    FROM
        "airflow"."driven_raw"."raw_batch_data"
)

SELECT
    *
FROM
    source_data
  );
  