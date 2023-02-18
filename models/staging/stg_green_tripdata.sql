{{ config(materialized='view') }}

SELECT  *
FROM    {{ source('staging','green_tripdata_partitioned') }}
LIMIT   100