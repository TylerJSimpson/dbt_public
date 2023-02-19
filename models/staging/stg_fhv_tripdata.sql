
{{ config(materialized='view') }}

SELECT
        -- identifiers
        {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
        CAST(dispatching_base_num AS string) AS vendorid,
        CAST(pulocationid AS integer) AS  pickup_locationid,
        CAST(dolocationid AS integer) AS dropoff_locationid,

        -- timestamps
        CAST(pickup_datetime AS timestamp) AS pickup_datetime,
        CAST(dropoff_datetime AS timestamp) AS dropoff_datetime,

        -- trip info
        sr_flag
FROM    {{ source('staging','fhv_2019_partitoned_clustered') }}
WHERE   dispatching_base_num IS NOT NULL

{% if var('is_test_run', default=false) %}

    LIMIT 100

{% endif %}