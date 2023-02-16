{{ config(materialized="view") }}

select 
    *
from {{ source("staging", "tripdata_partitioned") }}
limit 10
