{{ config(materialized="view") }}

select 
    dispatching_base_num,
    PUlocationID,
    DOlocationID,
    pickup_datetime,
    dropOff_datetime
from {{ source("staging", "tripdata_partitioned") }}
where PUlocationID is not null and DOlocationID is not null

-- {% if var('is_test_run', default=true) %}

-- limit 10

-- {% endif %}
