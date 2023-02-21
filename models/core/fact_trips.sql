{{ config(materialized='table') }}


with fhv_trips as (
    select * from {{ ref('stg_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    fhv_trips.dispatching_base_num,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_trips.pickup_datetime,
    fhv_trips.dropoff_datetime
from fhv_trips
inner join dim_zones as pickup_zone
on fhv_trips.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trips.DOlocationID = dropoff_zone.locationid
