-- trip_taxi_data.tripdata_nonpartitioned
-- trip_taxi_data.external_tripdata
-- trip_taxi_data.tripdata_partitioned

create or replace external table `zoomcamp2023-377809.trip_taxi_data.external_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp2023/tripdata/fhv_tripdata_2019-*.csv.gz']
);

-- Q1: 43244696
select count(*) from trip_taxi_data.external_tripdata limit 10;


create or replace table trip_taxi_data.tripdata_nonpartitioned as select * from trip_taxi_data.external_tripdata;


-- Q2: 
  -- 317.94 MB
  -- 653 ms
select count(distinct(affiliated_base_number)) from trip_taxi_data.tripdata_nonpartitioned;
  -- 2.52 GB
  -- 31 sec
select count(distinct(affiliated_base_number)) from trip_taxi_data.external_tripdata;


-- Q3: 717748 
select count(*) from trip_taxi_data.tripdata_nonpartitioned where PUlocationID is null and DOlocationID is null;


-- Q4: Partition by pickup_datetime Cluster on affiliated_base_number
create or replace table trip_taxi_data.tripdata_partitioned
partition by date(pickup_datetime)
cluster by dispatching_base_num as (select * from trip_taxi_data.tripdata_nonpartitioned);

-- Q5: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
select count(distinct(affiliated_base_number)) from trip_taxi_data.tripdata_partitioned 
where pickup_datetime >= '2019-03-01' and pickup_datetime <= '2019-03-31'

-- Q6: GCP Bucket
-- Q7: False 