
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_core2`.`core2_dm_daily_stats`
      
    partition by timestamp_trunc(pickup_date, day)
    

    OPTIONS()
    as (
      

with yel as 
(select 
    -- zone info
    pickup_location_id,
    dropoff_location_id,
    -- time info
    pickup_date,
    pickup_public_holiday, 
    extract(year from pickup_datetime) pickup_year,
    extract(month from pickup_datetime) pickup_month,
    extract(hour from pickup_datetime) pickup_hour,
    pickup_rush_hour_status, 
    pickup_weekday_name,
    -- other info 
    ratecode_id,
    payment_type, 
    trip_type_source trip_type,
    cast(null as string) hvfhs_license_number,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    avg(trip_duration_min) avg_trip_duration_min,
    sum(fare_amount) fare_amount, 
    sum(tip_amount) tip_amount,
    sum(total_amount) total_amount,
    sum(extra_amount+mta_tax+tolls_amount+improvement_surcharge+congestion_surcharge+airport_fee) total_fees,
    sum(passenger_count) passenger_count,
    count(1) num_trips,
    current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_yellow_fact_trips`



group by all 
),
grn as 
(select 
    -- zone info
    pickup_location_id,
    dropoff_location_id,
    -- time info
    pickup_date,
    pickup_public_holiday, 
    extract(year from pickup_datetime) pickup_year,
    extract(month from pickup_datetime) pickup_month,
    extract(hour from pickup_datetime) pickup_hour,
    pickup_rush_hour_status, 
    pickup_weekday_name,
    -- other info 
    ratecode_id,
    payment_type, 
    trip_type_source trip_type,
    cast(null as string) hvfhs_license_number,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    avg(trip_duration_min) avg_trip_duration_min,
    sum(fare_amount) fare_amount, 
    sum(tip_amount) tip_amount,
    sum(total_amount) total_amount,
    sum(extra_amount+mta_tax+tolls_amount+improvement_surcharge+congestion_surcharge) total_fees,
    sum(passenger_count) passenger_count,
    count(1) num_trips,
    current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_green_fact_trips`



group by all 
),
fhvhv as 
(select 
    -- zone info
    pickup_location_id,
    dropoff_location_id,
    -- time info
    pickup_date,
    pickup_public_holiday, 
    extract(year from pickup_datetime) pickup_year,
    extract(month from pickup_datetime) pickup_month,
    extract(hour from pickup_datetime) pickup_hour,
    pickup_rush_hour_status, 
    pickup_weekday_name,
    -- other info 
    cast(null as int64) ratecode_id,
    cast(null as int64) payment_type, 
    trip_type_source trip_type,
    hvfhs_license_number,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    avg(trip_duration_min) avg_trip_duration_min,
    sum(base_passenger_fare) fare_amount, 
    sum(tip_amount) tip_amount,
    cast(null as float64) total_amount,
    sum(toll_amount+black_card_fund_amount+sales_tax+congestion_surcharge+airport_fee) total_fees,
    cast(null as int64) passenger_count,
    count(1) num_trips,
    current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_fhvhv_fact_trips`



group by all 
)
select 
    -- zone info
    pickup_location_id,
    dropoff_location_id,
    -- time info
    pickup_date,
    pickup_public_holiday, 
    pickup_year,
    pickup_month,
    pickup_hour,
    pickup_rush_hour_status, 
    pickup_weekday_name,
    -- other info 
    ratecode_id,
    payment_type, 
    trip_type,
    hvfhs_license_number,
    -- aggregations
    avg_trip_distance,
    avg_trip_duration_min,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips,
    transformation_dt
from yel 
union all 
select 
    -- zone info
    pickup_location_id,
    dropoff_location_id,
    -- time info
    pickup_date,
    pickup_public_holiday, 
    pickup_year,
    pickup_month,
    pickup_hour,
    pickup_rush_hour_status, 
    pickup_weekday_name,
    -- other info 
    ratecode_id,
    payment_type, 
    trip_type,
    hvfhs_license_number,
    -- aggregations
    avg_trip_distance,
    avg_trip_duration_min,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips,
    transformation_dt
from grn 
union all 
select 
    -- zone info
    pickup_location_id,
    dropoff_location_id,
    -- time info
    pickup_date,
    pickup_public_holiday, 
    pickup_year,
    pickup_month,
    pickup_hour,
    pickup_rush_hour_status, 
    pickup_weekday_name,
    -- other info 
    ratecode_id,
    payment_type, 
    trip_type,
    hvfhs_license_number,
    -- aggregations
    avg_trip_distance,
    avg_trip_duration_min,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips,
    transformation_dt
from fhvhv
    );
  