
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_mart1`.`mart1_dm_daily_stats`
      
    partition by timestamp_trunc(pickup_date, day)
    

    OPTIONS()
    as (
      

with yel as 
(select 
    -- zone info
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    -- time info
    pickup_date,
    pickup_public_holiday, 
    -- other info 
    payment_description, 
    trip_type_source trip_type,
    cast(null as string) hvfs_description,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    sum(fare_amount) fare_amount, 
    sum(tip_amount) tip_amount,
    sum(total_amount) total_amount,
    sum(extra_amount+mta_tax+tolls_amount+improvement_surcharge+congestion_surcharge+airport_fee) total_fees,
    sum(passenger_count) passenger_count,
    count(1) num_trips
from `pipeline-analysis-455005`.`nytaxi_core1`.`core1_yellow_fact_trips`



group by all 
),
grn as 
(select 
    -- zone info
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    -- time info
    pickup_date,
    pickup_public_holiday,
    -- other info 
    payment_description, 
    trip_type_source trip_type,
    cast(null as string) hvfs_description,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    sum(fare_amount) fare_amount, 
    sum(tip_amount) tip_amount,
    sum(total_amount) total_amount,
    sum(extra_amount+mta_tax+tolls_amount+improvement_surcharge+congestion_surcharge) total_fees,
    sum(passenger_count) passenger_count,
    count(1) num_trips
from `pipeline-analysis-455005`.`nytaxi_core1`.`core1_green_fact_trips`



group by all 
),
fhvhv as 
(select 
    -- zone info
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    -- time info
    pickup_date,
    pickup_public_holiday,
    -- other info 
    cast(null as string) payment_description, 
    trip_type_source trip_type,
    hvfs_description,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    sum(base_passenger_fare) fare_amount, 
    sum(tip_amount) tip_amount,
    cast(null as float64) total_amount,
    sum(toll_amount+black_card_fund_amount+sales_tax+congestion_surcharge+airport_fee) total_fees,
    cast(null as int64) passenger_count,
    count(1) num_trips
from `pipeline-analysis-455005`.`nytaxi_core1`.`core1_fhvhv_fact_trips`



group by all 
)
select 
    -- zone info
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    -- time info
    pickup_date,
    pickup_public_holiday,
    -- other info 
    payment_description, 
    trip_type,
    hvfs_description,
    -- aggregations
    avg_trip_distance,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips
from yel 
union all 
select 
    -- zone info
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    -- time info
    pickup_date,
    pickup_public_holiday,
    -- other info 
    payment_description, 
    trip_type,
    hvfs_description,
    -- aggregations
    avg_trip_distance,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips
from grn 
union all 
select 
    -- zone info
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    -- time info
    pickup_date,
    pickup_public_holiday,
    -- other info 
    payment_description, 
    trip_type,
    hvfs_description,
    -- aggregations
    avg_trip_distance,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips
from fhvhv
    );
  