{{ config(
    unique_key=['pickup_month', 'trip_type']
)}}

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
    {{ dbt.date_trunc("month", "pickup_datetime") }} pickup_month, 
    -- other info 
    ratecode_description,
    payment_description, 
    trip_type_source trip_type,
    cast(null as string) hvfs_description,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    avg(trip_duration_min) avg_trip_duration_min,
    sum(fare_amount) fare_amount, 
    sum(tip_amount) tip_amount,
    sum(total_amount) total_amount,
    sum(extra_amount+mta_tax+tolls_amount+improvement_surcharge+congestion_surcharge+airport_fee) total_fees,
    sum(passenger_count) passenger_count,
    count(distinct case when pickup_public_holiday is true then pickup_date else null end) num_public_holidays,
    count(1) num_trips,
    {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('core1_yellow_fact_trips') }}

{% if is_incremental() %}

where pickup_month not in (select distinct pickup_month from {{ this }})

{% endif %}

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
    {{ dbt.date_trunc("month", "pickup_datetime") }} pickup_month, 
    -- other info 
    ratecode_description,
    payment_description, 
    trip_type_source trip_type,
    cast(null as string) hvfs_description,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    avg(trip_duration_min) avg_trip_duration_min,
    sum(fare_amount) fare_amount, 
    sum(tip_amount) tip_amount,
    sum(total_amount) total_amount,
    sum(extra_amount+mta_tax+tolls_amount+improvement_surcharge+congestion_surcharge) total_fees,
    sum(passenger_count) passenger_count,
    count(1) num_trips,
    count(distinct case when pickup_public_holiday is true then pickup_date else null end) num_public_holidays,
    {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('core1_green_fact_trips') }}

{% if is_incremental() %}

where pickup_month not in (select distinct pickup_month from {{ this }})

{% endif %}

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
    {{ dbt.date_trunc("month", "pickup_datetime") }} pickup_month, 
    -- other info 
    cast(null as string) ratecode_description,
    cast(null as string) payment_description, 
    trip_type_source trip_type,
    hvfs_description,
    -- aggregations
    avg(trip_distance) avg_trip_distance,
    avg(trip_duration_min) avg_trip_duration_min,
    sum(base_passenger_fare) fare_amount, 
    sum(tip_amount) tip_amount,
    cast(null as float64) total_amount,
    sum(toll_amount+black_card_fund_amount+sales_tax+congestion_surcharge+airport_fee) total_fees,
    cast(null as int64) passenger_count,
    count(1) num_trips,
    count(distinct case when pickup_public_holiday is true then pickup_date else null end) num_public_holidays,
    {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('core1_fhvhv_fact_trips') }}

{% if is_incremental() %}

where pickup_month not in (select distinct pickup_month from {{ this }})

{% endif %}

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
    pickup_month,
    -- other info 
    ratecode_description,
    payment_description, 
    trip_type,
    hvfs_description,
    -- aggregations
    avg_trip_distance,
    avg_trip_duration_min,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips,
    num_public_holidays,
    transformation_dt
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
    pickup_month,
    -- other info 
    ratecode_description,
    payment_description, 
    trip_type,
    hvfs_description,
    -- aggregations
    avg_trip_distance,
    avg_trip_duration_min,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips,
    num_public_holidays,
    transformation_dt
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
    pickup_month,
    -- other info 
    ratecode_description,
    payment_description, 
    trip_type,
    hvfs_description,
    -- aggregations
    avg_trip_distance,
    avg_trip_duration_min,
    fare_amount, 
    tip_amount,
    total_amount,
    total_fees,
    passenger_count,
    num_trips,
    num_public_holidays,
    transformation_dt
from fhvhv