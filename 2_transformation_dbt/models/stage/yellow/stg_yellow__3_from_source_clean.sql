{{ config(
    materialized="table",
    cluster_by = ["data_source", "trip_type_start_date", "pickup_date"]
)}}

select 
  -- IDs
  trip_id,
  vendor_id,
  -- time centric dimensions 
  pickup_datetime,
  dropoff_datetime,
  trip_type_start_date,
  trip_type_end_date,
  pickup_date,
  -- more time dimensions for later analysis 
  {{ dbt.datediff("pickup_datetime", "dropoff_datetime", "minute") }} trip_duration_min,
  extract(year from pickup_datetime) pickup_year,
  extract(dayofweek from pickup_datetime) pickup_weekday_num,
  {{ get_weekday_name("pickup_datetime") }} pickup_weekday_name,
  extract(week from pickup_datetime) pickup_calender_week_num,
  extract(month from pickup_datetime) pickup_month,
  extract(hour from pickup_datetime) pickup_hour,
  bigfunctions.eu.is_public_holiday(cast(pickup_datetime as date), 'US') pickup_public_holiday,
  extract(year from dropoff_datetime) dropoff_year,
  extract(dayofweek from dropoff_datetime) dropoff_weekday_num,
  {{ get_weekday_name("dropoff_datetime") }} dropoff_weekday_name,
  extract(week from dropoff_datetime) dropoff_calender_week_num,
  extract(month from dropoff_datetime) dropoff_month,
  extract(hour from dropoff_datetime) dropoff_hour,
  bigfunctions.eu.is_public_holiday(cast(dropoff_datetime as date), 'US') dropoff_public_holiday,
  -- location centric info 
  pickup_location_id,
  dropoff_location_id,
  -- trip categorization
  ratecode_id,
  store_and_fwd_flag,
  payment_type,
  -- for unit centric calculations
  passenger_count,
  trip_distance,
  -- revenue centric stats
  fare_amount,
  extra_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  -- data source centric info
  trip_type,
  data_source,
  creation_dt,
  {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('stg_yellow__2_filter_out_faulty') }}