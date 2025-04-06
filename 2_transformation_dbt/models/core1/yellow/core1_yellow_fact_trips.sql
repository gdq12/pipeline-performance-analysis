{{ config(
    materialized="table",
    partition_by={
      "field": "trip_type_start_date",
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by = ["data_source", "pickup_date"]
)}}

select 
  -- cols that help better scan the data 
  trp.trip_type_start_date,
  trp.data_source,
  trp.trip_type_source,
  trp.pickup_date,
  -- IDs
  trp.trip_id,
  trp.vendor_id,
  -- time centric dimensions 
  trp.pickup_datetime,
  trp.dropoff_datetime,
  trp.trip_type_end_date,
  -- more time dimensions for later analysis 
  {{ dbt.datediff("pickup_datetime", "dropoff_datetime", "minute") }} trip_duration_min,
  extract(year from trp.pickup_datetime) pickup_year,
  extract(dayofweek from trp.pickup_datetime) pickup_weekday_num,
  {{ get_weekday_name("pickup_datetime") }} pickup_weekday_name,
  extract(week from trp.pickup_datetime) pickup_calender_week_num,
  extract(month from trp.pickup_datetime) pickup_month,
  extract(hour from trp.pickup_datetime) pickup_hour,
  bigfunctions.eu.is_public_holiday(cast(trp.pickup_datetime as date), 'US') pickup_public_holiday,
  extract(year from dropoff_datetime) dropoff_year,
  extract(dayofweek from trp.dropoff_datetime) dropoff_weekday_num,
  {{ get_weekday_name("dropoff_datetime") }} dropoff_weekday_name,
  extract(week from trp.dropoff_datetime) dropoff_calender_week_num,
  extract(month from trp.dropoff_datetime) dropoff_month,
  extract(hour from trp.dropoff_datetime) dropoff_hour,
  bigfunctions.eu.is_public_holiday(cast(trp.dropoff_datetime as date), 'US') dropoff_public_holiday,
  -- location centric info 
  pz.borough pickup_borough,
  pz.zone pickup_zone,
  pz.service_zone pickup_service_zone,
  dz.borough dropoffp_borough,
  dz.zone dropoff_zone,
  dz.service_zone dropoff_service_zone,
  -- trip categorization
  trp.ratecode_id,
  trp.store_and_fwd_flag,
  trp.payment_type,
  -- for unit centric calculations
  trp.passenger_count,
  trp.trip_distance,
  -- revenue centric stats
  trp.fare_amount,
  trp.extra_amount,
  trp.mta_tax,
  trp.tip_amount,
  trp.tolls_amount,
  trp.improvement_surcharge,
  trp.total_amount,
  trp.congestion_surcharge,
  trp.airport_fee,
  -- data source centric info
  trp.creation_dt,
  {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('stg_yellow__2_filter_out_faulty') }} trp 
join {{ source('mapping.map', 'taxi_zone_lookup') }} pz on trp.pickup_location_id = pz.location_id 
join {{ source('mapping.map', 'taxi_zone_lookup') }} dz on trp.dropoff_location_id = pz.location_id 

{% if var('is_test_run', default = true) %}

  limit 100 

{% endif %}