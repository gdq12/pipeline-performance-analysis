{{ config(
    materialized="incremental",
    partition_by={
      "field": "trip_type_start_date",
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by = ["data_source", "pickup_date"]
)}}

with green as 
(select 
  data_source,
  pickup_date,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  store_and_fwd_flag,
  ratecode_id,
  pickup_location_id,
  dropoff_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  extra_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge,
  creation_dt,
  clone_dt,
  row_number() over (order by vendor_id, pickup_datetime, dropoff_datetime, store_and_fwd_flag, 
                              ratecode_id, pickup_location_id, dropoff_location_id, passenger_count, 
                              trip_distance, fare_amount, extra_amount, mta_tax, tip_amount, 
                              tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, 
                              trip_type, congestion_surcharge) row_num
from {{ ref('green__3_data_type') }}
)
select 
    cast(parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') as timestamp) trip_type_start_date,
    data_source,
    pickup_date,
    cast(last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) as timestamp) trip_type_end_date,
    regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type_source,
    {{ dbt_utils.generate_surrogate_key( ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'store_and_fwd_flag', 
                                          'ratecode_id', 'pickup_location_id', 'dropoff_location_id', 'passenger_count', 
                                          'trip_distance', 'fare_amount', 'extra_amount', 'mta_tax', 'tip_amount', 
                                          'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type', 
                                          'trip_type', 'congestion_surcharge', 'row_num']) }} trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra_amount,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    creation_dt,
    clone_dt,
    {{ dbt.current_timestamp() }} transformation_dt
from green

{% if is_incremental() %}

where data_source not in (select distinct data_source from {{ this }})

{% endif %}

{% if var('is_test_run', default = true) %}

  limit 10000 

{% endif %}