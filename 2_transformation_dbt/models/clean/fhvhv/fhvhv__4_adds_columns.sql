{{ config(
    materialized="incremental",
    partition_by={
      "field": "trip_type_start_date",
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by = ["data_source", "pickup_date"]
)}}

with fhvhv as 
(select 
  data_source,
  pickup_date,
  hvfhs_license_number, 
  dispatching_base_number, 
  originating_base_number,
  request_datetime,
  on_scene_datetime,
  pickup_datetime,
  dropoff_datetime, 
  pickup_location_id,
  dropoff_location_id,
  trip_distance, 
  trip_time, 
  base_passenger_fare, 
  toll_amount,
  black_card_fund_amount,
  sales_tax,
  congestion_surcharge,
  airport_fee,
  tip_amount,
  driver_pay_amount,
  shared_request_flag,
  shared_match_flag,
  access_a_ride_flag,
  wav_request_flag, 
  wav_match_flag, 
  creation_dt,
  clone_dt, 
  row_number() over (order by hvfhs_license_number,dispatching_base_number,originating_base_number, 
                              request_datetime, on_scene_datetime, pickup_datetime, dropoff_datetime,
                              pickup_location_id, dropoff_location_id, trip_distance,trip_time,
                              base_passenger_fare,toll_amount, black_card_fund_amount, sales_tax, 
                              congestion_surcharge, airport_fee, tip_amount, driver_pay_amount, 
                              shared_request_flag, shared_match_flag, access_a_ride_flag, 
                              wav_request_flag,wav_match_flag) row_num
from {{ ref('fhvhv__3_data_type') }}
)
select 
    parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') trip_type_start_date,
    data_source,
    pickup_date,
    cast(last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) as timestamp) trip_type_end_date,
    regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type_source,
    {{ dbt_utils.generate_surrogate_key( ['hvfhs_license_number','dispatching_base_number','originating_base_number', 
                                        'request_datetime', 'on_scene_datetime', 'pickup_datetime', 'dropoff_datetime',
                                        'pickup_location_id', 'dropoff_location_id', 'trip_distance','trip_time',
                                        'base_passenger_fare','toll_amount', 'black_card_fund_amount', 'sales_tax', 
                                        'congestion_surcharge', 'airport_fee', 'tip_amount', 'driver_pay_amount', 
                                        'shared_request_flag', 'shared_match_flag', 'access_a_ride_flag', 
                                        'wav_request_flag','wav_match_flag', 'row_num']) }} trip_id,
    hvfhs_license_number, 
    dispatching_base_number, 
    originating_base_number,
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime, 
    pickup_location_id,
    dropoff_location_id,
    trip_distance, 
    trip_time, 
    base_passenger_fare, 
    toll_amount,
    black_card_fund_amount,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tip_amount,
    driver_pay_amount,
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag, 
    wav_match_flag, 
    creation_dt,
    clone_dt,
    {{ dbt.current_timestamp() }} transformation_dt
from fhvhv

{% if is_incremental() %}

where data_source not in (select distinct data_source from {{ this }})

{% endif %}

{% if var('is_test_run', default = true) %}

  limit 10000 

{% endif %}