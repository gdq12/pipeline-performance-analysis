
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__4_adds_columns`
      
    partition by timestamp_trunc(trip_type_start_date, month)
    cluster by data_source, pickup_date

    OPTIONS()
    as (
      

select 
    parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') trip_type_start_date,
    data_source,
    pickup_date,
    cast(last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) as timestamp) trip_type_end_date,
    regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type_source,
    to_hex(md5(cast(coalesce(cast(hvfhs_license_number as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dispatching_base_number as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(originating_base_number as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(request_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(on_scene_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(trip_distance as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(trip_time as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(base_passenger_fare as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(toll_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(black_card_fund_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(sales_tax as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(congestion_surcharge as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(airport_fee as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tip_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(driver_pay_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(shared_request_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(shared_match_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(access_a_ride_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(wav_request_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(wav_match_flag as string), '_dbt_utils_surrogate_key_null_') as string))) trip_id,
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
    clone_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__3_data_type`





  limit 10000 


    );
  