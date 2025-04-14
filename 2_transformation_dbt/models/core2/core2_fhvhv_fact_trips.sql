{{ config(
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
    trp.trip_type_end_date,
    -- IDs
    trp.trip_id,
    trp.hvfhs_license_number, 
    trp.dispatching_base_number, 
    trp.originating_base_number,
    -- time centric dimensions
    trp.request_datetime,
    trp.on_scene_datetime,
    trp.trip_time,
    {{ dbt.datediff("pickup_datetime", "dropoff_datetime", "minute") }} trip_duration_min,
    trp.pickup_datetime,
    {{ get_weekday_name("pickup_datetime") }} pickup_weekday_name,
    bigfunctions.eu.is_public_holiday(cast(trp.pickup_datetime as date), 'US') pickup_public_holiday,
    {{ get_rush_hour_status("pickup_datetime") }} pickup_rush_hour_status,
    trp.dropoff_datetime, 
    {{ get_weekday_name("dropoff_datetime") }} dropoff_weekday_name,
    bigfunctions.eu.is_public_holiday(cast(trp.dropoff_datetime as date), 'US') dropoff_public_holiday,
    {{ get_rush_hour_status("dropoff_datetime") }} dropoff_rush_hour_status,
    -- location centric info 
    trp.pickup_location_id,
    trp.dropoff_location_id,
    -- trip metrics
    trp.trip_distance, 
    -- trip categorization
    trp.shared_request_flag,
    trp.shared_match_flag,
    trp.access_a_ride_flag,
    trp.wav_request_flag, 
    trp.wav_match_flag, 
    -- revenue centric stats
    trp.base_passenger_fare, 
    trp.toll_amount,
    trp.black_card_fund_amount,
    trp.sales_tax,
    trp.congestion_surcharge,
    trp.airport_fee,
    trp.tip_amount,
    trp.driver_pay_amount,
    -- data source centric info 
    trp.clone_dt,
    {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('stg_fhvhv__2_filter_out_faulty') }} trp 

{% if is_incremental() %}

where trp.data_source not in (select distinct data_source from {{ this }})

{% endif %}

{% if var('is_test_run', default = true) %}

  limit 100 

{% endif %}