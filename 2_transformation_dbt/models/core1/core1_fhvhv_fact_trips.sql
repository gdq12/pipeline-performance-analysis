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
    tbl.trip_type_start_date,
    tbl.data_source,
    tbl.trip_type_source,
    tbl.pickup_date,
    -- IDs
    tbl.trip_id,
    tbl.hvfhs_license_number, 
    mp.app_company_affiliation hvfs_description,
    tbl.dispatching_base_number, 
    tbl.originating_base_number,
    -- time centric dimensions
    tbl.request_datetime,
    tbl.on_scene_datetime,
    tbl.pickup_datetime,
    tbl.dropoff_datetime, 
    tbl.trip_type_end_date,
    -- more time dimensions for later analysis
    tbl.trip_time,
    {{ dbt.datediff("pickup_datetime", "dropoff_datetime", "minute") }} trip_duration_min,
    extract(year from trp.pickup_datetime) pickup_year,
    extract(dayofweek from trp.pickup_datetime) pickup_weekday_num,
    {{ get_weekday_name("pickup_datetime") }} pickup_weekday_name,
    extract(month from trp.pickup_datetime) pickup_month,
    extract(hour from trp.pickup_datetime) pickup_hour,
    bigfunctions.eu.is_public_holiday(cast(trp.pickup_datetime as date), 'US') pickup_public_holiday,
    {{ get_rush_hour_status("pickup_datetime") }} pickup_rush_hour_status,
    extract(year from dropoff_datetime) dropoff_year,
    extract(dayofweek from trp.dropoff_datetime) dropoff_weekday_num,
    {{ get_weekday_name("dropoff_datetime") }} dropoff_weekday_name,
    extract(month from trp.dropoff_datetime) dropoff_month,
    extract(hour from trp.dropoff_datetime) dropoff_hour,
    bigfunctions.eu.is_public_holiday(cast(trp.dropoff_datetime as date), 'US') dropoff_public_holiday,
    {{ get_rush_hour_status("dropoff_datetime") }} dropoff_rush_hour_status,
    -- location centric info 
    pz.borough pickup_borough,
    pz.zone pickup_zone,
    pz.service_zone pickup_service_zone,
    dz.borough dropoffp_borough,
    dz.zone dropoff_zone,
    dz.service_zone dropoff_service_zone,
    -- trip metrics
    tbl.trip_distance, 
    -- trip categorization
    tbl.shared_request_flag,
    tbl.shared_match_flag,
    tbl.access_a_ride_flag,
    tbl.wav_request_flag, 
    tbl.wav_match_flag, 
    -- revenue centric stats
    tbl.base_passenger_fare, 
    tbl.toll_amount,
    tbl.black_card_fund_amount,
    tbl.sales_tax,
    tbl.congestion_surcharge,
    tbl.airport_fee,
    tbl.tip_amount,
    tbl.driver_pay_amount,
    -- data source centric info 
    tbl.clone_dt
    {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('stg_fhvhv__2_filter_out_faulty') }} trp 
join {{ source('mapping.map', 'taxi_zone_lookup') }} pz on trp.pickup_location_id = pz.location_id 
join {{ source('mapping.map', 'taxi_zone_lookup') }} dz on trp.dropoff_location_id = pz.location_id 
join {{ source('mapping.map', 'hvlv_base_numbers') }} mp on tbl.hvfhs_license_number = mp.hvln 

{% if var('is_test_run', default = true) %}

  limit 100 

{% endif %}