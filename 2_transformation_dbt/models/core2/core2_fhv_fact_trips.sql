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
    trp.dispatching_base_number,
    trp.affiliated_base_number,
    -- time centric dimensions 
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
    -- trip categorization
    trp.sr_flag, 
    -- data source centric info 
    trp.clone_dt,
    {{ dbt.current_timestamp() }} transformation_dt
from {{ ref('stg_fhv__2_filter_out_faulty') }} trp 

{% if is_incremental() %}

where trp.data_source not in (select distinct data_source from {{ this }})

{% endif %}

{% if var('is_test_run', default = true) %}

  limit 100 

{% endif %}