{{ config(
    materialized="incremental",
    partition_by={
      "field": "trip_type_start_date",
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by = ["data_source", "pickup_date"]
)}}

with fhv as 
(select 
  data_source,
  pickup_date,
  dispatching_base_number,
  pickup_datetime, 
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  sr_flag, 
  affiliated_base_number,
  creation_dt, 
  clone_dt,
  row_number() over (order by dispatching_base_number, pickup_datetime, dropoff_datetime, 
                              pickup_location_id, dropoff_location_id, sr_flag, 
                              affiliated_base_number) row_num
from {{ ref('fhv__3_data_type') }}
)
select 
    cast(parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') as timestamp) trip_type_start_date,
    data_source,
    pickup_date,
    cast(last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) as timestamp) trip_type_end_date,
    regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type_source,
    {{ dbt_utils.generate_surrogate_key( ['dispatching_base_number', 'pickup_datetime', 'dropoff_datetime', 
                                        'pickup_location_id', 'dropoff_location_id', 'sr_flag', 
                                        'affiliated_base_number', 'pickup_date', 'row_num']) }} trip_id,
    dispatching_base_number,
    pickup_datetime, 
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    sr_flag, 
    affiliated_base_number,
    creation_dt, 
    clone_dt,
    {{ dbt.current_timestamp() }} transformation_dt
from fhv

{% if is_incremental() %}

where data_source not in (select distinct data_source from {{ this }})

{% endif %}

{% if var('is_test_run', default = true) %}

  limit 10000 

{% endif %}