

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
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__3_data_type`
)
select 
    cast(parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') as timestamp) trip_type_start_date,
    data_source,
    pickup_date,
    cast(last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) as timestamp) trip_type_end_date,
    regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type_source,
    to_hex(md5(cast(coalesce(cast(dispatching_base_number as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(sr_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(affiliated_base_number as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_date as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(row_num as string), '_dbt_utils_surrogate_key_null_') as string))) trip_id,
    dispatching_base_number,
    pickup_datetime, 
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    sr_flag, 
    affiliated_base_number,
    creation_dt, 
    clone_dt,
    current_timestamp() transformation_dt
from fhv



where data_source not in (select distinct data_source from `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__4_adds_columns`)





  limit 10000 

