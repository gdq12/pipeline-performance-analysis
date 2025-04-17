
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__4_adds_columns`
        
  (
    trip_type_start_date timestamp,
    data_source string,
    pickup_date timestamp,
    trip_type_end_date timestamp,
    trip_type_source string,
    trip_id string,
    dispatching_base_number string,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    pickup_location_id int64,
    dropoff_location_id int64,
    sr_flag int64,
    affiliated_base_number string,
    creation_dt timestamp,
    clone_dt timestamp,
    transformation_dt timestamp
    
    )

      
    partition by timestamp_trunc(trip_type_start_date, month)
    cluster by data_source, pickup_date

    OPTIONS()
    as (
      
    select trip_type_start_date, data_source, pickup_date, trip_type_end_date, trip_type_source, trip_id, dispatching_base_number, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, sr_flag, affiliated_base_number, creation_dt, clone_dt, transformation_dt
    from (
        

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




    ) as model_subq
    );
  