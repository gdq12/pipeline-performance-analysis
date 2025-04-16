
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_clean`.`yellow__4_adds_columns`
        
  (
    trip_type_start_date datetime,
    data_source string,
    pickup_date timestamp,
    trip_type_end_date timestamp,
    trip_type_source string,
    trip_id string,
    vendor_id int64,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    passenger_count int64,
    trip_distance float64,
    pickup_location_id int64,
    ratecode_id int64,
    store_and_fwd_flag string,
    dropoff_location_id int64,
    payment_type int64,
    fare_amount float64,
    extra_amount float64,
    mta_tax float64,
    tip_amount float64,
    tolls_amount float64,
    improvement_surcharge float64,
    total_amount float64,
    congestion_surcharge float64,
    airport_fee float64,
    creation_dt timestamp,
    clone_dt timestamp,
    transformation_dt timestamp
    
    )

      
    partition by timestamp_trunc(trip_type_start_date, month)
    cluster by data_source, pickup_date

    OPTIONS()
    as (
      
    select trip_type_start_date, data_source, pickup_date, trip_type_end_date, trip_type_source, trip_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, pickup_location_id, ratecode_id, store_and_fwd_flag, dropoff_location_id, payment_type, fare_amount, extra_amount, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, creation_dt, clone_dt, transformation_dt
    from (
        

with yellow as 
(select 
  data_source,
  pickup_date,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_location_id,
  ratecode_id,
  store_and_fwd_flag,
  dropoff_location_id,
  payment_type,
  fare_amount,
  extra_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  creation_dt,
  clone_dt,
  row_number() over (order by vendor_id, pickup_datetime , dropoff_datetime, passenger_count, 
                          trip_distance, pickup_location_id, ratecode_id, store_and_fwd_flag, 
                          dropoff_location_id, payment_type, fare_amount, extra_amount,
                          mta_tax, tip_amount, tolls_amount, improvement_surcharge, 
                          total_amount, congestion_surcharge, airport_fee) row_num
from `pipeline-analysis-455005`.`nytaxi_clean`.`yellow__3_data_type_cast`
)
select 
  parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') trip_type_start_date,
  data_source,
  pickup_date,
  cast(last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) as timestamp) trip_type_end_date,
  regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type_source,
  to_hex(md5(cast(coalesce(cast(vendor_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime  as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(passenger_count as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(trip_distance as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ratecode_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(store_and_fwd_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(payment_type as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fare_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(extra_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(mta_tax as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tip_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tolls_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(improvement_surcharge as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(total_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(congestion_surcharge as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(airport_fee as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(row_num as string), '_dbt_utils_surrogate_key_null_') as string))) trip_id,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_location_id,
  ratecode_id,
  store_and_fwd_flag,
  dropoff_location_id,
  payment_type,
  fare_amount,
  extra_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  creation_dt,
  clone_dt,
  current_timestamp() transformation_dt
from yellow




    ) as model_subq
    );
  