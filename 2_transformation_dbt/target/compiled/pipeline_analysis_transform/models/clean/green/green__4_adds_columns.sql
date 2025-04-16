

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
from `pipeline-analysis-455005`.`nytaxi_clean`.`green__3_data_type`
)
select 
    parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') trip_type_start_date,
    data_source,
    pickup_date,
    cast(last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) as timestamp) trip_type_end_date,
    regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type_source,
    to_hex(md5(cast(coalesce(cast(vendor_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(store_and_fwd_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ratecode_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(passenger_count as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(trip_distance as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fare_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(extra_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(mta_tax as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tip_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tolls_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ehail_fee as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(improvement_surcharge as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(total_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(payment_type as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(trip_type as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(congestion_surcharge as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(row_num as string), '_dbt_utils_surrogate_key_null_') as string))) trip_id,
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
    current_timestamp() transformation_dt
from green



