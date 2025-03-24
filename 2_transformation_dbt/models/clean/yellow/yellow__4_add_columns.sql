select 
  {{ dbt_utils.generate_surrogate_key( ['vendor_id', 'pickup_datetime ', 'dropoff_datetime', 'passenger_count', 
                                        'trip_distance', 'pickup_location_id', 'ratecode_id', 'store_and_fwd_flag', 
                                        'dropoff_location_id', 'payment_type', 'fare_amount', 'mta_tax', 'tip_amount', 
                                        'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge',
                                        'data_source']) }} trip_id,
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
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  pickup_date,
  regexp_replace(regexp_substr(data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type,
  parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') trip_type_start_date,
  last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) trip_type_end_date,
  data_source,
  creation_dt
from {{ ref('yellow__3_data_type_cast') }}