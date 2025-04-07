with trps as 
(




)
select 
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_longitude, 
  pickup_latitude,
  ratecode_id,
  store_and_fwd_flag,
  dropoff_longitude, 
  dropoff_latitude,
  payment_type,
  fare_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  total_amount,
  congestion_surcharge,
  pickup_date,
  data_source,
  creation_dt
from trps