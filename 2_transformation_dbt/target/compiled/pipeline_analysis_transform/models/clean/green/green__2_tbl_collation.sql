with trps as 
(




)
select 
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
    pickup_date,
    data_source,
    creation_dt,
    clone_dt
from trps