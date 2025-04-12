select 
    dispatching_base_number,
    pickup_datetime, 
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    sr_flag, 
    affiliated_base_number,
    count(1) row_count,
    max(trip_id) second_trip_id
from {{ ref('fhv__4_adds_columns') }}
group by dispatching_base_number,
        pickup_datetime, 
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        sr_flag, 
        affiliated_base_number