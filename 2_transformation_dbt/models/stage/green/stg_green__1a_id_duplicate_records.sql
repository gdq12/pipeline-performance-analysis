select 
  pickup_datetime,
  dropoff_datetime, 
  ratecode_id, 
  pickup_location_id, 
  dropoff_location_id, 
  passenger_count, 
  trip_distance,
  count(1) row_count,
  sum(fare_amount) total_fare_amount
from {{ ref('green__4_adds_columns') }}
group by pickup_datetime,
        dropoff_datetime, 
        ratecode_id, 
        pickup_location_id, 
        dropoff_location_id, 
        passenger_count, 
        trip_distance