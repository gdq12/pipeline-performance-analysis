select 
  hvfhs_license_number, 
  dispatching_base_number, 
  originating_base_number, 
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id, 
  dropoff_location_id,
  trip_distance, 
  trip_time, 
  count(*) row_count,
  sum(base_passenger_fare) total_fare_amount
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__4_adds_columns`
group by hvfhs_license_number, 
        dispatching_base_number, 
        originating_base_number, 
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id, 
        dropoff_location_id,
        trip_distance, 
        trip_time