

  create or replace view `pipeline-analysis-452722`.`nytaxi_stage2`.`stg_yellow__1a_id_duplicate_records`
  OPTIONS()
  as 

select 
  pickup_datetime, dropoff_datetime, ratecode_id, pickup_location_id, dropoff_location_id, passenger_count, trip_distance
  , count(1) row_count
  , sum(fare_amount) total_fare_amount
from `pipeline-analysis-452722`.`nytaxi_clean`.`yellow__4_adds_columns`
group by pickup_datetime, dropoff_datetime, ratecode_id, pickup_location_id, dropoff_location_id, passenger_count, trip_distance;

