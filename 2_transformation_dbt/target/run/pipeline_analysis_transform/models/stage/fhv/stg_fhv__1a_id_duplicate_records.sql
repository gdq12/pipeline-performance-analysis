

  create or replace view `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhv__1a_id_duplicate_records`
  OPTIONS()
  as select 
    dispatching_base_number,
    pickup_datetime, 
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    sr_flag, 
    affiliated_base_number,
    count(1) row_count,
    max(trip_id) second_trip_id
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__4_adds_columns`
group by dispatching_base_number,
        pickup_datetime, 
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        sr_flag, 
        affiliated_base_number;

