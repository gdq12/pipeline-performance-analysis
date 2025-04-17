

  create or replace view `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhvhv__1b_id_faulty_trips`
  OPTIONS()
  as select 
    data_source, 
    trip_id
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__4_adds_columns`
where (
      -- invalid trip timestamps
      (pickup_datetime >= dropoff_datetime)
      or 
      -- driver arrived onscene before trip request not possible 
      (on_scene_datetime >= request_datetime)
      or
      -- trip timestamps not inline with source parquet
      (pickup_datetime < cast(trip_type_start_date as timestamp))
      or 
      -- pickup timestamp predates on_scene timestamp by more than 1 minnute
      (timestamp_diff(on_scene_datetime, pickup_datetime, minute) > 1 
        and on_scene_datetime > pickup_datetime
        )
      or
      -- trips with unknown pickup or dropoff location
      (coalesce(pickup_location_id, 264) = 264 or coalesce(dropoff_location_id, 264) = 264)
      or 
      -- all trips must report some distance to be valid
      (trip_distance < 0)
      or 
      -- trips with airport fee but not an airport destination
      (airport_fee > 0 
        and (regexp_substr(pickup_location_id||' - '||dropoff_location_id, '132|138') is null 
            or regexp_substr(pickup_location_id||' - '||dropoff_location_id, '^1 - | - 1$') is null)
        )
      or 
      -- trips where a wheel chair was requested but a wheel chair friendly vehicle wasnt provided 
      (wav_request_flag = 'Y' and coalesce(wav_match_flag, 'N') = 'N')
      or 
      -- customer did not agree to a share ride but the ride was shared 
      (coalesce(shared_request_flag, 'N') = 'N' and shared_match_flag = 'Y')
      or 
      -- driver does a trip at a loss (where they make a negative profit)
      (base_passenger_fare < driver_pay_amount)
);

