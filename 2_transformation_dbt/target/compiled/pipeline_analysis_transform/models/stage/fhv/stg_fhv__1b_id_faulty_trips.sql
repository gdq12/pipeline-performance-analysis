select 
    data_source, 
    trip_id
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__4_adds_columns` fhv 
left join `pipeline-analysis-455005`.`nytaxi_mapping`.`hvlv_base_numbers` mp on fhv.dispatching_base_number = mp.license_base_number
left join `pipeline-analysis-455005`.`nytaxi_mapping`.`hvlv_base_numbers` mp2 on fhv.affiliated_base_number = mp2.license_base_number
where (
      -- invalid trip timestamps
      (pickup_datetime >= dropoff_datetime)
      or 
      -- trip timestamps not inline with source parquet
      (pickup_datetime < cast(trip_type_start_date as timestamp))
      or 
      -- trips with unknown pickup or dropoff location
      (coalesce(pickup_location_id, 264) = 264 or coalesce(dropoff_location_id, 264) = 264)
      or 
      -- trip sr_flag incorrectly flagged (ride not associated with an HVLN)
      (sr_flag = 1 and (mp.license_base_number is null or mp2.license_base_number is null))
)