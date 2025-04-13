

  create or replace view `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__3_data_type`
  OPTIONS()
  as select 
    safe_cast(hvfhs_license_number as string) hvfhs_license_number, 
    safe_cast(dispatching_base_number as string) dispatching_base_number, 
    safe_cast(originating_base_number as string) originating_base_number,
    cast(request_datetime as timestamp) request_datetime,
    cast(on_scene_datetime as timestamp) on_scene_datetime,
    cast(pickup_datetime as timestamp) pickup_datetime,
    cast(dropoff_datetime as timestamp) dropoff_datetime, 
    safe_cast(pickup_location_id as INT64) pickup_location_id,
    safe_cast(dropoff_location_id as INT64) dropoff_location_id,
    cast(trip_distance as float64) trip_distance, 
    safe_cast(trip_time as INT64)  trip_time, 
    cast(base_passenger_fare as float64) base_passenger_fare, 
    cast(toll_amount as float64) toll_amount,
    cast(black_card_fund_amount as float64) black_card_fund_amount,
    cast(sales_tax as float64) sales_tax,
    cast(congestion_surcharge as float64) congestion_surcharge,
    cast(airport_fee as float64) airport_fee,
    cast(tip_amount as float64) tip_amount,
    cast(driver_pay_amount as float64) driver_pay_amount,
    safe_cast(shared_request_flag as string) shared_request_flag,
    safe_cast(shared_match_flag as string) shared_match_flag,
    safe_cast(access_a_ride_flag as string) access_a_ride_flag,
    safe_cast(wav_request_flag as string) wav_request_flag, 
    safe_cast(wav_match_flag as string) wav_match_flag, 
    cast(pickup_date as timestamp) pickup_date,
    safe_cast(data_source as string) data_source,
    cast(creation_dt as timestamp) creation_dt,
    cast(clone_dt as timestamp) clone_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__2_tbl_collation`;

