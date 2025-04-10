with trps as 
(




)
select 
    hvfhs_license_number, 
    dispatching_base_number, 
    originating_base_number,
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime, 
    pickup_location_id,
    dropoff_location_id,
    trip_distance, 
    trip_time, 
    base_passenger_fare, 
    toll_amount,
    black_card_fund_amount,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tip_amount,
    driver_pay_amount,
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag, 
    wav_match_flag, 
    pickup_date,
    data_source,
    creation_dt,
    clone_dt
from trps