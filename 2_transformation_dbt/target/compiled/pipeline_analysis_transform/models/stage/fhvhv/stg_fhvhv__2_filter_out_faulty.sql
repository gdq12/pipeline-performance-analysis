select 
    tbl.trip_type_start_date,
    tbl.data_source,
    tbl.pickup_date,
    tbl.trip_type_end_date,
    tbl.trip_type_source,
    tbl.trip_id,
    tbl.hvfhs_license_number, 
    tbl.dispatching_base_number, 
    tbl.originating_base_number,
    tbl.request_datetime,
    tbl.on_scene_datetime,
    tbl.pickup_datetime,
    tbl.dropoff_datetime, 
    tbl.pickup_location_id,
    tbl.dropoff_location_id,
    tbl.trip_distance, 
    tbl.trip_time, 
    tbl.base_passenger_fare, 
    tbl.toll_amount,
    tbl.black_card_fund_amount,
    tbl.sales_tax,
    tbl.congestion_surcharge,
    tbl.airport_fee,
    tbl.tip_amount,
    tbl.driver_pay_amount,
    tbl.shared_request_flag,
    tbl.shared_match_flag,
    tbl.access_a_ride_flag,
    tbl.wav_request_flag, 
    tbl.wav_match_flag, 
    tbl.creation_dt,
    tbl.clone_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__4_adds_columns` tbl
left join `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhvhv__1b_id_faulty_trips` ft1 on tbl.data_source = ft1.data_source 
                                                    and tbl.trip_id = ft1.trip_id
left join `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhvhv__1a_id_duplicate_records` ft2 on tbl.hvfhs_license_number = ft2.hvfhs_license_number 
                                                        and tbl.dispatching_base_number = ft2.dispatching_base_number 
                                                        and tbl.originating_base_number = ft2.originating_base_number 
                                                        and tbl.pickup_datetime = ft2.pickup_datetime
                                                        and tbl.dropoff_datetime = ft2.dropoff_datetime
                                                        and tbl.pickup_location_id = ft2.pickup_location_id 
                                                        and tbl.dropoff_location_id = ft2.dropoff_location_id
                                                        and tbl.trip_distance = ft2.trip_distance 
                                                        and tbl.trip_time = ft2.trip_time
                                                        and ft2.row_count = 2
                                                        and ft2.total_fare_amount = 0
                                                        and tbl.base_passenger_fare < 0
where ft1.data_source is null and ft2.pickup_location_id is null