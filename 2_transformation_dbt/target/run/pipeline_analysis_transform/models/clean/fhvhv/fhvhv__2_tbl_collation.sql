

  create or replace view `pipeline-analysis-455005`.`nytaxi_clean`.`fhvhv__2_tbl_collation`
  OPTIONS()
  as with trps as 
(





  

  

  select 
    
            
                access_a_ride_flag as access_a_ride_flag
            
        , 
            
                airport_fee as airport_fee
            
        , 
            
                base_passenger_fare as base_passenger_fare
            
        , 
            
                bcf as black_card_fund_amount
            
        , 
            
                congestion_surcharge as congestion_surcharge
            
        , 
            
                creation_dt as creation_dt
            
        , 
            
                data_source as data_source
            
        , 
            
                dispatching_base_num as dispatching_base_number
            
        , 
            
                driver_pay as driver_pay_amount
            
        , 
            
                dropoff_datetime as dropoff_datetime
            
        , 
            
                do_location_id as dropoff_location_id
            
        , 
            
                hvfhs_license_num as hvfhs_license_number
            
        , 
            
                on_scene_datetime as on_scene_datetime
            
        , 
            
                originating_base_num as originating_base_number
            
        , 
            
                pickup_date as pickup_date
            
        , 
            
                pickup_datetime as pickup_datetime
            
        , 
            
                pu_location_id as pickup_location_id
            
        , 
            
                request_datetime as request_datetime
            
        , 
            
                sales_tax as sales_tax
            
        , 
            
                shared_match_flag as shared_match_flag
            
        , 
            
                shared_request_flag as shared_request_flag
            
        , 
            
                tips as tip_amount
            
        , 
            
                tolls as toll_amount
            
        , 
            
                trip_miles as trip_distance
            
        , 
            
                trip_time as trip_time
            
        , 
            
                safe_cast(wav_match_flag as string) as wav_match_flag
            
        , 
            
                wav_request_flag as wav_request_flag
            
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhvhv_tripdata_2019-02`
    union all 

  

  

  select 
    
            
                access_a_ride_flag as access_a_ride_flag
            
        , 
            
                airport_fee as airport_fee
            
        , 
            
                base_passenger_fare as base_passenger_fare
            
        , 
            
                bcf as black_card_fund_amount
            
        , 
            
                congestion_surcharge as congestion_surcharge
            
        , 
            
                creation_dt as creation_dt
            
        , 
            
                data_source as data_source
            
        , 
            
                dispatching_base_num as dispatching_base_number
            
        , 
            
                driver_pay as driver_pay_amount
            
        , 
            
                dropoff_datetime as dropoff_datetime
            
        , 
            
                do_location_id as dropoff_location_id
            
        , 
            
                hvfhs_license_num as hvfhs_license_number
            
        , 
            
                on_scene_datetime as on_scene_datetime
            
        , 
            
                originating_base_num as originating_base_number
            
        , 
            
                pickup_date as pickup_date
            
        , 
            
                pickup_datetime as pickup_datetime
            
        , 
            
                pu_location_id as pickup_location_id
            
        , 
            
                request_datetime as request_datetime
            
        , 
            
                sales_tax as sales_tax
            
        , 
            
                shared_match_flag as shared_match_flag
            
        , 
            
                shared_request_flag as shared_request_flag
            
        , 
            
                tips as tip_amount
            
        , 
            
                tolls as toll_amount
            
        , 
            
                trip_miles as trip_distance
            
        , 
            
                trip_time as trip_time
            
        , 
            
                safe_cast(wav_match_flag as string) as wav_match_flag
            
        , 
            
                wav_request_flag as wav_request_flag
            
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhvhv_tripdata_2019-04`
    union all 

  

  

  select 
    
            
                access_a_ride_flag as access_a_ride_flag
            
        , 
            
                airport_fee as airport_fee
            
        , 
            
                base_passenger_fare as base_passenger_fare
            
        , 
            
                bcf as black_card_fund_amount
            
        , 
            
                congestion_surcharge as congestion_surcharge
            
        , 
            
                creation_dt as creation_dt
            
        , 
            
                data_source as data_source
            
        , 
            
                dispatching_base_num as dispatching_base_number
            
        , 
            
                driver_pay as driver_pay_amount
            
        , 
            
                dropoff_datetime as dropoff_datetime
            
        , 
            
                do_location_id as dropoff_location_id
            
        , 
            
                hvfhs_license_num as hvfhs_license_number
            
        , 
            
                on_scene_datetime as on_scene_datetime
            
        , 
            
                originating_base_num as originating_base_number
            
        , 
            
                pickup_date as pickup_date
            
        , 
            
                pickup_datetime as pickup_datetime
            
        , 
            
                pu_location_id as pickup_location_id
            
        , 
            
                request_datetime as request_datetime
            
        , 
            
                sales_tax as sales_tax
            
        , 
            
                shared_match_flag as shared_match_flag
            
        , 
            
                shared_request_flag as shared_request_flag
            
        , 
            
                tips as tip_amount
            
        , 
            
                tolls as toll_amount
            
        , 
            
                trip_miles as trip_distance
            
        , 
            
                trip_time as trip_time
            
        , 
            
                safe_cast(wav_match_flag as string) as wav_match_flag
            
        , 
            
                wav_request_flag as wav_request_flag
            
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhvhv_tripdata_2019-07`
    union all 

  

  

  select 
    
            
                access_a_ride_flag as access_a_ride_flag
            
        , 
            
                airport_fee as airport_fee
            
        , 
            
                base_passenger_fare as base_passenger_fare
            
        , 
            
                bcf as black_card_fund_amount
            
        , 
            
                congestion_surcharge as congestion_surcharge
            
        , 
            
                creation_dt as creation_dt
            
        , 
            
                data_source as data_source
            
        , 
            
                dispatching_base_num as dispatching_base_number
            
        , 
            
                driver_pay as driver_pay_amount
            
        , 
            
                dropoff_datetime as dropoff_datetime
            
        , 
            
                do_location_id as dropoff_location_id
            
        , 
            
                hvfhs_license_num as hvfhs_license_number
            
        , 
            
                on_scene_datetime as on_scene_datetime
            
        , 
            
                originating_base_num as originating_base_number
            
        , 
            
                pickup_date as pickup_date
            
        , 
            
                pickup_datetime as pickup_datetime
            
        , 
            
                pu_location_id as pickup_location_id
            
        , 
            
                request_datetime as request_datetime
            
        , 
            
                sales_tax as sales_tax
            
        , 
            
                shared_match_flag as shared_match_flag
            
        , 
            
                shared_request_flag as shared_request_flag
            
        , 
            
                tips as tip_amount
            
        , 
            
                tolls as toll_amount
            
        , 
            
                trip_miles as trip_distance
            
        , 
            
                trip_time as trip_time
            
        , 
            
                safe_cast(wav_match_flag as string) as wav_match_flag
            
        , 
            
                wav_request_flag as wav_request_flag
            
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhvhv_tripdata_2020-04`
    union all 

  

  

  select 
    
            
                access_a_ride_flag as access_a_ride_flag
            
        , 
            
                airport_fee as airport_fee
            
        , 
            
                base_passenger_fare as base_passenger_fare
            
        , 
            
                bcf as black_card_fund_amount
            
        , 
            
                congestion_surcharge as congestion_surcharge
            
        , 
            
                creation_dt as creation_dt
            
        , 
            
                data_source as data_source
            
        , 
            
                dispatching_base_num as dispatching_base_number
            
        , 
            
                driver_pay as driver_pay_amount
            
        , 
            
                dropoff_datetime as dropoff_datetime
            
        , 
            
                do_location_id as dropoff_location_id
            
        , 
            
                hvfhs_license_num as hvfhs_license_number
            
        , 
            
                on_scene_datetime as on_scene_datetime
            
        , 
            
                originating_base_num as originating_base_number
            
        , 
            
                pickup_date as pickup_date
            
        , 
            
                pickup_datetime as pickup_datetime
            
        , 
            
                pu_location_id as pickup_location_id
            
        , 
            
                request_datetime as request_datetime
            
        , 
            
                sales_tax as sales_tax
            
        , 
            
                shared_match_flag as shared_match_flag
            
        , 
            
                shared_request_flag as shared_request_flag
            
        , 
            
                tips as tip_amount
            
        , 
            
                tolls as toll_amount
            
        , 
            
                trip_miles as trip_distance
            
        , 
            
                trip_time as trip_time
            
        , 
            
                safe_cast(wav_match_flag as string) as wav_match_flag
            
        , 
            
                wav_request_flag as wav_request_flag
            
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhvhv_tripdata_2020-05`
    union all 

  

  

  select 
    
            
                access_a_ride_flag as access_a_ride_flag
            
        , 
            
                airport_fee as airport_fee
            
        , 
            
                base_passenger_fare as base_passenger_fare
            
        , 
            
                bcf as black_card_fund_amount
            
        , 
            
                congestion_surcharge as congestion_surcharge
            
        , 
            
                creation_dt as creation_dt
            
        , 
            
                data_source as data_source
            
        , 
            
                dispatching_base_num as dispatching_base_number
            
        , 
            
                driver_pay as driver_pay_amount
            
        , 
            
                dropoff_datetime as dropoff_datetime
            
        , 
            
                do_location_id as dropoff_location_id
            
        , 
            
                hvfhs_license_num as hvfhs_license_number
            
        , 
            
                on_scene_datetime as on_scene_datetime
            
        , 
            
                originating_base_num as originating_base_number
            
        , 
            
                pickup_date as pickup_date
            
        , 
            
                pickup_datetime as pickup_datetime
            
        , 
            
                pu_location_id as pickup_location_id
            
        , 
            
                request_datetime as request_datetime
            
        , 
            
                sales_tax as sales_tax
            
        , 
            
                shared_match_flag as shared_match_flag
            
        , 
            
                shared_request_flag as shared_request_flag
            
        , 
            
                tips as tip_amount
            
        , 
            
                tolls as toll_amount
            
        , 
            
                trip_miles as trip_distance
            
        , 
            
                trip_time as trip_time
            
        , 
            
                safe_cast(wav_match_flag as string) as wav_match_flag
            
        , 
            
                wav_request_flag as wav_request_flag
            
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhvhv_tripdata_2020-10`
    union all 

  

  

  select 
    
            
                access_a_ride_flag as access_a_ride_flag
            
        , 
            
                airport_fee as airport_fee
            
        , 
            
                base_passenger_fare as base_passenger_fare
            
        , 
            
                bcf as black_card_fund_amount
            
        , 
            
                congestion_surcharge as congestion_surcharge
            
        , 
            
                creation_dt as creation_dt
            
        , 
            
                data_source as data_source
            
        , 
            
                dispatching_base_num as dispatching_base_number
            
        , 
            
                driver_pay as driver_pay_amount
            
        , 
            
                dropoff_datetime as dropoff_datetime
            
        , 
            
                do_location_id as dropoff_location_id
            
        , 
            
                hvfhs_license_num as hvfhs_license_number
            
        , 
            
                on_scene_datetime as on_scene_datetime
            
        , 
            
                originating_base_num as originating_base_number
            
        , 
            
                pickup_date as pickup_date
            
        , 
            
                pickup_datetime as pickup_datetime
            
        , 
            
                pu_location_id as pickup_location_id
            
        , 
            
                request_datetime as request_datetime
            
        , 
            
                sales_tax as sales_tax
            
        , 
            
                shared_match_flag as shared_match_flag
            
        , 
            
                shared_request_flag as shared_request_flag
            
        , 
            
                tips as tip_amount
            
        , 
            
                tolls as toll_amount
            
        , 
            
                trip_miles as trip_distance
            
        , 
            
                trip_time as trip_time
            
        , 
            
                safe_cast(wav_match_flag as string) as wav_match_flag
            
        , 
            
                wav_request_flag as wav_request_flag
            
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhvhv_tripdata_2020-11`
    
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
    creation_dt
from trps;

