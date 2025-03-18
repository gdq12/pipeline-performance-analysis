

  create or replace view `pipeline-analysis-452722`.`nytaxi_transform_stage`.`st_yellow_columns`
  OPTIONS()
  as 



select
  
    passenger_count 
      , 
    trip_distance 
      , 
    payment_type 
      , 
    mta_tax 
      , 
    data_source 
      , 
    creation_dt 
      , 
    vendor_id 
      , 
    store_and_fwd_flag 
      , 
    fare_amount 
      , 
    tip_amount 
      , 
    tolls_amount 
      , 
    total_amount 
      , 
    tpep_pickup_datetime 
      , 
    tpep_dropoff_datetime 
      , 
    ratecode_id 
      , 
    pu_location_id 
      , 
    do_location_id 
      , 
    extra 
      , 
    improvement_surcharge 
      , 
    congestion_surcharge 
      , 
    airport_fee 
      , 
    tpep_pickup_date 
      , 
    rate_code 
      , 
    surcharge 
      , 
    pickup_datetime 
      , 
    dropoff_datetime 
      , 
    pickup_longitude 
      , 
    pickup_latitude 
      , 
    dropoff_longitude 
      , 
    dropoff_latitude 
      , 
    pickup_date 
      , 
    vendor_name 
      , 
    trip_pickup_date_time 
      , 
    trip_dropoff_date_time 
      , 
    start_lon 
      , 
    start_lat 
      , 
    store_and_forward 
      , 
    end_lon 
      , 
    end_lat 
      , 
    fare_amt 
      , 
    tip_amt 
      , 
    tolls_amt 
      , 
    total_amt 
      , 
    trip_pickup_date 
      

from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2009-01`;

