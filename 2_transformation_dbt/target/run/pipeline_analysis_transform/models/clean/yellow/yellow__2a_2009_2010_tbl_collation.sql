

  create or replace view `pipeline-analysis-455005`.`nytaxi_clean`.`yellow__2a_2009_2010_tbl_collation`
  OPTIONS()
  as with trps as 
(





  

  

  select 
    
          
            clone_dt as clone_dt
          
        , 
          
            surcharge as congestion_surcharge
          
        , 
          
            creation_dt as creation_dt
          
        , 
          
            data_source as data_source
          
        , 
          
            trip_dropoff_date_time as dropoff_datetime
          
        , 
          
            end_lat as dropoff_latitude
          
        , 
          
            end_lon as dropoff_longitude
          
        , 
          
            fare_amt as fare_amount
          
        , 
          
            mta_tax as mta_tax
          
        , 
          
            passenger_count as passenger_count
          
        , 
          
            payment_type as payment_type
          
        , 
          
            trip_pickup_date as pickup_date
          
        , 
          
            trip_pickup_date_time as pickup_datetime
          
        , 
          
            start_lat as pickup_latitude
          
        , 
          
            start_lon as pickup_longitude
          
        , 
          
            safe_cast(rate_code as INT64) as ratecode_id
          
        , 
          
            safe_cast(store_and_forward as string) as store_and_fwd_flag
          
        , 
          
            tip_amt as tip_amount
          
        , 
          
            tolls_amt as tolls_amount
          
        , 
          
            total_amt as total_amount
          
        , 
          
            trip_distance as trip_distance
          
        , 
          
            vendor_name as vendor_id
          
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`yellow_tripdata_2009-01`
    
)
select 
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_longitude, 
  pickup_latitude,
  ratecode_id,
  store_and_fwd_flag,
  dropoff_longitude, 
  dropoff_latitude,
  payment_type,
  fare_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  total_amount,
  congestion_surcharge,
  pickup_date,
  data_source,
  creation_dt,
  clone_dt
from trps;

