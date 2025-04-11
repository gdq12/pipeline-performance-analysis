

  create or replace view `pipeline-analysis-455005`.`nytaxi_clean`.`yellow__2_post_2010_tbl_collation`
  OPTIONS()
  as with trps as 
(





  

  

  select 
    
      airport_fee as airport_fee
        , 
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      extra as extra_amount
        , 
      fare_amount as fare_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      mta_tax as mta_tax
        , 
      passenger_count as passenger_count
        , 
      payment_type as payment_type
        , 
      tpep_pickup_date as pickup_date
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      pu_location_id as pickup_location_id
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      total_amount as total_amount
        , 
      trip_distance as trip_distance
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`yellow_tripdata_2020-08`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
        , 
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      extra as extra_amount
        , 
      fare_amount as fare_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      mta_tax as mta_tax
        , 
      passenger_count as passenger_count
        , 
      payment_type as payment_type
        , 
      tpep_pickup_date as pickup_date
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      pu_location_id as pickup_location_id
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      total_amount as total_amount
        , 
      trip_distance as trip_distance
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`yellow_tripdata_2020-10`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
        , 
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      extra as extra_amount
        , 
      fare_amount as fare_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      mta_tax as mta_tax
        , 
      passenger_count as passenger_count
        , 
      payment_type as payment_type
        , 
      tpep_pickup_date as pickup_date
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      pu_location_id as pickup_location_id
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      total_amount as total_amount
        , 
      trip_distance as trip_distance
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`yellow_tripdata_2020-11`
    
)
select 
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_location_id,
  ratecode_id,
  store_and_fwd_flag,
  dropoff_location_id,
  payment_type,
  fare_amount,
  extra_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  pickup_date,
  data_source,
  creation_dt,
  clone_dt
from trps;

