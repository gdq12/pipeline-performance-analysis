

  create or replace view `pipeline-analysis-452722`.`nytaxi_stage`.`vw_yellow_tripdata`
  OPTIONS()
  as 

with t1 as 
(





  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-01`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-02`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-03`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-04`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2012-12`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2013-01`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2013-05`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2013-06`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-01`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-02`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-08`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-09`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-01`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-02`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-06`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-07`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-09`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-10`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-11`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-12`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2019-01`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2020-08`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2020-10`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2020-11`
    union all 

  

  

  select 
    
      vendor_id as vendor_id
        , 
      tpep_pickup_datetime as pickup_datetime
        , 
      tpep_dropoff_datetime as dropoff_datetime
        , 
      passenger_count as passenger_count
        , 
      trip_distance as trip_distance
        , 
      ratecode_id as ratecode_id
        , 
      store_and_fwd_flag as store_and_fwd_flag
        , 
      pu_location_id as pickup_location_id
        , 
      do_location_id as dropoff_location_id
        , 
      payment_type as payment_type
        , 
      fare_amount as fare_amount
        , 
      extra as extra_amount
        , 
      mta_tax as mta_tax
        , 
      tip_amount as tip_amount
        , 
      tolls_amount as tolls_amount
        , 
      improvement_surcharge as improvement_surcharge
        , 
      total_amount as total_amount
        , 
      congestion_surcharge as congestion_surcharge
        , 
      airport_fee as airport_fee
        , 
      tpep_pickup_date as pickup_date
        , 
      data_source as data_source
        , 
      creation_dt as creation_dt
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2023-02`
    
)
select 
  trp.vendor_id
  , trp.pickup_datetime
  , trp.dropoff_datetime
  , trp.passenger_count
  , trp.trip_distance
  , trp.pickup_location_id
  , trp.ratecode_id
  , trp.store_and_fwd_flag
  , trp.dropoff_location_id
  , trp.payment_type
  , trp.fare_amount
  , trp.mta_tax
  , trp.tip_amount
  , trp.tolls_amount
  , trp.improvement_surcharge
  , trp.total_amount
  , trp.congestion_surcharge
  , trp.pickup_date
  , regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$') trip_type
  , parse_datetime('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01') data_start_date
  , last_day(parse_date('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) data_end_date
  , trp.data_source
  , trp.creation_dt
from t1 trp;

