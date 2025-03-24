

  create or replace view `pipeline-analysis-452722`.`nytaxi_clean`.`yellow__2_post_2010_tbl_collation`
  OPTIONS()
  as 

with trps as 
(





  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-01`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-02`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-03`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2011-04`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2012-12`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2013-01`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2013-05`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2013-06`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-01`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-02`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-08`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2014-09`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-01`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-02`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-06`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-07`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-09`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-10`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-11`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2018-12`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2019-01`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2020-08`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2020-10`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2020-11`
    union all 

  

  

  select 
    
      airport_fee as airport_fee
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
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2023-02`
    
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
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  pickup_date,
  data_source,
  creation_dt
from trps;

