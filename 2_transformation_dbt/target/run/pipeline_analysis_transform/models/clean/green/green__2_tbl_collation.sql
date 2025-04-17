

  create or replace view `pipeline-analysis-455005`.`nytaxi_clean`.`green__2_tbl_collation`
  OPTIONS()
  as with trps as 
(





  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2014-01`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2014-02`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2014-04`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2014-05`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2014-11`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2015-11`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2015-12`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2016-01`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2016-02`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2016-12`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2017-03`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2017-07`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2017-10`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2017-11`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2017-12`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2018-02`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2018-04`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2018-05`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2018-07`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2018-10`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2019-01`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2019-09`
    union all 

  

  

  select 
    
      clone_dt as clone_dt
        , 
      congestion_surcharge as congestion_surcharge
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      lpep_dropoff_datetime as dropoff_datetime
        , 
      do_location_id as dropoff_location_id
        , 
      ehail_fee as ehail_fee
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
      lpep_pickup_date as pickup_date
        , 
      lpep_pickup_datetime as pickup_datetime
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
      trip_type as trip_type
        , 
      vendor_id as vendor_id
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`green_tripdata_2023-02`
    
)
select 
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra_amount,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    pickup_date,
    data_source,
    creation_dt,
    clone_dt
from trps;

