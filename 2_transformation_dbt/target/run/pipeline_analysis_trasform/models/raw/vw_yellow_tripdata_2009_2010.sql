

  create or replace view `pipeline-analysis-452722`.`nytaxi_stage`.`vw_yellow_tripdata_2009_2010`
  OPTIONS()
  as 

with t1 as 
(





  

  

  select 
    
          
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
          
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2009-01`
    union all 

  

  

  select 
    
          
            surcharge as congestion_surcharge
          
        , 
          
            creation_dt as creation_dt
          
        , 
          
            data_source as data_source
          
        , 
          
            dropoff_datetime as dropoff_datetime
          
        , 
          
            dropoff_latitude as dropoff_latitude
          
        , 
          
            dropoff_longitude as dropoff_longitude
          
        , 
          
            fare_amount as fare_amount
          
        , 
          
            mta_tax as mta_tax
          
        , 
          
            passenger_count as passenger_count
          
        , 
          
            payment_type as payment_type
          
        , 
          
            pickup_date as pickup_date
          
        , 
          
            pickup_datetime as pickup_datetime
          
        , 
          
            pickup_latitude as pickup_latitude
          
        , 
          
            pickup_longitude as pickup_longitude
          
        , 
          
            safe_cast(rate_code as INT64) as ratecode_id
          
        , 
          
            safe_cast(store_and_fwd_flag as string) as store_and_fwd_flag
          
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
          
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2010-01`
    union all 

  

  

  select 
    
          
            surcharge as congestion_surcharge
          
        , 
          
            creation_dt as creation_dt
          
        , 
          
            data_source as data_source
          
        , 
          
            dropoff_datetime as dropoff_datetime
          
        , 
          
            dropoff_latitude as dropoff_latitude
          
        , 
          
            dropoff_longitude as dropoff_longitude
          
        , 
          
            fare_amount as fare_amount
          
        , 
          
            mta_tax as mta_tax
          
        , 
          
            passenger_count as passenger_count
          
        , 
          
            payment_type as payment_type
          
        , 
          
            pickup_date as pickup_date
          
        , 
          
            pickup_datetime as pickup_datetime
          
        , 
          
            pickup_latitude as pickup_latitude
          
        , 
          
            pickup_longitude as pickup_longitude
          
        , 
          
            safe_cast(rate_code as INT64) as ratecode_id
          
        , 
          
            safe_cast(store_and_fwd_flag as string) as store_and_fwd_flag
          
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
          
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2010-02`
    union all 

  

  

  select 
    
          
            surcharge as congestion_surcharge
          
        , 
          
            creation_dt as creation_dt
          
        , 
          
            data_source as data_source
          
        , 
          
            dropoff_datetime as dropoff_datetime
          
        , 
          
            dropoff_latitude as dropoff_latitude
          
        , 
          
            dropoff_longitude as dropoff_longitude
          
        , 
          
            fare_amount as fare_amount
          
        , 
          
            mta_tax as mta_tax
          
        , 
          
            passenger_count as passenger_count
          
        , 
          
            payment_type as payment_type
          
        , 
          
            pickup_date as pickup_date
          
        , 
          
            pickup_datetime as pickup_datetime
          
        , 
          
            pickup_latitude as pickup_latitude
          
        , 
          
            pickup_longitude as pickup_longitude
          
        , 
          
            safe_cast(rate_code as INT64) as ratecode_id
          
        , 
          
            safe_cast(store_and_fwd_flag as string) as store_and_fwd_flag
          
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
          
        
  from `pipeline-analysis-452722`.`nytaxi_stage`.`yellow_tripdata_2010-04`
    
)
select 
  trp.vendor_id
  , trp.pickup_datetime
  , trp.dropoff_datetime
  , trp.passenger_count
  , trp.trip_distance
  , pu.zone_id pickup_location_id
  , trp.ratecode_id
  , trp.store_and_fwd_flag
  , du.zone_id dropoff_location_id
  , trp.payment_type
  , trp.fare_amount
  , trp.mta_tax
  , trp.tip_amount
  , trp.tolls_amount
  , trp.total_amount
  , trp.congestion_surcharge
  , trp.pickup_date
  , regexp_replace(regexp_substr(trp.data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type
  , parse_datetime('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01') data_start_date
  , last_day(parse_date('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) data_end_date
  , trp.data_source
  , trp.creation_dt
from t1 trp
join `pipeline-analysis-452722`.`mapping`.`taxi_zone_geom` pu on (ST_DWithin(pu.zone_geom,ST_GeogPoint(trp.pickup_longitude, trp.pickup_latitude), 0))
join `pipeline-analysis-452722`.`mapping`.`taxi_zone_geom` du on (ST_DWithin(du.zone_geom,ST_GeogPoint(trp.dropoff_longitude, trp.dropoff_latitude), 0))
where trp.pickup_longitude between -90 and 90 
and trp.pickup_latitude between -90 and 90
and trp.dropoff_longitude between -90 and 90 
and trp.dropoff_latitude between -90 and 90;

