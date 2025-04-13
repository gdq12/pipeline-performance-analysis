

  create or replace view `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__2_tbl_collation`
  OPTIONS()
  as with trps as 
(





  

  

  select 
    
      affiliated_base_number as affiliated_base_number
        , 
      clone_dt as clone_dt
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      dispatching_base_num as dispatching_base_number
        , 
      drop_off_datetime as dropoff_datetime
        , 
      d_olocation_id as dropoff_location_id
        , 
      pickup_date as pickup_date
        , 
      pickup_datetime as pickup_datetime
        , 
      p_ulocation_id as pickup_location_id
        , 
      sr_flag as sr_flag
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhv_tripdata_2020-07`
    union all 

  

  

  select 
    
      affiliated_base_number as affiliated_base_number
        , 
      clone_dt as clone_dt
        , 
      creation_dt as creation_dt
        , 
      data_source as data_source
        , 
      dispatching_base_num as dispatching_base_number
        , 
      drop_off_datetime as dropoff_datetime
        , 
      d_olocation_id as dropoff_location_id
        , 
      pickup_date as pickup_date
        , 
      pickup_datetime as pickup_datetime
        , 
      p_ulocation_id as pickup_location_id
        , 
      sr_flag as sr_flag
        
  from `pipeline-analysis-455005`.`nytaxi_raw`.`fhv_tripdata_2020-08`
    
)
select 
    dispatching_base_number,
    pickup_datetime, 
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    sr_flag, 
    affiliated_base_number,
    pickup_date,
    data_source, 
    creation_dt, 
    clone_dt
from trps;

