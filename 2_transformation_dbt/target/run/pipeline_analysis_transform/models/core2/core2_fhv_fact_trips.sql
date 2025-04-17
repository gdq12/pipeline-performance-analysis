
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_core2`.`core2_fhv_fact_trips`
        
  (
    trip_type_start_date timestamp,
    data_source string,
    trip_type_source string,
    pickup_date timestamp,
    trip_type_end_date timestamp,
    trip_id string,
    dispatching_base_number string,
    affiliated_base_number string,
    trip_duration_min int64,
    pickup_datetime timestamp,
    pickup_weekday_name string,
    pickup_public_holiday boolean,
    pickup_rush_hour_status string,
    dropoff_datetime timestamp,
    dropoff_weekday_name string,
    dropoff_public_holiday boolean,
    dropoff_rush_hour_status string,
    pickup_location_id int64,
    dropoff_location_id int64,
    sr_flag int64,
    clone_dt timestamp,
    transformation_dt timestamp
    
    )

      
    partition by timestamp_trunc(trip_type_start_date, month)
    cluster by data_source, pickup_date

    OPTIONS()
    as (
      
    select trip_type_start_date, data_source, trip_type_source, pickup_date, trip_type_end_date, trip_id, dispatching_base_number, affiliated_base_number, trip_duration_min, pickup_datetime, pickup_weekday_name, pickup_public_holiday, pickup_rush_hour_status, dropoff_datetime, dropoff_weekday_name, dropoff_public_holiday, dropoff_rush_hour_status, pickup_location_id, dropoff_location_id, sr_flag, clone_dt, transformation_dt
    from (
        

select 
    -- cols that help better scan the data 
    trp.trip_type_start_date,
    trp.data_source,
    trp.trip_type_source,
    trp.pickup_date,
    trp.trip_type_end_date,
    -- IDs
    trp.trip_id,
    trp.dispatching_base_number,
    trp.affiliated_base_number,
    -- time centric dimensions 
    

    datetime_diff(
        cast(dropoff_datetime as datetime),
        cast(pickup_datetime as datetime),
        minute
    )

   trip_duration_min,
    trp.pickup_datetime,
    case 
        when extract(dayofweek from pickup_datetime) = 1
            then 'SUNDAY'
        when extract(dayofweek from pickup_datetime) = 2
            then 'MONDAY'
        when extract(dayofweek from pickup_datetime) = 3
            then 'TUESDAY'
        when extract(dayofweek from pickup_datetime) = 4
            then 'WEDNESDAY'
        when extract(dayofweek from pickup_datetime) = 5
            then 'THURSDAY'
        when extract(dayofweek from pickup_datetime) = 6
            then 'FRIDAY'
        when extract(dayofweek from pickup_datetime) = 7
            then 'SATURDAY'
    end pickup_weekday_name,
    bigfunctions.eu.is_public_holiday(cast(trp.pickup_datetime as date), 'US') pickup_public_holiday,
    case 
        when extract(dayofweek from pickup_datetime) in (2, 3, 4, 5, 6)
            and extract(hour from pickup_datetime) in (6, 7, 8, 9, 10)
            then 'MORNING RUSH HOUR'
        when extract(dayofweek from pickup_datetime) in (2, 3, 4, 5, 6)
            and extract(hour from pickup_datetime) in (4, 5, 6, 7, 8)
            then 'MORNING RUSH HOUR'
        else null 
    end pickup_rush_hour_status,
    trp.dropoff_datetime,
    case 
        when extract(dayofweek from dropoff_datetime) = 1
            then 'SUNDAY'
        when extract(dayofweek from dropoff_datetime) = 2
            then 'MONDAY'
        when extract(dayofweek from dropoff_datetime) = 3
            then 'TUESDAY'
        when extract(dayofweek from dropoff_datetime) = 4
            then 'WEDNESDAY'
        when extract(dayofweek from dropoff_datetime) = 5
            then 'THURSDAY'
        when extract(dayofweek from dropoff_datetime) = 6
            then 'FRIDAY'
        when extract(dayofweek from dropoff_datetime) = 7
            then 'SATURDAY'
    end dropoff_weekday_name,
    bigfunctions.eu.is_public_holiday(cast(trp.dropoff_datetime as date), 'US') dropoff_public_holiday,
    case 
        when extract(dayofweek from dropoff_datetime) in (2, 3, 4, 5, 6)
            and extract(hour from dropoff_datetime) in (6, 7, 8, 9, 10)
            then 'MORNING RUSH HOUR'
        when extract(dayofweek from dropoff_datetime) in (2, 3, 4, 5, 6)
            and extract(hour from dropoff_datetime) in (4, 5, 6, 7, 8)
            then 'MORNING RUSH HOUR'
        else null 
    end dropoff_rush_hour_status,
    -- location centric info 
    trp.pickup_location_id,
    trp.dropoff_location_id,
    -- trip categorization
    trp.sr_flag, 
    -- data source centric info 
    trp.clone_dt,
    current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhv__2_filter_out_faulty` trp 




    ) as model_subq
    );
  