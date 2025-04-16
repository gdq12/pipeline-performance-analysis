
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_core2`.`core2_green_fact_trips`
        
  (
    trip_type_start_date datetime,
    data_source string,
    trip_type_source string,
    pickup_date timestamp,
    trip_type_end_date timestamp,
    trip_id string,
    vendor_id int64,
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
    ratecode_id int64,
    store_and_fwd_flag string,
    payment_type int64,
    trip_type int64,
    passenger_count int64,
    trip_distance float64,
    fare_amount float64,
    extra_amount float64,
    mta_tax float64,
    tip_amount float64,
    tolls_amount float64,
    ehail_fee float64,
    improvement_surcharge float64,
    total_amount float64,
    congestion_surcharge float64,
    clone_dt timestamp,
    transformation_dt timestamp
    
    )

      
    partition by timestamp_trunc(trip_type_start_date, month)
    cluster by data_source, pickup_date

    OPTIONS()
    as (
      
    select trip_type_start_date, data_source, trip_type_source, pickup_date, trip_type_end_date, trip_id, vendor_id, trip_duration_min, pickup_datetime, pickup_weekday_name, pickup_public_holiday, pickup_rush_hour_status, dropoff_datetime, dropoff_weekday_name, dropoff_public_holiday, dropoff_rush_hour_status, pickup_location_id, dropoff_location_id, ratecode_id, store_and_fwd_flag, payment_type, trip_type, passenger_count, trip_distance, fare_amount, extra_amount, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, congestion_surcharge, clone_dt, transformation_dt
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
    trp.vendor_id,
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
    trp.ratecode_id,
    trp.store_and_fwd_flag,
    trp.payment_type,
    trp.trip_type,
    -- for unit centric calculations
    trp.passenger_count,
    trp.trip_distance,
    -- revenue centric stats
    trp.fare_amount,
    trp.extra_amount,
    trp.mta_tax,
    trp.tip_amount,
    trp.tolls_amount,
    trp.ehail_fee,
    trp.improvement_surcharge,
    trp.total_amount,
    trp.congestion_surcharge,
    -- data source centric info
    trp.clone_dt, 
    current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_stage`.`stg_green__2_filter_out_faulty` trp 




    ) as model_subq
    );
  