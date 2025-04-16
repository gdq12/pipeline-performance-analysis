
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_core1`.`core1_yellow_fact_trips`
        
  (
    trip_type_start_date datetime,
    data_source string,
    trip_type_source string,
    pickup_date timestamp,
    trip_id string,
    vendor_id int64,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    trip_type_end_date timestamp,
    trip_duration_min int64,
    pickup_year int64,
    pickup_weekday_num int64,
    pickup_weekday_name string,
    pickup_month int64,
    pickup_hour int64,
    pickup_public_holiday boolean,
    pickup_rush_hour_status string,
    dropoff_year int64,
    dropoff_weekday_num int64,
    dropoff_weekday_name string,
    dropoff_month int64,
    dropoff_hour int64,
    dropoff_public_holiday boolean,
    dropoff_rush_hour_status string,
    pickup_borough string,
    pickup_zone string,
    pickup_service_zone string,
    dropoff_borough string,
    dropoff_zone string,
    dropoff_service_zone string,
    ratecode_description string,
    store_and_fwd_flag string,
    payment_description string,
    passenger_count int64,
    trip_distance float64,
    fare_amount float64,
    extra_amount float64,
    mta_tax float64,
    tip_amount float64,
    tolls_amount float64,
    improvement_surcharge float64,
    total_amount float64,
    congestion_surcharge float64,
    airport_fee float64,
    clone_dt timestamp,
    transformation_dt timestamp
    
    )

      
    partition by timestamp_trunc(trip_type_start_date, month)
    cluster by data_source, pickup_date

    OPTIONS()
    as (
      
    select trip_type_start_date, data_source, trip_type_source, pickup_date, trip_id, vendor_id, pickup_datetime, dropoff_datetime, trip_type_end_date, trip_duration_min, pickup_year, pickup_weekday_num, pickup_weekday_name, pickup_month, pickup_hour, pickup_public_holiday, pickup_rush_hour_status, dropoff_year, dropoff_weekday_num, dropoff_weekday_name, dropoff_month, dropoff_hour, dropoff_public_holiday, dropoff_rush_hour_status, pickup_borough, pickup_zone, pickup_service_zone, dropoff_borough, dropoff_zone, dropoff_service_zone, ratecode_description, store_and_fwd_flag, payment_description, passenger_count, trip_distance, fare_amount, extra_amount, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, clone_dt, transformation_dt
    from (
        

select 
  -- cols that help better scan the data 
  trp.trip_type_start_date,
  trp.data_source,
  trp.trip_type_source,
  trp.pickup_date,
  -- IDs
  trp.trip_id,
  trp.vendor_id,
  -- time centric dimensions 
  trp.pickup_datetime,
  trp.dropoff_datetime,
  trp.trip_type_end_date,
  -- more time dimensions for later analysis 
  

    datetime_diff(
        cast(dropoff_datetime as datetime),
        cast(pickup_datetime as datetime),
        minute
    )

   trip_duration_min,
  extract(year from trp.pickup_datetime) pickup_year,
  extract(dayofweek from trp.pickup_datetime) pickup_weekday_num,
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
  extract(month from trp.pickup_datetime) pickup_month,
  extract(hour from trp.pickup_datetime) pickup_hour,
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
  extract(year from dropoff_datetime) dropoff_year,
  extract(dayofweek from trp.dropoff_datetime) dropoff_weekday_num,
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
  extract(month from trp.dropoff_datetime) dropoff_month,
  extract(hour from trp.dropoff_datetime) dropoff_hour,
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
  pz.borough pickup_borough,
  pz.zone pickup_zone,
  pz.service_zone pickup_service_zone,
  dz.borough dropoff_borough,
  dz.zone dropoff_zone,
  dz.service_zone dropoff_service_zone,
  -- trip categorization
  case safe_cast(ratecode_id as INT64)
        when 1 then 'STANDARD RATE'
        when 2 then 'JFK'
        when 3 then 'NEWARK'
        when 4 then 'NASSAU/WESTCHESTER'
        when 5 then 'NEGOTIATED FARE'
        when 6 then 'GROUP RIDE'
        when 99 then 'UNKNOWN'
    end ratecode_description,
  trp.store_and_fwd_flag,
  case safe_cast(ratecode_id as INT64)
        when 0 then 'FLEX FARE TRIP'
        when 1 then 'CREDIT CARD'
        when 2 then 'CASH'
        when 3 then 'NO CHARGE'
        when 4 then 'DISPUTE'
        when 5 then 'UNKNOWN'
        when 6 then 'VOIDED TRIP'
    end payment_description,
  -- for unit centric calculations
  trp.passenger_count,
  trp.trip_distance,
  -- revenue centric stats
  trp.fare_amount,
  trp.extra_amount,
  trp.mta_tax,
  trp.tip_amount,
  trp.tolls_amount,
  trp.improvement_surcharge,
  trp.total_amount,
  trp.congestion_surcharge,
  trp.airport_fee,
  -- data source centric info
  trp.clone_dt,
  current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_stage`.`stg_yellow__2_filter_out_faulty` trp 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` pz on trp.pickup_location_id = pz.location_id 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` dz on trp.dropoff_location_id = dz.location_id 




    ) as model_subq
    );
  