

select 
  -- cols that help better scan the data 
  trip_type_start_date,
  data_source,
  pickup_date,
  -- IDs
  trip_id,
  vendor_id,
  -- time centric dimensions 
  pickup_datetime,
  dropoff_datetime,
  trip_type_end_date,
  -- more time dimensions for later analysis 
  

    datetime_diff(
        cast(dropoff_datetime as datetime),
        cast(pickup_datetime as datetime),
        minute
    )

   trip_duration_min,
  extract(year from pickup_datetime) pickup_year,
  extract(dayofweek from pickup_datetime) pickup_weekday_num,
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
  extract(week from pickup_datetime) pickup_calender_week_num,
  extract(month from pickup_datetime) pickup_month,
  extract(hour from pickup_datetime) pickup_hour,
  bigfunctions.eu.is_public_holiday(cast(pickup_datetime as date), 'US') pickup_public_holiday,
  extract(year from dropoff_datetime) dropoff_year,
  extract(dayofweek from dropoff_datetime) dropoff_weekday_num,
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
  extract(week from dropoff_datetime) dropoff_calender_week_num,
  extract(month from dropoff_datetime) dropoff_month,
  extract(hour from dropoff_datetime) dropoff_hour,
  bigfunctions.eu.is_public_holiday(cast(dropoff_datetime as date), 'US') dropoff_public_holiday,
  -- location centric info 
  pickup_location_id,
  dropoff_location_id,
  -- trip categorization
  ratecode_id,
  store_and_fwd_flag,
  payment_type,
  -- for unit centric calculations
  passenger_count,
  trip_distance,
  -- revenue centric stats
  fare_amount,
  extra_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  -- data source centric info
  creation_dt,
  current_timestamp() transformation_dt
from `pipeline-analysis-452722`.`nytaxi_stage2`.`stg_yellow__2_filter_out_faulty`