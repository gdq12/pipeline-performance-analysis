

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
  extract(week from trp.pickup_datetime) pickup_calender_week_num,
  extract(month from trp.pickup_datetime) pickup_month,
  extract(hour from trp.pickup_datetime) pickup_hour,
  bigfunctions.eu.is_public_holiday(cast(trp.pickup_datetime as date), 'US') pickup_public_holiday,
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
  extract(week from trp.dropoff_datetime) dropoff_calender_week_num,
  extract(month from trp.dropoff_datetime) dropoff_month,
  extract(hour from trp.dropoff_datetime) dropoff_hour,
  bigfunctions.eu.is_public_holiday(cast(trp.dropoff_datetime as date), 'US') dropoff_public_holiday,
  -- location centric info 
  pz.borough pickup_borough,
  pz.zone pickup_zone,
  pz.service_zone pickup_service_zone,
  dz.borough dropoffp_borough,
  dz.zone dropoff_zone,
  dz.service_zone dropoff_service_zone,
  -- trip categorization
  trp.ratecode_id,
  trp.store_and_fwd_flag,
  trp.payment_type,
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
  trp.creation_dt,
  current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_stage`.`stg_yellow__2_filter_out_faulty` trp 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` pz on trp.pickup_location_id = pz.location_id 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` dz on trp.dropoff_location_id = pz.location_id 



  limit 100 

