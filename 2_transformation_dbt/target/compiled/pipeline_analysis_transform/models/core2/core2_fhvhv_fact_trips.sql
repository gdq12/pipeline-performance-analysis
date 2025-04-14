

select 
    -- cols that help better scan the data 
    trp.trip_type_start_date,
    trp.data_source,
    trp.trip_type_source,
    trp.pickup_date,
    trp.trip_type_end_date,
    -- IDs
    trp.trip_id,
    trp.hvfhs_license_number, 
    trp.dispatching_base_number, 
    trp.originating_base_number,
    -- time centric dimensions
    trp.request_datetime,
    trp.on_scene_datetime,
    trp.trip_time,
    

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
    -- trip metrics
    trp.trip_distance, 
    -- trip categorization
    trp.shared_request_flag,
    trp.shared_match_flag,
    trp.access_a_ride_flag,
    trp.wav_request_flag, 
    trp.wav_match_flag, 
    -- revenue centric stats
    trp.base_passenger_fare, 
    trp.toll_amount,
    trp.black_card_fund_amount,
    trp.sales_tax,
    trp.congestion_surcharge,
    trp.airport_fee,
    trp.tip_amount,
    trp.driver_pay_amount,
    -- data source centric info 
    trp.clone_dt,
    current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhvhv__2_filter_out_faulty` trp 



