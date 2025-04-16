
  
    

    create or replace table `pipeline-analysis-455005`.`nytaxi_mart2`.`mart2_dm_daily_stats`
        
  (
    pickup_borough string,
    pickup_zone string,
    pickup_service_zone string,
    dropoff_borough string,
    dropoff_zone string,
    dropoff_service_zone string,
    pickup_date timestamp,
    pickup_public_holiday boolean,
    pickup_year int64,
    pickup_month int64,
    pickup_hour int64,
    pickup_rush_hour_status string,
    pickup_weekday_name string,
    applied_rate string,
    payment_type string,
    trip_type string,
    hvfs_license_name string,
    avg_trip_distance float64,
    avg_trip_duration_min float64,
    fare_amount float64,
    tip_amount float64,
    total_amount float64,
    total_fees float64,
    passenger_count int64,
    num_trips int64,
    transformation_dt timestamp
    
    )

      
    partition by timestamp_trunc(pickup_date, day)
    

    OPTIONS()
    as (
      
    select pickup_borough, pickup_zone, pickup_service_zone, dropoff_borough, dropoff_zone, dropoff_service_zone, pickup_date, pickup_public_holiday, pickup_year, pickup_month, pickup_hour, pickup_rush_hour_status, pickup_weekday_name, applied_rate, payment_type, trip_type, hvfs_license_name, avg_trip_distance, avg_trip_duration_min, fare_amount, tip_amount, total_amount, total_fees, passenger_count, num_trips, transformation_dt
    from (
        

select 
    -- zone info
    pz.borough pickup_borough,
    pz.zone pickup_zone,
    pz.service_zone pickup_service_zone,
    dz.borough dropoff_borough,
    dz.zone dropoff_zone,
    dz.service_zone dropoff_service_zone,
    -- time info
    dm.pickup_date,
    dm.pickup_public_holiday, 
    dm.pickup_year,
    dm.pickup_month,
    dm.pickup_hour,
    dm.pickup_rush_hour_status, 
    dm.pickup_weekday_name,
    -- other info 
    case safe_cast(ratecode_id as INT64)
        when 1 then 'STANDARD RATE'
        when 2 then 'JFK'
        when 3 then 'NEWARK'
        when 4 then 'NASSAU/WESTCHESTER'
        when 5 then 'NEGOTIATED FARE'
        when 6 then 'GROUP RIDE'
        when 99 then 'UNKNOWN'
    end applied_rate,
    case safe_cast(ratecode_id as INT64)
        when 0 then 'FLEX FARE TRIP'
        when 1 then 'CREDIT CARD'
        when 2 then 'CASH'
        when 3 then 'NO CHARGE'
        when 4 then 'DISPUTE'
        when 5 then 'UNKNOWN'
        when 6 then 'VOIDED TRIP'
    end payment_type,
    dm.trip_type,
    coalesce(mp.app_company_affiliation, 'N/A') hvfs_license_name,
    -- aggregations
    dm.avg_trip_distance,
    dm.avg_trip_duration_min,
    dm.fare_amount, 
    dm.tip_amount,
    dm.total_amount,
    dm.total_fees,
    dm.passenger_count,
    dm.num_trips,
    current_timestamp() transformation_dt
from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_dm_daily_stats` dm 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` pz on dm.pickup_location_id = pz.location_id 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` dz on dm.dropoff_location_id = dz.location_id 
left join `pipeline-analysis-455005`.`nytaxi_mapping`.`hvlv_base_numbers` mp on coalesce(dm.hvfhs_license_number, 'N/A') = coalesce(mp.hvln, 'N/A')


    ) as model_subq
    );
  