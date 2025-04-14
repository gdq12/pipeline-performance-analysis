

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
    dm.transformation_dt
from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_dm_daily_stats` dm 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` pz on dm.pickup_location_id = pz.location_id 
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup` dz on dm.dropoff_location_id = dz.location_id 
left join `pipeline-analysis-455005`.`nytaxi_mapping`.`hvlv_base_numbers` mp on coalesce(dm.hvfhs_license_number, 'N/A') = coalesce(mp.hvln, 'N/A')

