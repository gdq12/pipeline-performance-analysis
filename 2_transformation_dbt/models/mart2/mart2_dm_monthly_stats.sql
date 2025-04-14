{{ config(
    unique_key=['pickup_month', 'trip_type']
)}}

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
    dm.pickup_year,
    dm.pickup_month,
    dm.pickup_rush_hour_status, 
    dm.pickup_weekday_name,
    -- other info 
    {{ get_ratecode_description("ratecode_id") }} applied_rate,
    {{ get_payment_description("payment_type") }} payment_type,
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
    dm.num_public_holidays,
    dm.num_trips,
    dm.transformation_dt
from {{ ref('core2_dm_monthly_stats') }} dm 
join {{ source('mapping.map', 'taxi_zone_lookup') }} pz on dm.pickup_location_id = pz.location_id 
join {{ source('mapping.map', 'taxi_zone_lookup') }} dz on dm.dropoff_location_id = dz.location_id 
left join {{ source('mapping.map', 'hvlv_base_numbers') }} mp on coalesce(dm.hvfhs_license_number, 'N/A') = coalesce(mp.hvln, 'N/A')

{% if is_incremental() %}

where pickup_month not in (select distinct pickup_month from {{ this }})

{% endif %}