

  create or replace view `pipeline-analysis-455005`.`nytaxi_clean`.`green__3_data_type`
  OPTIONS()
  as select 
    case safe_cast(vendor_id as string)
        when '1' then 1
        when '2' then 2
        when '3' then 3
        when '4' then 4
        when '5' then 5
        when '6' then 6
        when '7' then 7
        when 'CMT' then 1
        when 'VTS' then 8
        when 'DDS' then 9
        else null
    end vendor_id,
    cast(pickup_datetime as timestamp) pickup_datetime,
    cast(dropoff_datetime as timestamp) dropoff_datetime,
    case 
        when trim(safe_cast(store_and_fwd_flag as string))
            in ('0', '0.0', 'N') then 'N'
        when trim(safe_cast(store_and_fwd_flag as string))
            in ('1', '1.0', 'Y') then 'Y'
        else null 
    end  store_and_fwd_flag,
    case 
        when (safe_cast(ratecode_id as string) is null 
            or safe_cast(ratecode_id as string) in ('99', '99.0')
            )
            and (safe_cast(pickup_location_id as INT64) = 132
            or safe_cast(dropoff_location_id as INT64) = 132)
            then 2
        when (safe_cast(ratecode_id as string) is null 
            or safe_cast(ratecode_id as string) in ('99', '99.0')
            )
            and (safe_cast(pickup_location_id as INT64) = 1
            or safe_cast(dropoff_location_id as INT64) = 1)
            then 3
        when (safe_cast(ratecode_id as string) is null 
            or safe_cast(ratecode_id as string) in ('99', '99.0')
            )
            and (safe_cast(pickup_location_id as INT64) = 265
            or safe_cast(dropoff_location_id as INT64) = 265)
            then 4
        when safe_cast(ratecode_id as string) in ('1', '1.0') then 1
        when safe_cast(ratecode_id as string) in ('2', '2.0') then 2
        when safe_cast(ratecode_id as string) in ('3', '3.0') then 3
        when safe_cast(ratecode_id as string) in ('4', '4.0') then 4
        when safe_cast(ratecode_id as string) in ('5', '5.0') then 5
        when safe_cast(ratecode_id as string) in ('6', '6.0') then 6
        when (safe_cast(ratecode_id as string) is null 
            or safe_cast(ratecode_id as string) in ('99', '99.0')
            ) then 99
    end ratecode_id,
    safe_cast(pickup_location_id as INT64) pickup_location_id,
    safe_cast(dropoff_location_id as INT64) dropoff_location_id,
    safe_cast(passenger_count as INT64) passenger_count,
    cast(trip_distance as float64) trip_distance,
    cast(fare_amount as float64) fare_amount,
    cast(null as float64) extra_amount,
    cast(mta_tax as float64) mta_tax,
    cast(tip_amount as float64) tip_amount,
    cast(tolls_amount as float64) tolls_amount,
    cast(ehail_fee as float64) ehail_fee,
    cast(null as float64) improvement_surcharge,
    cast(total_amount as float64) total_amount,
    case 
        when 
            lower(trim(safe_cast(payment_type as string))) is null 
            and cast(tip_amount as float64) > 0 then 1
        when lower(trim(safe_cast(payment_type as string))) 
            in ('cash', 'csh', 'cas', '2', '2.0') then 2
        when lower(trim(safe_cast(payment_type as string))) 
            in ('credit', 'cre', 'crd', '1', '1.0') then 1
        when lower(trim(safe_cast(payment_type as string))) 
            in ('dispute', 'dis', '4', '4.0') then 4
        when lower(trim(safe_cast(payment_type as string)))
            in ('no', 'no charge', 'noc', '3', '3.0') then 3
        when lower(trim(safe_cast(payment_type as string)))
            in ('na', '0', '5', '5.0') then 5
        when lower(trim(safe_cast(payment_type as string)))
            in ('void', 'voi', '6', '6.0') then 6
        else 5
    end payment_type,
    safe_cast(trip_type as INT64) trip_type,
    cast(congestion_surcharge as float64) congestion_surcharge,
    cast(pickup_date as timestamp) pickup_date,
    safe_cast(data_source as string) data_source,
    cast(creation_dt as timestamp) creation_dt,
    cast(clone_dt as timestamp) clone_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`green__2_tbl_collation`;

