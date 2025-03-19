

with t1 as
(select 
    case safe_cast(vendor_id as string)
        when 'CMT' then 1
        when '1' then 1
        when '2' then 2
        when 'VTS' then 3
        when 'DDS' then 4
        else null
    end vendor_id
    ,cast(pickup_datetime as timestamp) pickup_datetime 
    ,cast(dropoff_datetime as timestamp) dropoff_datetime
    ,safe_cast(passenger_count as INT64) passenger_count
    ,cast(trip_distance as float64) trip_distance
    ,safe_cast(pickup_location_id as INT64) pickup_location_id
    ,safe_cast(ratecode_id as string) ratecode_id
    ,case 
        when trim(safe_cast(store_and_fwd_flag as string))
            in ('0', '0.0', 'N') then 'N'
        when trim(safe_cast(store_and_fwd_flag as string))
            in ('1', '1.0', 'Y') then 'Y'
        else null 
    end  store_and_fwd_flag
    ,safe_cast(dropoff_location_id as INT64) dropoff_location_id
    ,case 
        when lower(trim(safe_cast(payment_type as string))) 
            in ('cash', 'csh', 'cas') then 2
        when lower(trim(safe_cast(payment_type as string))) 
            in ('credit', 'cre', 'crd') then 1
        when lower(trim(safe_cast(payment_type as string))) 
            in ('dispute', 'dis') then 4
        when lower(trim(safe_cast(payment_type as string)))
            in ('no', 'no charge', 'noc') then 3
        when lower(trim(safe_cast(payment_type as string)))
            in ('na') then 5
        else null 
    end payment_type
    ,cast(fare_amount as float64) fare_amount
    ,cast(mta_tax as float64) mta_tax
    ,cast(tip_amount as float64) tip_amount
    ,cast(tolls_amount as float64) tolls_amount
    ,cast(null as float64) improvement_surcharge
    ,cast(total_amount as float64) total_amount
    ,cast(congestion_surcharge as float64) congestion_surcharge
    ,cast(pickup_date as timestamp) pickup_date
    ,safe_cast(trip_type as string) trip_type
    ,cast(data_start_date as timestamp) data_start_date
    ,cast(data_end_date as timestamp) data_end_date
    ,safe_cast(data_source as string) data_source
    ,cast(creation_dt as timestamp) creation_dt
from `pipeline-analysis-452722`.`nytaxi_stage`.`vw_yellow_tripdata_2009_2010`
where pickup_datetime < dropoff_datetime
union all 
select 
    case safe_cast(vendor_id as string)
        when 'CMT' then 1
        when '1' then 1
        when '2' then 2
        when 'VTS' then 3
        when 'DDS' then 4
        else null
    end vendor_id
    ,cast(pickup_datetime as timestamp) pickup_datetime 
    ,cast(dropoff_datetime as timestamp) dropoff_datetime
    ,safe_cast(passenger_count as INT64) passenger_count
    ,cast(trip_distance as float64) trip_distance
    ,safe_cast(pickup_location_id as INT64) pickup_location_id
    ,safe_cast(ratecode_id as string) ratecode_id
    ,case 
        when trim(safe_cast(store_and_fwd_flag as string))
            in ('0', '0.0', 'N') then 'N'
        when trim(safe_cast(store_and_fwd_flag as string))
            in ('1', '1.0', 'Y') then 'Y'
        else null 
    end  store_and_fwd_flag
    ,safe_cast(dropoff_location_id as INT64) dropoff_location_id
    ,case 
        when lower(trim(safe_cast(payment_type as string))) 
            in ('cash', 'csh', 'cas') then 2
        when lower(trim(safe_cast(payment_type as string))) 
            in ('credit', 'cre', 'crd') then 1
        when lower(trim(safe_cast(payment_type as string))) 
            in ('dispute', 'dis') then 4
        when lower(trim(safe_cast(payment_type as string)))
            in ('no', 'no charge', 'noc') then 3
        when lower(trim(safe_cast(payment_type as string)))
            in ('na') then 5
        else null 
    end payment_type
    ,cast(fare_amount as float64) fare_amount
    ,cast(mta_tax as float64) mta_tax
    ,cast(tip_amount as float64) tip_amount
    ,cast(tolls_amount as float64) tolls_amount
    ,cast(improvement_surcharge as float64) improvement_surcharge
    ,cast(total_amount as float64) total_amount
    ,cast(congestion_surcharge as float64) congestion_surcharge
    ,cast(pickup_date as timestamp) pickup_date
    ,safe_cast(trip_type as string) trip_type
    ,cast(data_start_date as timestamp) data_start_date
    ,cast(data_end_date as timestamp) data_end_date
    ,safe_cast(data_source as string) data_source
    ,cast(creation_dt as timestamp) creation_dt
from `pipeline-analysis-452722`.`nytaxi_stage`.`vw_yellow_tripdata`
where pickup_datetime < dropoff_datetime
)
select 
    to_hex(md5(cast(coalesce(cast(vendor_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime  as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(passenger_count as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(trip_distance as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ratecode_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(store_and_fwd_flag as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(payment_type as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fare_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(mta_tax as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tip_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tolls_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(improvement_surcharge as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(total_amount as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(congestion_surcharge as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(data_source as string), '_dbt_utils_surrogate_key_null_') as string))) trip_id
    ,vendor_id
    ,pickup_datetime 
    ,dropoff_datetime
    ,passenger_count
    ,trip_distance
    ,pickup_location_id
    ,ratecode_id
    ,store_and_fwd_flag
    ,dropoff_location_id
    ,payment_type
    ,fare_amount
    ,mta_tax
    ,tip_amount
    ,tolls_amount
    ,improvement_surcharge
    ,total_amount
    ,congestion_surcharge
    ,pickup_date
    ,trip_type
    ,data_start_date
    ,data_end_date
    ,data_source
    ,creation_dt
from t1 



  limit 100 

