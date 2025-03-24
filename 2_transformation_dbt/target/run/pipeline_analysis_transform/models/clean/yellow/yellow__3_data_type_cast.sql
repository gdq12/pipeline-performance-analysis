

  create or replace view `pipeline-analysis-452722`.`nytaxi`.`yellow__3_data_type_cast`
  OPTIONS()
  as with trps as 
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
    , cast(null as float64) extra_amount
    ,cast(mta_tax as float64) mta_tax
    ,cast(tip_amount as float64) tip_amount
    ,cast(tolls_amount as float64) tolls_amount
    ,cast(null as float64) improvement_surcharge
    ,cast(total_amount as float64) total_amount
    ,cast(congestion_surcharge as float64) congestion_surcharge
    , cast(null as float64) airport_fee
    ,cast(pickup_date as timestamp) pickup_date
    ,safe_cast(data_source as string) data_source
    ,cast(creation_dt as timestamp) creation_dt
from `pipeline-analysis-452722`.`nytaxi`.`yellow__2b_2009_2010_location_id_update`
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
    ,cast(extra_amount as float64) extra_amount
    ,cast(mta_tax as float64) mta_tax
    ,cast(tip_amount as float64) tip_amount
    ,cast(tolls_amount as float64) tolls_amount
    ,cast(improvement_surcharge as float64) improvement_surcharge
    ,cast(total_amount as float64) total_amount
    ,cast(congestion_surcharge as float64) congestion_surcharge
    ,cast(airport_fee as float64) airport_fee
    ,cast(pickup_date as timestamp) pickup_date
    ,safe_cast(data_source as string) data_source
    ,cast(creation_dt as timestamp) creation_dt
from `pipeline-analysis-452722`.`nytaxi`.`yellow__2_post_2010_tbl_collation`
)
select 
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_location_id,
  ratecode_id,
  store_and_fwd_flag,
  dropoff_location_id,
  payment_type,
  fare_amount,
  extra_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  pickup_date,
  data_source,
  creation_dt
from trps;

