select 
    safe_cast(dispatching_base_number as string) dispatching_base_number,
    cast(pickup_datetime as timestamp) pickup_datetime, 
    cast(dropoff_datetime as timestamp) dropoff_datetime,
    safe_cast(pickup_location_id as INT64) pickup_location_id,
    safe_cast(dropoff_location_id as INT64)dropoff_location_id,
    case safe_cast(sr_flag as string)
        when '1.0' then 1
        when '1' then 1
        else null
    end sr_flag, 
    safe_cast(affiliated_base_number as string) affiliated_base_number,
    cast(pickup_date as timestamp) pickup_date,
    safe_cast(data_source as string) data_source, 
    cast(creation_dt as timestamp) creation_dt, 
    cast(clone_dt as timestamp) clone_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__2_tbl_collation`