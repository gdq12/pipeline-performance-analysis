select 
    {{ dbt.safe_cast("dispatching_base_number", api.Column.translate_type("string")) }} dispatching_base_number,
    cast(pickup_datetime as timestamp) pickup_datetime, 
    cast(dropoff_datetime as timestamp) dropoff_datetime,
    {{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} pickup_location_id,
    {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }}dropoff_location_id,
    {{ update_sr_flag("sr_flag") }} sr_flag, 
    {{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("string")) }} affiliated_base_number,
    cast(pickup_date as datetime) pickup_date,
    {{ dbt.safe_cast("data_source", api.Column.translate_type("string")) }} data_source, 
    cast(creation_dt as datetime) creation_dt, 
    cast(clone_dt as timestamp) clone_dt
from {{ ref('fhv__2_tbl_collation') }}