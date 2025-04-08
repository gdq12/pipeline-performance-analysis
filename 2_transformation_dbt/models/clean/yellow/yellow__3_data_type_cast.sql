with trps as 
(select 
    {{ update_vendor_id("vendor_id") }} vendor_id
    ,cast(pickup_datetime as timestamp) pickup_datetime 
    ,cast(dropoff_datetime as timestamp) dropoff_datetime
    ,{{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} passenger_count
    ,cast(trip_distance as float64) trip_distance
    ,{{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} pickup_location_id
    ,{{ update_ratecode_id("ratecode_id", "pickup_location_id", "dropoff_location_id") }} ratecode_id
    ,{{ update_store_and_fwd_flag("store_and_fwd_flag") }}  store_and_fwd_flag
    ,{{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} dropoff_location_id
    ,{{ update_payment_type("payment_type", "tip_amount") }} payment_type
    ,cast(fare_amount as float64) fare_amount
    ,cast(null as float64) extra_amount
    ,cast(mta_tax as float64) mta_tax
    ,cast(tip_amount as float64) tip_amount
    ,cast(tolls_amount as float64) tolls_amount
    ,cast(null as float64) improvement_surcharge
    ,cast(total_amount as float64) total_amount
    ,cast(congestion_surcharge as float64) congestion_surcharge
    ,cast(null as float64) airport_fee
    ,cast(pickup_date as timestamp) pickup_date
    ,{{ dbt.safe_cast("data_source", api.Column.translate_type("string")) }} data_source
    ,cast(creation_dt as timestamp) creation_dt
    ,cast(clone_dt as timestamp) clone_dt
from {{ ref('yellow__2b_2009_2010_location_id_update') }}
union all 
select 
    {{ update_vendor_id("vendor_id") }} vendor_id
    ,cast(pickup_datetime as timestamp) pickup_datetime 
    ,cast(dropoff_datetime as timestamp) dropoff_datetime
    ,{{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} passenger_count
    ,cast(trip_distance as float64) trip_distance
    ,{{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} pickup_location_id
    ,{{ update_ratecode_id("ratecode_id", "pickup_location_id", "dropoff_location_id") }} ratecode_id
    ,{{ update_store_and_fwd_flag("store_and_fwd_flag") }}  store_and_fwd_flag
    ,{{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} dropoff_location_id
    ,{{ update_payment_type("payment_type", "tip_amount") }} payment_type
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
    ,{{ dbt.safe_cast("data_source", api.Column.translate_type("string")) }} data_source
    ,cast(creation_dt as timestamp) creation_dt
    ,cast(clone_dt as timestamp) clone_dt
from {{ ref('yellow__2_post_2010_tbl_collation') }}
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
  creation_dt,
  clone_dt
from trps 