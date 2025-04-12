select 
    data_source, 
    trip_id
from {{ ref('yellow__4_adds_columns') }}
where (
      -- invalid trip timestamps
      (pickup_datetime >= dropoff_datetime)
      or 
      -- trip timestamps not inline with source parquet
      (pickup_datetime < cast(trip_type_start_date as timestamp))
      or 
      -- trips with unknown pickup or dropoff location
      (coalesce(pickup_location_id, 264) = 264 or coalesce(dropoff_location_id, 264) = 264)
      or 
      -- trip with airport fee but location not at airport 
      (coalesce(airport_fee, 0) > 0 and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '138|132') is null)
      or 
      -- fare amount negative but payment_type not void, dispute or no charge
      (fare_amount <= 0 and payment_type not in (3, 4, 6))
      or 
      -- all trips must report some distance to be valid
      (trip_distance < 0)
      or 
      -- trips where charges dont add up
      (abs(total_amount) - abs(fare_amount+extra_amount+mta_tax+tip_amount+tolls_amount+improvement_surcharge+congestion_surcharge+airport_fee) > 1)
      or 
      -- trips with non-JFK destination but got the rate charge
      (coalesce(ratecode_id, 0) in (2, 0) and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '132') is null)
      or  
      -- trips with non-Newark destination but got the rate charge
      (coalesce(ratecode_id, 0) in (3, 0) and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '^1 - | - 1$') is null)
      or 
      -- trips tip with outer city rate but locations not outside the city
      (coalesce(ratecode_id, 0) in (4, 0) and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '265') is null)
      or 
      -- rows that dont have valid passenger count (either 0 or over the legal limit for a single ride)
      (passenger_count > 6 or passenger_count <= 0)
      or
      -- all trips with a positive trip amount should have a cc ratecode ID 
      (tip_amount > 0 and payment_type != 1)
)