-- verify that qhistory table ok populated
select count(*)/count(distinct data_source)
from `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project`
;

select distinct data_source
from `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project`
;

-- calc size of data loaded from parquets
select 
  regexp_substr(table_id, 'yellow|green|fhvhv|fhv') data_type
  , sum(row_count) row_count
  , sum(size_bytes)/pow(10,9) size_gb
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.__TABLES__
where regexp_substr(table_id, 'external') is null 
group by regexp_substr(table_id, 'yellow|green|fhvhv|fhv')
;

-- get some initial stats on each query 
SELECT 
  regexp_substr(t1.data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}') label
  , t1.data_source
  , t1.start_time, t1.end_time
  , t1.dml_statistics.inserted_row_count
  , t1.total_bytes_processed/pow(10,9) tota_gb_processed
  , t1.total_bytes_billed/pow(10,9) total_gb_billed
  , t1.job_id, t1.query
FROM `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project` t1
-- join region-eu.INFORMATION_SCHEMA.JOBS t2 on t1.job_id = t2.job_id and t1.user_email = t2.user_email
where t1.statement_type in ('INSERT', 'CREATE_TABLE_AS_SELECT')
and regexp_substr(t1.query, 'nytaxi_monitoring') is null
and t1.total_bytes_processed is null
;

-- how long did it take to load every trip type from buckets 
select 
  regexp_substr(data_source, 'yellow|green|fhvhv|fhv') data_type
  , min(start_time) start_time_min
  , max(end_time) end_time_max
  , max(end_time) - min(start_time) duration
FROM `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project`
group by regexp_substr(data_source, 'yellow|green|fhvhv|fhv')
;

-- verify that all tables in stage are clustered
select * 
from pipeline-analysis-455005.nytaxi_raw_backup.INFORMATION_SCHEMA.TABLES
where regexp_substr(ddl, 'CLUSTER BY data_source') is not null
;

-- see volume processing size per iteration
SELECT 
  min(total_bytes_processed)/pow(10,9) min_gb_processed
  , max(total_bytes_processed)/pow(10,9) max_gb_processed
  , avg(total_bytes_processed)/pow(10,9) avg_gb_processed
FROM pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project
where statement_type in ('INSERT', 'CREATE_TABLE_AS_SELECT')
and regexp_substr(query, 'nytaxi_monitoring.query_history_extract_load_spark') is null
;

-- get some current (size) stats for tables in stage 
select 
  project_id, dataset_id, table_id
  , row_count
  , size_bytes/pow(10,9) size_gb
from pipeline-analysis-455005.nytaxi_raw_backup.__TABLES__
order by size_bytes desc
;

-- try to determine the variation in col_name/data_types
select 
  distinct regexp_substr(table_name, 'yellow|green|fhvhv|fhv') data_type
  , column_name, data_type, ordinal_position
from pipeline-analysis-455005.nytaxi_raw_backup.INFORMATION_SCHEMA.COLUMNS
order by 1, 2
;

-- column name and data type mapping to be applied to all tables 
with t1 as 
(select 
  regexp_substr(table_name, 'yellow|green|fhvhv|fhv') trip_type
  , table_name
  , column_name old_column_name
  , case
      when column_name in ('start_lon', 'pickup_longitude') then 'pickup_longitude'
      when column_name in ('start_lat', 'pickup_latitude') then 'pickup_latitude'
      when column_name in ('end_lon', 'dropoff_longitude') then 'dropoff_longitude'
      when column_name in ('end_lat', 'dropoff_latitude') then 'dropoff_latitude'
      when column_name = 'store_and_forward' then 'store_and_fwd_flag'
      when column_name = 'vendor_name' then 'vendor_id'
      when column_name = 'rate_code' then 'ratecode_id'
      when column_name = 'trip_miles' then 'trip_distance'
      when regexp_substr(column_name, 'num$') is not null then regexp_replace(column_name, 'num$', 'number')
      when column_name = 'dispatching_base_num' then 'dispatching_base_number'
      when column_name in ('extra', 'tips', 'tolls', 'driver_pay') then regexp_replace(column_name, 's$', '')||'_amount'
      when column_name = 'surcharge' then 'congestion_surcharge'
      when column_name = 'bcf' then 'black_card_fun_amount'
      when regexp_substr(column_name, 'do_') is not null then regexp_replace(column_name, 'do_', 'dropoff_')
      when regexp_substr(column_name, 'd_o') is not null then regexp_replace(column_name, 'd_o', 'dropoff_')
      when regexp_substr(column_name, 'drop_off') is not null then regexp_replace(column_name, 'drop_off', 'dropoff_')
      when regexp_substr(column_name, 'pu_') is not null then regexp_replace(column_name, 'pu_', 'pickup_')
      when regexp_substr(column_name, 'p_u') is not null then regexp_replace(column_name, 'p_u', 'pickup_')
      when regexp_substr(column_name, 'amt') is not null then regexp_replace(column_name, 'amt', 'amount')
      when regexp_substr(column_name, 'tpep|lpep') is not null then regexp_replace(column_name, '[tl]pep_', '')
      when regexp_substr(column_name, 'trip_') is not null 
        and regexp_substr(column_name, 'date') is not null 
        then regexp_replace(regexp_replace(column_name, 'trip_', ''), 'date_time$', 'datetime')
      else column_name end new_column_name
  , data_type old_data_type
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.INFORMATION_SCHEMA.COLUMNS
where column_name != '__index_level_0__'
and regexp_substr(table_name, 'external|mapping') is null
), 
t2 as 
(select 
  trip_type 
  , table_name
  , old_column_name 
  , new_column_name
  , old_data_type
  , case 
    when regexp_substr(new_column_name, 'datetime|date|creation_dt') is not null then 'TIMESTAMP'
    when regexp_substr(new_column_name, 'fee|amount|surcharge|tax|distance|longitude|latitude|fare') is not null then 'FLOAT64'
    when regexp_substr(new_column_name, 'data_source|flag|base_number|license_number') is not null then 'STRING'
    else 'INT64'
    end new_data_type
from t1
)
select 
  trip_type
  , table_name
  , old_column_name 
  , new_column_name 
  , old_data_type
  , new_data_type 
from t2
order by 2, 4
;

-- example queries to cleanup data 
select 
  case 
    when trp.vendor_name = 'CMT' then 1
    when trp.vendor_name = 'VTS' then 3
    when trp.vendor_name = 'DDS' then 4
    end vendor_id
  , trp.trip_pickup_date_time pickup_datetime
  , trp.trip_dropoff_date_time dropoff_date_time
  , trp.passenger_count
  , trp.trip_distance
  , pu.zone_id pu_location_id
  , trp.rate_code rate_code_id
  , case 
      when coalesce(trp.store_and_forward, 0) = 0 then 'N'
      when trp.store_and_forward = 1 then 'Y'
      end store_and_fwd_flag
  , du.zone_id do_location_id
  , case 
      when lower(trp.payment_type) = 'cash' then 2
      when lower(trp.payment_type) = 'dispute' then 4
      when lower(trp.payment_type) = 'no charge' then 3
      when lower(trp.payment_type) = 'credit' then 1
      else null 
      end payment_type
  , trp.fare_amt fare_amount
  , trp.surcharge
  , trp.mta_tax
  , trp.tip_amt tip_amount
  , trp.tolls_amt tolls_amount
  , trp.total_amt total_amount
  , trp.trip_pickup_date pickup_date
  , regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$') trip_type
  , parse_datetime('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01') data_start_date
  , last_day(parse_date('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) data_end_date
  , trp.data_source
  , trp.creation_dt
from `pipeline-analysis-452722.nytaxi_stage.yellow_tripdata_2009-01` trp
join `pipeline-analysis-452722.mapping.taxi_zone_geom` pu on (ST_DWithin(pu.zone_geom,ST_GeogPoint(trp.start_lon, trp.start_lat), 0))
join `pipeline-analysis-452722.mapping.taxi_zone_geom` du on (ST_DWithin(du.zone_geom,ST_GeogPoint(trp.end_lon, trp.end_lat), 0))
where trp.data_source = 'gs://yellow-taxi-data-extract-load/2025-03-11_10:46:29_yellow_tripdata_2009-01'
and trp.start_lon between -90 and 90 
and trp.start_lat between -90 and 90
and trp.end_lon between -90 and 90 
and trp.end_lat between -90 and 90
limit 10
;

-- determine where there are conflicting data types amoungst all the yellow_tripdata tables 
select new_column_name, count(distinct data_type)
from `pipeline-analysis-452722.nytaxi_stage.vw_column_name_mapping`
where regexp_substr(table_name, 'vw_') is null  
and regexp_substr(table_name, '2009|2010') is not null 
group by new_column_name
order by 2 desc
;

-- to see the different variations of the field for later data cleanup 
select distinct vendor_id, ratecode_id, store_and_fwd_flag, payment_type
from `pipeline-analysis-452722.nytaxi_stage.vw_yellow_tripdata_2009_2010`
order by 1, 2, 3, 4
;

-- column cleamup notes 

ratecode_id cast as integer

case 
    when vendor_id = '1' then 1::int64
    when vendor_id = 'CMT' then 1::int64
    when vendor_id = 'VTS' then 3::int64
    when vendor_id = 'DDS' then 4::int64
    else null 
    end vendor_id

case 
    when trim(store_and_fwd_flag) in ('0', '0.0', 'N') then 'N'
    when trim(store_and_fwd_flag) in ('1', '1.0', 'Y') then 'Y'
    else null 
    end store_an_fwd_flag

case 
    when trim(lower(payment_type)) in ('cash', 'csh', 'cas') then 2::int64
    when trim(lower(payment_type)) in ('credit', 'cre', 'crd') then 1::int64
    when trim(lower(payment_type)) in ('dispute', 'dis') then 4::int64
    when trim(lower(payment_type)) in ('no', 'no charge', 'noc') then 3::int64
    when trim(lower(payment_type)) in ('na') then 5::int64
    else null 
    end payment_type

-- try to pinpoint some faulty lines 
select count(1)
from `pipeline-analysis-452722.nytaxi_stage.vw_yellow_tripdata_2009_2010`
-- where pickup_datetime >= dropoff_datetime
-- where pickup_datetime = dropoff_datetime --and trip_distance = 0
-- where vendor_id is null 
;

-- examine data uniqueness
select 
  vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id
  , ratecode_id, payment_type
  , count(1) row_count
from `pipeline-analysis-452722.nytaxi_stage.vw_yellow_tripdata_2009_2010`
where pickup_datetime < dropoff_datetime
group by vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id
        , ratecode_id, payment_type
having count(1) > 1
order by 8 desc
;

-- looking candidate records that are prob inccorect 
select count(1)
from `pipeline-analysis-452722.nytaxi_clean.yellow__4_final_clean` 
where (
      -- invalid trip timestamps
      (pickup_datetime >= dropoff_datetime)
      or 
      -- trip timestamps not inline with source parquet
      (pickup_datetime < cast(trip_type_start_date as timestamp))
      or 
      -- trips with unknown pickup or dropoff location
      (pickup_location_id = 264 or dropoff_location_id = 264)
      or 
      -- trip with airport fee but location not at airport 
      (coalesce(airport_fee, 0) > 0 and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '138|132') is null)
      or 
      -- fare amount negative but payment_type not void, dispute or no charge
      (fare_amount <= 0 and payment_type not in (3, 4, 6) and trip_type_start_date >= '2022-05-01')
      or 
      -- all trips must report some distance to be valid
      (trip_distance < 0)
      or 
      -- trips where charges dont add up
      (abs(total_amount) - abs(fare_amount+extra_amount+mta_tax+tip_amount+tolls_amount+improvement_surcharge+congestion_surcharge+airport_fee) > 1)
      or 
      -- trips with non-JFK destination but got the rate charge
      (coalesce(ratecode_id, 0) in (2, 0) and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '132') is null and trip_type_start_date >= '2022-05-01')
      or  
      -- trips with non-Newark destination but got the rate charge
      (coalesce(ratecode_id, 0) in (3, 0) and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '^1 - | - 1$') is null and trip_type_start_date >= '2022-05-01')
      or 
      -- trips tip with outer city rate but locations not outside the city
      (coalesce(ratecode_id, 0) in (4, 0) and regexp_substr(pickup_location_id||' - '||dropoff_location_id, '265') is null and trip_type_start_date >= '2022-05-01')
      or 
      -- rows that dont have valid passenger count (either 0 or over the legal limit for a single ride)
      (passenger_count > 6 or passenger_count <= 0)
      -- all trips with a positive trip amount should have a cc ratecode ID
      or  
      (tip_amount > 0 and payment_type != 1 and trip_type_start_date >= '2022-05-01')
)
;

-- look at records that cancel each other out 
create or replace view `pipeline-analysis-452722.nytaxi_clean.yellow__4_duplicate_rows` as 
select 
  pickup_datetime, dropoff_datetime, ratecode_id, pickup_location_id, dropoff_location_id, passenger_count, trip_distance
  , count(1) row_count
  , sum(fare_amount) total_fare_amount
from `pipeline-analysis-452722.nytaxi_clean.yellow__4_final_clean` 
group by pickup_datetime, dropoff_datetime, ratecode_id, pickup_location_id, dropoff_location_id, passenger_count, trip_distance
;

select count(1)
from `pipeline-analysis-452722.nytaxi_clean.yellow__4_final_clean` t
join (select 
  pickup_datetime, dropoff_datetime, ratecode_id, pickup_location_id, dropoff_location_id, passenger_count, trip_distance
from `pipeline-analysis-452722.nytaxi_clean.yellow__4_duplicate_rows`
where row_count = 2
and total_fare_amount = 0) t1 on t.pickup_datetime = t1.pickup_datetime
and t.dropoff_datetime = t1.dropoff_datetime
and coalesce(t.ratecode_id, 0) = coalesce(t1.ratecode_id, 0)
and t.pickup_location_id = t1.pickup_location_id 
and t.dropoff_location_id = t1.dropoff_location_id 
and t.passenger_count = t1.passenger_count
and t.trip_distance = t1.trip_distance 
where t.fare_amount > 0 -- 3,411,650 -- 1,697,427
-- order by t.pickup_datetime, t.dropoff_datetime, t.ratecode_id, t.pickup_location_id, t.dropoff_location_id, t.passenger_count, t.trip_distance
;

-- comparison of raw data vs stage clean data size 
select 
  sum(row_count)
  , sum(size_bytes)/pow(10,9) size_gb
from pipeline-analysis-452722.nytaxi_stage.__TABLES__
;
-- 1,778,238,040 (411 GB)

select 
  sum(row_count)
  , sum(size_bytes)/pow(10,9) size_gb
from pipeline-analysis-452722.nytaxi_stage2.__TABLES__
where table_id = 'stg_yellow__3_from_source_clean'
;
 -- 1,648,566,991 (656 GB)