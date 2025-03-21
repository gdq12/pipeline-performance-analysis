-- are there any failed jobs
SELECT  *
FROM `pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark`
where error_result.message is not null
;

-- get some initial stats on each query 
SELECT 
  regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}') label
  , data_source
  , start_time, end_time
  , dml_statistics.inserted_row_count
  , total_bytes_processed/pow(10,9) tota_gb_processed
  , total_bytes_billed/pow(10,9) total_gb_billed
  , job_id, query
FROM pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark
where statement_type in ('INSERT', 'CREATE_TABLE_AS_SELECT')
and regexp_substr(query, 'insert into `pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark`') is null
;

-- get some current (size) stats for tables in stage 
select 
  project_id, dataset_id, table_id
  , row_count
  , size_bytes/pow(10,9) size_gb
from pipeline-analysis-452722.nytaxi_stage.__TABLES__
order by size_bytes desc
;

-- how much volume in total was transfered to bigquery
select 
  sum(row_count) row_count
  , sum(size_bytes)/pow(10,9) size_gb
from pipeline-analysis-452722.nytaxi_stage.__TABLES__
;

-- see volume processing size per iteration
SELECT 
  min(total_bytes_processed)/pow(10,9) min_gb_processed
  , max(total_bytes_processed)/pow(10,9) max_gb_processed
  , avg(total_bytes_processed)/pow(10,9) avg_gb_processed
FROM pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark
where statement_type in ('INSERT', 'CREATE_TABLE_AS_SELECT')
and regexp_substr(query, 'insert into `pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark`') is null
;

-- verify that all tables in stage are clustered
select * 
from pipeline-analysis-452722.nytaxi_stage.INFORMATION_SCHEMA.TABLES
where regexp_substr(ddl, 'CLUSTER BY data_source') is not null
-- just to make sure there is no clustering of another col 
-- where regexp_substr(ddl, 'CLUSTER BY vendor_id') is not null 
;

-- try to determine the variation in col_name/data_types
select 
  distinct column_name, data_type, ordinal_position
from pipeline-analysis-452722.nytaxi_stage.INFORMATION_SCHEMA.COLUMNS
order by column_name
;

-- spot check to see if there are records that dont pertain to the respective source parquet
-- quite difficult to do with all the different data types, better to do post the col cleaning
select * from 
(select 
  parse_datetime('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01') start_date
  , last_day(parse_date('%Y-%m-%d', regexp_substr(data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) end_date
  , *
from `pipeline-analysis-452722.nytaxi_stage.yellow_tripdata_2009-01`
)

where (
  -- (start_date > parse_datetime('%Y-%m-%d %H:%M:%S', trip_pickup_date_time))
  -- or
  (end_date < parse_datetime('%Y-%m-%d %H:%M:%S', trip_dropoff_date_time))
) 
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