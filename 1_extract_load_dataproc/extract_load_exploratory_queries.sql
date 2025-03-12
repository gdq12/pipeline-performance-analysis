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