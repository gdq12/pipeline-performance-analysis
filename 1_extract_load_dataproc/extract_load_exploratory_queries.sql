-- which jobs failed to populate stage 
select 
  regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}') label
  , query
from pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark
where regexp_substr(error_result.message, 'Inserted row has wrong column count') is not null
order by 1
;

--what kind of error messages were there 
select 
  distinct regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}') label
  , error_result.reason, error_result.message
from pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark
where error_result.message is not null
order by 1
;

-- determine possible column name mismatch 
select 
  regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}') label
  , regexp_substr(query, "on t1.column_name = '[a-z_].+'") 
  , query , error_result.reason, error_result.message
from pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark
where error_result.message is not null
;

-- what jobs didnt make it to stage 
select distinct data_source 
from pipeline-analysis-452722.nytaxi_monitoring.query_history_extract_load_spark
where data_source not in 
(select data_source from pipeline-analysis-452722.nytaxi_stage.yellow_tripdata
union distinct 
select data_source from pipeline-analysis-452722.nytaxi_stage.yellow_tripdata_2009
union distinct 
select data_source from pipeline-analysis-452722.nytaxi_stage.yellow_tripdata_2010
)
order by 1
;

-- check to see each parquet was loaded to stage
with ext as
(SELECT  
  regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}') label
  , data_source, table_name, num_row
  , max(creation_dt) creation_dt, count(1) num_columns
FROM `pipeline-analysis-452722.nytaxi_raw.extract_load_log`
where regexp_substr(table_name, 'external') is not null 
group by regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}')
          , data_source, table_name, num_row
), 
stg as 
(SELECT  
  regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}') label
  , data_source, table_name, num_row
  , max(creation_dt) creation_dt, count(1) num_columns
FROM `pipeline-analysis-452722.nytaxi_raw.extract_load_log`
where regexp_substr(table_name, 'external') is null 
group by regexp_substr(data_source, '[a-z]{1,6}_tripdata_[0-9]{4}-[0-9]{2}')
          , data_source, table_name, num_row
)
select 
  ext.label, ext.data_source
  , ext.table_name, stg.table_name
  , ext.creation_dt, stg.creation_dt
  , ext.num_row, stg.num_row
  , ext.num_columns, stg.num_columns
from ext ext 
join stg stg on ext.label = stg.label 
            and ext.data_source = stg.data_source 
where ext.num_columns != stg.num_columns
;