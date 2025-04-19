-- see how clone tbl macro commands will affect processing data testing size
create or replace temporary table incremental_load_size as
with t1 as
(select table_id, size_bytes/pow(10,12) size_tb, '(1)' label
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.__TABLES__
where regexp_substr(table_id, 'external|mapping') is null
and table_id in ('yellow_tripdata_2009-01', 'yellow_tripdata_2011-01', 'green_tripdata_2014-01', 'fhvhv_tripdata_2019-02', 'fhv_tripdata_2015-01')
),
t2 as
(select table_id, size_bytes/pow(10,12) size_tb, '(2)' label
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.__TABLES__
where regexp_substr(table_id, 'external|mapping') is null
and table_id not in (select table_id from t1)
and (regexp_substr(table_id, 'yellow|green|fhvhv|fhv') is not null
     and regexp_substr(table_id, '2011|2015|2019') is not null)
),
t3 as
(select table_id, size_bytes/pow(10,12) size_tb, '(3)' label
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.__TABLES__
where regexp_substr(table_id, 'external|mapping') is null
and table_id not in (select table_id from t1)
and table_id not in (select table_id from t2)
and (regexp_substr(table_id, 'green|fhvhv|fhv') is not null
      and regexp_substr(table_id, '2018|2020') is not null)
),
t4 as
(select table_id, size_bytes/pow(10,12) size_tb, '(4)' label
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.__TABLES__
where regexp_substr(table_id, 'external|mapping') is null
and table_id not in (select table_id from t1)
and table_id not in (select table_id from t2)
and table_id not in (select table_id from t3)
and (regexp_substr(table_id, 'yellow|green|fhvhv|fhv') is not null
    and regexp_substr(table_id, '2010|2014|2017|2021|2022|2023') is not null)
),
t5 as
(select table_id, size_bytes/pow(10,12) size_tb, '(5)' label
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.__TABLES__
where regexp_substr(table_id, 'external|mapping') is null
and table_id not in (select table_id from t1)
and table_id not in (select table_id from t2)
and table_id not in (select table_id from t3)
and table_id not in (select table_id from t4)
),
t6 as
(select table_id, size_bytes/pow(10,12) size_tb, '(6)' label
from `pipeline-analysis-455005`.`nytaxi_raw_backup`.__TABLES__
where regexp_substr(table_id, 'external|mapping') is null
),
all_tbl as
(select * from t1
union all
select * from t2
union all
select * from t3
union all
select * from t4
union all
select * from t5
union all
select * from t6
)
select label, sum(size_tb) load_tb_size
from all_tbl
group by label
;

create or replace temporary table incremental_load_processed as
with t1 as
(select
  log_comment
  , regexp_substr(log_comment, '\\([0-9]{1}\\)') label
  , regexp_substr(log_comment, '^[a-z0-9\\/]{1,11}') transformation_type
  , count(1) num_queries
  , sum(total_bytes_processed)/pow(10,12) total_tb_processed
  , sum(total_bytes_billed)/pow(10,12) total_tb_billed
  , min(start_time) start_time
  , max(end_time) end_time
from`pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project`
where log_comment is not null
and start_time >= '2025-04-17'
group by all
)
select
  t7.log_comment, t7.label, t7.transformation_type
  , (t7.end_time - t7.start_time) duration
  , t7.num_queries
  , t7.total_tb_processed
  , t7.total_tb_billed
  , t7.total_tb_billed*6.25 cost_dollars
from t1 t7
;

create or replace temporary table jobs_1_liner_info as
select 
  -- some info on query 
  job_id, log_comment, statement_type
  , case 
      when statement_type = 'SELECT' 
      and regexp_substr(query, 'test.pipeline_analysis_transform') is not null 
        then 'DBT_TEST'
      when statement_type in ('CREATE_TABLE_AS_SELECT', 'MERGE') 
      and regexp_substr(query, '__dbt_tmp') is not null 
        then 'INCREMENTAL'
      when statement_type in ('CREATE_TABLE_AS_SELECT', 'MERGE') 
      and regexp_substr(query, '__dbt_tmp') is null 
        then 'FULL_REFRESH'
      when statement_type = 'CREATE_VIEW'
        then 'VIEW'
      when statement_type = 'SELECT'
      and regexp_substr(query, '_tbl_collation|_data_types') is not null
      and regexp_substr(query, 'dbt_version') is not null
        then 'VIEW'
      else null 
      end run_tag
  , case 
      when regexp_substr(statement_type, 'CREATE_TABLE|CREATE_VIEW|MERGE') is not null 
      then destination_table.table_id 
      else null 
      end destination_table
  , case 
      when regexp_substr(statement_type, 'CREATE_TABLE|CREATE_VIEW|MERGE') is not null 
      then destination_table.dataset_id 
      else null 
      end destination_schema
  , query
  -- some run time stats
  , start_time, end_time, total_bytes_processed, total_bytes_billed
  -- data byte exchange related
  , total_modified_partitions
  , dml_statistics.inserted_row_count, dml_statistics.deleted_row_count, dml_statistics.updated_row_count
  , transferred_bytes
  -- , query_info.optimization_details, query_info.performance_insights.avg_previous_execution_ms
  -- , query_info.performance_insights.stage_performance_standalone_insights
from `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project`
;

create or replace temporary table job_stages_info as 
select 
  job_id, query, log_comment
  , js.id job_stage_id, js.name job_stage_name
  , js.start_ms, js.end_ms
  , js.wait_ratio_avg, js.wait_ms_avg, js.wait_ratio_max, js.wait_ms_max
  , js.read_ratio_avg, js.read_ms_avg, js.read_ratio_max, js.read_ms_max
  , js.compute_ratio_avg, js.compute_ms_avg, js.compute_ratio_max, js.compute_ms_max
  , js.write_ratio_avg, js.write_ms_avg, js.write_ratio_max, js.write_ms_max
  , js.shuffle_output_bytes, js.shuffle_output_bytes_spilled
  , js.records_read, js.records_written
  , js.slot_ms, js.compute_mode
from `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project`,
unnest(job_stages) js
;

create or replace temporary table job_performance_insights as 
select 
  t.job_id, t.query, t.log_comment
  -- warning and optimzation approaches taken 
  , t.query_info.resource_warning, t.query_info.optimization_details
  -- perhaps can use hashes to compare query performance btw runs?
  , t.query_info.query_hashes.normalized_literals
  -- some stats that compares current performance vs previous run 
  , t.query_info.performance_insights.avg_previous_execution_ms
  -- stage_performance_standalone_insights --> perhaps insights on current run 
  , sa1.stage_id, sa1.slot_contention, sa1.insufficient_shuffle_quota
  , sa1_bi.code, sa1_bi.message
  , sa1_jo.left_rows, sa1_jo.right_rows, sa1_jo.output_rows, sa1_jo.step_index
  -- stage performance changes insights 
  , sa2.stage_id, sa2.input_data_change.records_read_diff_percentage
from `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project` t
left join unnest(t.query_info.performance_insights.stage_performance_standalone_insights) sa1
left join unnest(sa1.bi_engine_reasons) sa1_bi
left join unnest(sa1.high_cardinality_joins) sa1_jo
left join unnest(t.query_info.performance_insights.stage_performance_change_insights) sa2
;