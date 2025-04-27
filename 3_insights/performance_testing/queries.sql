-----------------------------------------------------------------temporary tables for analysis---------------------------------------------------
-- quantify volume of data loaded to nytaxi_stage for each incremental run
create or replace temp table incremental_load_size as
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
-- for regular temp table for analysis 
select label, sum(size_tb) load_tb_size
from all_tbl
group by label
-- to check for slot contention, data_volume and tbl num per run #
-- select 
--     regexp_substr(table_id, 'yellow|green|fhvhv|fhv') trip_type
--     , label
--     , count(table_id) num_tbls
--     , sum(size_tb) load_tb_size
-- from all_tbl
-- group by all
-- order by 2, 3 desc
;

select * from incremental_load_size limit 10;

-- quatify volume of data scanned per incremental run 
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
and date_trunc(start_time, DAY) = '2025-04-17'
group by all
)
select
  t7.log_comment, t7.label, t7.transformation_type
  , time_diff(cast(t7.end_time as time), cast(t7.start_time as time), second) duration_seconds
  , t7.num_queries
  , t7.total_tb_processed
  , t7.total_tb_billed
  , t7.total_tb_billed*6.25 cost_dollars
from t1 t7
;

select * from incremental_load_processed limit 10;

-- get 1-liner info per jonb/query 
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
from `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project`
where date_trunc(start_time, DAY) = '2025-04-17'
;

select * from jobs_1_liner_info;

-- get detailed stats for all stages per job
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
where date_trunc(start_time, DAY) = '2025-04-17'
;

select * from job_stages_info limit 10;

-- query insights that are available for jobs ran
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
  , sa2.stage_id stage_id_change_insights, sa2.input_data_change.records_read_diff_percentage
from `pipeline-analysis-455005.nytaxi_monitoring.query_history_extract_load_transform_project` t
left join unnest(t.query_info.performance_insights.stage_performance_standalone_insights) sa1
left join unnest(sa1.bi_engine_reasons) sa1_bi
left join unnest(sa1.high_cardinality_joins) sa1_jo
left join unnest(t.query_info.performance_insights.stage_performance_change_insights) sa2
where date_trunc(t.start_time, DAY) = '2025-04-17'
;

select * from job_performance_insights limit 10;

-------------------------------------------------------eda queries--------------------------------------------------------------------------
-- what are the stats at a high level overview
select 
    inc.label
    , inc.load_tb_size
    , (inc.load_tb_size-lag(inc.load_tb_size) over (partition by prc.transformation_type order by inc.label))/inc.load_tb_size load_volume_change_perc
    , prc.transformation_type
    , regexp_substr(prc.log_comment, 'incremental|initial_tables|full refresh') load_type
    , prc.duration_seconds
    , (prc.duration_seconds-lag(prc.duration_seconds) over (partition by prc.transformation_type order by inc.label))/prc.duration_seconds run_time_change_perc
    , prc.total_tb_billed
    , (prc.total_tb_billed-lag(prc.total_tb_billed) over (partition by prc.transformation_type order by inc.label))/prc.total_tb_billed scanned_volume_change_perc
    , prc.cost_dollars
    , (prc.cost_dollars-lag(prc.cost_dollars) over (partition by prc.transformation_type order by inc.label))/prc.cost_dollars cost_change_perc
from incremental_load_size inc
join incremental_load_processed prc on inc.label = prc.label 
order by inc.label, prc.transformation_type
;

-- check to see how the transformation difference per table 
with t1 as 
(select 
    inc.label, prc.transformation_type
    , regexp_replace(regexp_replace(jb1.destination_table, '__dbt_tmp|core1_|core2_|mart1_|mart2_', ''), 'revenue', 'stats') destination_table
    , inc.load_tb_size
    , sum(jb1.total_bytes_processed)/pow(10,9) total_gb_processed
    , sum(time_diff(cast(jb1.end_time as time), cast(jb1.start_time as time), second)) duration_seconds
from incremental_load_size inc
join incremental_load_processed prc on inc.label = prc.label
join jobs_1_liner_info jb1 on prc.log_comment = jb1.log_comment
where jb1.total_bytes_processed > 0
and jb1.destination_table is not null
group by all
)
select 
    label, transformation_type
    , destination_table
    , load_tb_size
    , total_gb_processed
    , (total_gb_processed-lag(total_gb_processed) over (partition by destination_table, label order by transformation_type))/total_gb_processed scanned_tb_change_perc
    , duration_seconds
    , (duration_seconds-lag(duration_seconds) over (partition by destination_table, label order by transformation_type))/duration_seconds time_spent_change_perc
from t1 
order by 3, 1, 2
;

-- look into dimension table performance 
select 
    jb1.statement_type, prc.transformation_type, inc.label
    , regexp_replace(regexp_replace(jb1.destination_table, '__dbt_tmp|core1_|core2_|mart1_|mart2_', ''), 'revenue', 'stats') destination_table
    , regexp_replace(jb1.destination_schema, 'nytaxi_|1$|2$', '') destination_schema
    , jb1.total_modified_partitions total_modified_partitions, jb1.total_bytes_billed/pow(10,9) total_gb_billed
    , jb1.inserted_row_count inserted_row_count, jb1.deleted_row_count deleted_row_count, jb1.updated_row_count updated_row_count
from incremental_load_size inc
join incremental_load_processed prc on inc.label = prc.label
join jobs_1_liner_info jb1 on prc.log_comment = jb1.log_comment
where regexp_substr(destination_table, 'dm_') is not null
order by inc.label, prc.transformation_type, destination_table
;

-- what could of caused slot contention
with sc as 
(select 
    distinct jb1.job_id, stg.job_stage_id
    , 'model.pipeline_analysis_transform.'||regexp_replace(jb1.destination_table, '__dbt_tmp', '') model_name
    , prc.transformation_type
    , trim(regexp_replace(stg.job_stage_name, '^[A-Z0-9].+:', '')) job_stage_name_adjusted
from incremental_load_size inc
join incremental_load_processed prc on inc.label = prc.label
join job_performance_insights ing on prc.log_comment = ing.log_comment
join job_stages_info stg on ing.job_id = stg.job_id and ing.stage_id = stg.job_stage_id
join jobs_1_liner_info jb1 on ing.job_id = jb1.job_id and stg.job_id = jb1.job_id
where ing.slot_contention is true
), 
t1 as 
(select
    prc.transformation_type, inc.label
    , stg.job_id, stg.query, stg.log_comment, jb1.start_time job_start_time, jb1.end_time job_end_time
    , stg.job_stage_id, stg.job_stage_name, trim(regexp_replace(stg.job_stage_name, '^[A-Z0-9].+:', '')) job_stage_name_adjusted
    , jb1.destination_schema, jb1.destination_table
    , 'model.pipeline_analysis_transform.'||regexp_replace(jb1.destination_table, '__dbt_tmp', '') model_name
    , stg.start_ms, stg.end_ms
    , stg.wait_ratio_avg, stg.wait_ms_avg, stg.wait_ratio_max, stg.wait_ms_max
    , stg.read_ratio_avg, stg.read_ms_avg, stg.read_ratio_max, stg.read_ms_max
    , stg.compute_ratio_avg, stg.compute_ms_avg, stg.compute_ratio_max, stg.compute_ms_max
    , stg.write_ratio_avg, stg.write_ms_avg, stg.write_ratio_max, stg.write_ms_max
    , stg.shuffle_output_bytes, stg.shuffle_output_bytes_spilled
    , stg.records_read, stg.records_written
    , stg.slot_ms
    , case when sc2.job_stage_id is null then null else 'Y' end slot_contention
from incremental_load_size inc
join incremental_load_processed prc on inc.label = prc.label
join job_stages_info stg on prc.log_comment = stg.log_comment
join jobs_1_liner_info jb1 on stg.job_id = jb1.job_id 
join sc sc on 'model.pipeline_analysis_transform.'||regexp_replace(jb1.destination_table, '__dbt_tmp', '') = sc.model_name and prc.transformation_type = sc.transformation_type
left join sc sc2 on stg.job_id = sc2.job_id and stg.job_stage_id = sc2.job_stage_id
),
t2 as 
(select t1.job_id, sum(jb2.total_bytes_billed)/pow(10,9) parallel_model_bytes, count(distinct jb2.job_id) parallel_num_jobs
from (select 
    distinct job_id, job_start_time, job_end_time, destination_schema, destination_table
    from t1) t1 
left join jobs_1_liner_info jb2 on t1.job_start_time <= jb2.start_time 
                                and t1.job_end_time >= jb2.end_time 
                                and t1.destination_schema != jb2.destination_schema 
                                and t1.destination_table != jb2.destination_table 
group by all
)
select 
    t1.transformation_type, t1.label
    , t1.model_name, t1.job_stage_name_adjusted, t1.slot_contention, t2.parallel_model_bytes, t2.parallel_num_jobs
    , t1.wait_ms_avg, t1.wait_ms_max, (t1.wait_ms_max-t1.wait_ms_avg) wait_diff
    , t1.read_ms_avg, t1.read_ms_max, (t1.read_ms_max-t1.read_ms_avg) read_diff
    , t1.compute_ms_avg, t1.compute_ms_max, (t1.compute_ms_max-t1.compute_ms_avg) compute_diff
    , t1.write_ms_avg, t1.write_ms_max, (t1.write_ms_max-t1.write_ms_avg) write_diff
    , t1.records_read, t1.records_written
    , t1.slot_ms
from t1 t1 
join t2 t2 on t1.job_id = t2.job_id
-- where t1.job_stage_name_adjusted in ('Sort+', 'Join+')
-- and slot_contention is not null
-- group by all 
-- order by t1.job_stage_name_adjusted, t1.label, t1.slot_contention desc
;

-- impact incremental data volume has on wait and conpute ms
-- filename: transformationType_tblName_slot_compute_compare
with t1 as 
(select
    prc.transformation_type, inc.label
    , stg.job_id
    , regexp_replace(regexp_replace(jb1.destination_table, '__dbt_tmp|core1_|core2_|mart1_|mart2_', ''), 'revenue', 'stats') destination_table
    , avg(stg.wait_ms_max-stg.wait_ms_avg) wait_diff
    , avg(stg.read_ms_max-stg.read_ms_avg) read_diff
    , avg(stg.compute_ms_max-stg.compute_ms_avg) compute_diff
    , avg(stg.write_ms_max-stg.write_ms_avg) write_diff
    , avg(stg.records_read) records_read, avg(stg.records_written) records_written
    , avg(stg.slot_ms) slot_ms
from incremental_load_size inc
join incremental_load_processed prc on inc.label = prc.label
join job_stages_info stg on prc.log_comment = stg.log_comment
join jobs_1_liner_info jb1 on stg.job_id = jb1.job_id 
where jb1.destination_schema not in ('nytaxi_clean', 'nytaxi_stage')
group by all
), 
t2 as 
(select jb1.job_id, jb1.total_bytes_billed/pow(10,9) total_gb_billed
from jobs_1_liner_info jb1 
where jb1.destination_schema not in ('nytaxi_clean', 'nytaxi_stage')
)
select 
    t1.transformation_type, t1.label
    , t1.destination_table
    , t2.total_gb_billed
    , t1.wait_diff
    , t1.read_diff
    , t1.compute_diff
    , t1.write_diff
    , t1.records_read, t1.records_written
    , t1.slot_ms
from t1 t1 
join t2 t2 on t1.job_id = t2.job_id
where t1.destination_table is not null
;