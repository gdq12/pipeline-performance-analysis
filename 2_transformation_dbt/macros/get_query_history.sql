{#

#}

{% macro get_query_history() -%}

{% set query_tag=var("run_tag") %}

{% do log("---------------------updating query history based on [" ~ query_tag ~ "] run------------------------------", info = True) %}

{% set qhistory_query%}
insert into `{{ env_var('PROJECT_ID') }}.{{ env_var('BQ_SCHEMA_PREFIX') }}_monitoring.query_history_extract_load_transform_project`
select creation_time, project_id, project_number, user_email, job_id, job_type
, statement_type, priority, start_time, end_time, query, state, reservation_id
, total_bytes_processed, total_slot_ms, error_result, cache_hit, destination_table
, referenced_tables, labels, timeline, job_stages, total_bytes_billed, transaction_id
, parent_job_id, session_info, dml_statistics, total_modified_partitions
, bi_engine_statistics, query_info, transferred_bytes, materialized_view_statistics
, edition, job_creation_reason, metadata_cache_statistics, search_statistics
, cast(null as string) data_source, '{{ query_tag }}' log_comment
, current_timestamp() creation_dt
from region-eu.INFORMATION_SCHEMA.JOBS_BY_USER
where TIMESTAMP(DATETIME(start_time,"UTC")) >= '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'
{% endset %}

{% do run_query(qhistory_query) %}

{%- endmacro %}