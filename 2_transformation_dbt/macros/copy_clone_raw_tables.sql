{# 
    cmd:  dbt run-operation copy_clone_raw_tables --args '{tbl_substr: 2019|2020|2021}' 
    error message: 'dict' object is not callable

#}

{% macro copy_clone_raw_tables(tbl_substr) -%}

{% set schema_drop %}
drop schema if exists {{ env('BQ_RAW_SCHEMA') }}
{% endset %}
{% do run_query(schema_drop) %}

{% set schema_create %}
create schema if not exists {{ env('BQ_RAW_SCHEMA') }} options (location = 'EU')
{% endset %}
{% do run_query(schema_create) %}

{% set tbl_query %} 
select table_name
from `{{ env('PROJECT_ID') }}`.`{{ env('BQ_RAW_SCHEMA') }}_backup`.INFORMATION_SCHEMA.TABLES
where regexp_substr(table_name, '{{ tbl_substr }}') is not null
{% endset %}

{% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

{% for tbl_name in table_names['table_name'] %}

    {% set clone_query %}
    create table 
    `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.`{{ tbl_name }}`
    clone `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.`{{ tbl_name }}`
    {% endset %}

    {% do run_query(clone_query) %}

{% endfor %}

{%- endmacro %}