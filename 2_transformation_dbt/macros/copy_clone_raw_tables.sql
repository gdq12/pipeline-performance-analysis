{# 
    cmd:  dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: '.*', yr_str: "2019|2020|2021"}' --debug

#}

{% macro copy_clone_raw_tables(tbl_name_str, yr_str) -%}

{% set drop_old_tbl_queries %} 
select concat("drop table `", table_catalog, "`.`",table_schema,"`.`",   table_name, "`;" ) querie_str
from `pipeline-analysis-455005`.`nytaxi_raw`.INFORMATION_SCHEMA.TABLES
where table_schema = 'nytaxi_raw'
{% endset %}

{% set queries = dbt_utils.get_query_results_as_dict(drop_old_tbl_queries)%}

{% if 'drop table ' in queries['querie_str'][0] %}

    {% for query in queries['querie_str'] %}

        {% do run_query(query) %}

    {% endfor %}

    {% set schema_drop %}
    drop schema if exists {{ env_var('BQ_RAW_SCHEMA') }}
    {% endset %}

    {% do run_query(schema_drop) %}

{% else %}

    {% set schema_drop %}
    drop schema if exists {{ env_var('BQ_RAW_SCHEMA') }}
    {% endset %}

    {% do run_query(schema_drop) %}

{% endif %}

{% set schema_create %}
create schema if not exists {{ env_var('BQ_RAW_SCHEMA') }} options (location = 'EU')
{% endset %}
{% do run_query(schema_create) %}

{% set tbl_query %} 
select table_name
from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.INFORMATION_SCHEMA.TABLES
where regexp_substr(table_name, '{{ tbl_name_str }}') is not null
and regexp_substr(table_name, '{{ yr_str }}') is not null
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