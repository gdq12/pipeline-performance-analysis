{# 
    
    This macro copy clones tables originally created by dataproc to a new schema for developement and performance testing
    Example commands:
        -- to specify tbl names with specific trip type substr and yrs
        dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: 'yellow', yr_str: "2019|2020|2021"}'
        -- when dont want to specify 1 where clause use '.*' instead
        dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: 'yellow', yr_str: ".*"}'

#}

{% macro copy_clone_raw_tables(tbl_name_str, yr_str) -%}

{# compile drop table queries if there are any tables in the target schema #}
{% set drop_old_tbl_queries %} 
select concat("drop table `", table_catalog, "`.`",table_schema,"`.`",   table_name, "`;" ) querie_str
from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.INFORMATION_SCHEMA.TABLES
where table_schema = '{{ env_var("BQ_RAW_SCHEMA") }}'
{% endset %}

{% set queries = dbt_utils.get_query_results_as_dict(drop_old_tbl_queries)%}

{# if query produce drop table query strings, then drop tables, then drop schema #}
{% if 'drop table ' in queries['querie_str'][0] %}

    {% for query in queries['querie_str'] %}

        {% do run_query(query) %}

    {% endfor %}

    {% set schema_drop %}
    drop schema if exists {{ env_var('BQ_RAW_SCHEMA') }}
    {% endset %}

    {% do run_query(schema_drop) %}

{# if no drop table query strings exists, then just drop schema #}
{% else %}

    {% set schema_drop %}
    drop schema if exists {{ env_var('BQ_RAW_SCHEMA') }}
    {% endset %}

    {% do run_query(schema_drop) %}

{% endif %}

{# create a fresh new schema #}
{% set schema_create %}
create schema if not exists {{ env_var('BQ_RAW_SCHEMA') }} options (location = 'EU')
{% endset %}
{% do run_query(schema_create) %}

{# based on macro input, find tables from the backup schema that fit the parameters #}
{% set tbl_query %} 
select table_name
from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.INFORMATION_SCHEMA.TABLES
where regexp_substr(table_name, '{{ tbl_name_str }}') is not null
and regexp_substr(table_name, '{{ yr_str }}') is not null
and regexp_substr(table_name, 'external') is null
{% endset %}

{% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

{# copy clone the tables into the newly created schema that fit the macro input parameters #}
{% for tbl_name in table_names['table_name'] %}

    {% set clone_query %}
    create table 
    `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.`{{ tbl_name }}`
    clone `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.`{{ tbl_name }}`
    {% endset %}

    {% do run_query(clone_query) %}

{% endfor %}

{%- endmacro %}