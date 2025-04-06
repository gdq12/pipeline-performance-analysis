{# 
    
    This macro copy clones tables originally created by dataproc to a new schema for developement and performance testing
    Example commands:
        -- to specify tbl names with specific trip type substr and yrs
        dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: 'yellow', yr_str: "2019|2020|2021", method: "refresh_schema"}'
        dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: 'green', yr_str: "2021", method: "add_tables"}'
        -- when dont want to specify 1 where clause use '.*' instead
        dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: 'yellow', yr_str: ".*", method: "refresh_schema"}'

#}

{% macro copy_clone_raw_tables(tbl_name_str, yr_str, method) -%}

{% if method == 'refresh_schema' %}

    {% do log("---------------------" ~ method ~ " chosen, Recreating raw schema" ~ "--------------------", info = True) %}
    {% do log("-------------------------Compiling drop queries for Schema----------------------------------", info=True) %}

    {% set drop_old_tbl_queries %} 
    select concat("drop table `", table_catalog, "`.`",table_schema,"`.`",   table_name, "`;" ) querie_str
    from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.INFORMATION_SCHEMA.TABLES
    where table_schema = '{{ env_var("BQ_RAW_SCHEMA") }}'
    {% endset %}

    {% set queries = dbt_utils.get_query_results_as_dict(drop_old_tbl_queries)%}

    {% set schema_drop %}
    drop schema if exists {{ env_var('BQ_RAW_SCHEMA') }}
    {% endset %}

    {% if 'drop table ' in queries['querie_str'][0] %}

        {% for query in queries['querie_str'] %}

            {% do log("executing query: " ~ query, info = True) %}
            {% do run_query(query) %}

        {% endfor %}

        {% do log("executing query: " ~ schema_drop, info = True)%}
        {% do run_query(schema_drop) %}

    {% else %}

        {% do log("no tables found, can just drop schema: " ~ schema_drop)%}
        {% do run_query(schema_drop) %}

    {% endif %}

    {% do log("---------------------Env cleaned up, Can copy tables now-----------------------------------", info=True) %}

    {% set schema_create %}
    create schema if not exists {{ env_var('BQ_RAW_SCHEMA') }} options (location = 'EU')
    {% endset %}
    {% do run_query(schema_create) %}

    {% do log("New schema created", info=True) %}

    {% set tbl_query %} 
    select table_name
    from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.INFORMATION_SCHEMA.TABLES
    where regexp_substr(table_name, '{{ tbl_name_str }}') is not null
    and regexp_substr(table_name, '{{ yr_str }}') is not null
    and regexp_substr(table_name, 'external') is null
    {% endset %}

    {% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

    {% if table_names['table_name'] | length == 0 %}

        {% do log("No tables found in backup schema that fit the input parameters, no cloning to carry out", info = True) %}

    {% else %}

        {% do log("Copy cloning the following tables to the new schema: " ~ table_names, info=True) %}

        {% for tbl_name in table_names['table_name'] %}

            {% set clone_query %}
            create table 
            `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.`{{ tbl_name }}`
            clone `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.`{{ tbl_name }}`
            {% endset %}

            {% do run_query(clone_query) %}

        {% endfor %}

    {% endif %}

{% else %}

    {% do log("-----------" ~ method ~ " chosen, cloning tables to raw schema (not already there)" ~ "--------------", info = True) %}

    {% set tbl_query %} 
    select t1.table_name
    from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.INFORMATION_SCHEMA.TABLES t1 
    left join `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.INFORMATION_SCHEMA.TABLES t2 on t1.table_name = t2.table_name 
    where t2.table_name is null 
    and regexp_substr(t1.table_name, '{{ tbl_name_str }}') is not null
    and regexp_substr(t1.table_name, '{{ yr_str }}') is not null
    and regexp_substr(t1.table_name, 'external') is null
    {% endset %}

    {% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

    {% if table_names['table_name'] | length == 0 %}

        {% do log("No tables found in backup schema that fit the input parameters, no cloning to carry out", info = True) %}

    {% else %}

        {% do log("Copy cloning the following tables to the new schema: " ~ table_names, info=True) %}

        {% for tbl_name in table_names['table_name'] %}

            {% set clone_query %}
            create table 
            `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.`{{ tbl_name }}`
            clone `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.`{{ tbl_name }}`
            {% endset %}

            {% do run_query(clone_query) %}

        {% endfor %}

    {% endif %}

{% endif %}

{{ log("-------------------------Schema create and Clone tasks complete----------------------------------", info=True) }}

{%- endmacro %}