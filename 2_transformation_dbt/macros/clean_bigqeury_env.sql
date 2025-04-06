{# 
    
    This macro compares current bigqeury tables and views to the model nodes and removes the difference.
    The hard-coded schemas and tables included are to exclude resources created by the spark service account from consideration.
    cmd: dbt run-operation clean_bigquery_env

#}

{% macro clean_bigquery_env() -%}

{{ log("-----------------------------Compiling current project model/node list----------------------------------", info=True) }}

{% set current_dbt_models = [] %}

{% for node in graph.nodes.values()
    | selectattr("resource_type", "equalto", "model") %}

    {% set model_info = node.schema ~ "." ~ node.name ~ "." ~ node.config.materialized %}

   {% do current_dbt_models.append(model_info) %}

{% endfor %}

{% do log("found the following models in project: " ~ current_dbt_models, info = True)%}

{{ log("------------------------Compiling current resources in BigQuery---------------------------", info=True) }}

{% set schema_list_query %}
select schema_name
from `{{ env_var('PROJECT_ID') }}`.`region-{{ env_var('BQ_REGION') }}`.INFORMATION_SCHEMA.SCHEMATA
where schema_name not in ('{{ env_var("BQ_RAW_SCHEMA") }}_backup', '{{ env_var("BQ_SCHEMA_PREFIX") }}_monitoring', '{{ env_var("BQ_RAW_SCHEMA") }}')
{% endset %}

{% set schema_dict = dbt_utils.get_query_results_as_dict(schema_list_query)%}

{% do log("Scanning the following schemas in BigQuery: " ~ schema_dict, info = True)%}

{% for schema_name in schema_dict['schema_name'] %}

    {% set tbl_list_query%}
    select sch1.table_schema, sch1.table_name, trim(replace(lower(sch1.table_type), 'base', '')) table_type
    from `{{ env_var('PROJECT_ID') }}`.`{{ schema_name }}`.INFORMATION_SCHEMA.TABLES sch1
    left join (select table_schema, table_name
                from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_SCHEMA_PREFIX') }}_mapping`.INFORMATION_SCHEMA.TABLES
                where table_name in ('hvlv_base_numbers', 'taxi_zone_geom', 'taxi_zone_lookup')
                ) sch2 on sch1.table_name = sch2.table_name 
                    and sch1.table_name = sch2.table_name
    where sch2.table_name is null
    {% endset %}

    {% set bq_result = dbt_utils.get_query_results_as_dict(tbl_list_query)%}

    {% do log("------------------------------num of tables found in schema: " ~ bq_result['table_name'] | length ~ "--------------------------", info = True) %}

    {% for i in range(bq_result['table_name'] | length)%}
    
        {% set bq_resource = bq_result['table_schema'][i] ~ "." ~ bq_result['table_name'][i] ~ "." ~ bq_result['table_type'][i]%}
        {% set target_tbl_name = "`" ~ bq_result['table_schema'][i] ~ "`.`" ~ bq_result['table_name'][i] ~ "`" %}
        {% set target_table_type = bq_result['table_type'][i] %}

        {% if bq_resource in current_dbt_models %}
            {% do log(bq_resource ~ ": found in current project models, wont be dropped", info = True) %}
        {% else %}
            {% do log("detected BigQuery resource not found in current project " ~ bq_resource ~ ", dropping " ~ target_table_type ~ ": " ~ target_tbl_name, info = True) %}

            {% set drop_tbl_query %}
            drop {{ target_table_type }} `{{ env_var('PROJECT_ID') }}`.{{ target_tbl_name }}
            {% endset %}

            {% do run_query(drop_tbl_query) %}

        {% endif %}

    {% endfor %}

{% endfor %}

{{ log("--------------------------Scanning for any empty Schemas--------------------------------", info=True) }}

{% for schema_name in schema_dict['schema_name'] %}

    {% set schema_scan_query%}
    select count(table_name) num_tables
    from `{{ env_var('PROJECT_ID') }}`.`{{ schema_name }}`.INFORMATION_SCHEMA.TABLES 
    {% endset %}

    {% set tbl_count_dict = dbt_utils.get_query_results_as_dict(schema_scan_query)%}

    {% if tbl_count_dict['num_tables'][0] == 0 %}

        {% do log(schema_name ~ " has " ~ tbl_count_dict['num_tables'][0] ~ " resources/tables, schema will be dropped", info = True) %}

        {% set schema_drop %}
        drop schema if exists {{ schema_name }}
        {% endset %}

        {% do run_query(schema_drop) %}
    
    {% else %}

        {% do log(schema_name ~ " has " ~ tbl_count_dict['num_tables'][0] ~ " resources/tables, schema will not be dropped", info = True) %}

    {% endif %}

{% endfor %}

{{ log("-----------------------------BigQuery cleanup complete----------------------------------", info=True) }}

{%- endmacro %}