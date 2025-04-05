{# 
    
    This macro compares current bigqeury tables and views to the model nodes and removes the difference.

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

        {% if bq_resource in current_dbt_models %}
            {% do log(bq_resource ~ " wont be dropped", info = True) %}
        {% else %}
            {% do log("detected BigQuery resource " ~ bq_resource ~ ", dropping table: " ~ target_tbl_name, info = True) %}

            {% set drop_tbl_query %}
            drop table `{{ env_var('PROJECT_ID') }}`.{{target_tbl_name}}
            {% endset %}

            {% do run_query(drop_tbl_query) %}

        {% endif %}

    {% endfor %}

{% endfor %}

{{ log("-----------------------------BigQuery cleanup complete----------------------------------", info=True) }}

{%- endmacro %}