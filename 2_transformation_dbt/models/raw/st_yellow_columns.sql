{{ config(materialized='view') }}

{% set table_names = dbt_utils.get_column_values(ref('column_name_data_type_mapping'), 'table_name', order_by='table_name') %}

{% for tbl_name in table_names %}

  {% set col_query %}
  select old_column_name, new_column_name 
  from {{ ref('column_name_data_type_mapping') }}
  where table_name = '{{ tbl_name }}'
  {% endset %}

  {% set col_dict = dbt_utils.get_query_results_as_dict(col_query)%}

  select 
    {% for old_name, new_name in zip(col_dict['old_column_name'], col_dict['new_column_name']) %}
      {{ old_name }} as {{ new_name }}
        {% if not loop.last -%} , {% endif -%}
    {% endfor %}
  from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_SCHEMA')}}`.`{{ tbl_name }}`
    {% if not loop.last -%} union {% endif -%}
{% endfor %}