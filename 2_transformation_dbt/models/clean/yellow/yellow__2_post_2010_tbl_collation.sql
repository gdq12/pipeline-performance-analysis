{{ config(materialized='view') }}

with trps as 
({% set tbl_query %}
  select distinct table_name
  from {{ ref('yellow__1_column_name_mapping') }}
  where regexp_substr(table_name, '2009|2010') is null
  and regexp_substr(table_name, 'yellow_tripdata') is not null
  and table_name != 'yellow__1_column_name_mapping'
  order by 1
{% endset %}

{% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

{% for tbl_name in table_names['table_name'] %}

  {% set col_query %}
  select old_column_name, new_column_name 
  from {{ ref('yellow__1_column_name_mapping') }}
  where table_name = '{{ tbl_name }}'
  order by 2
  {% endset %}

  {% set col_dict = dbt_utils.get_query_results_as_dict(col_query)%}

  select 
    {% for old_name, new_name in zip(col_dict['old_column_name'], col_dict['new_column_name']) %}
      {{ old_name }} as {{ new_name }}
        {% if not loop.last -%} , {% endif -%}
    {% endfor %}
  from `{{ env_var('PROJECT_ID') }}`.`nytaxi_stage`.`{{ tbl_name }}`
    {% if not loop.last -%} union all {% endif -%}
{% endfor %}
)
select 
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_location_id,
  ratecode_id,
  store_and_fwd_flag,
  dropoff_location_id,
  payment_type,
  fare_amount,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  pickup_date,
  data_source,
  creation_dt
from trps