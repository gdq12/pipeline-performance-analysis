{{ config(materialized='view') }}

with t1 as 
({% set table_names = dbt_utils.get_column_values(
  table = ref('vw_column_name_mapping'), 
  column = 'table_name', 
  order_by = 'table_name', 
  where = "regexp_substr(table_name, '2009|2010') is null"
) %}

{% for tbl_name in table_names %}

  {% set col_query %}
  select old_column_name, new_column_name 
  from {{ ref('vw_column_name_mapping') }}
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
)
select 
  {{ update_vendor_id(trp.vendor_id) }} vendor_id
  , trp.pickup_datetime
  , trp.dropoff_datetime
  , trp.passenger_count
  , trp.trip_distance
  , trp.pickup_location_id
  , trp.rate_code
  , trp.store_and_fwd_flag
  , trp.dropoff_location_id
  , trp.payment_type
  , trp.fare_amount
  , trp.surcharge
  , trp.mta_tax
  , trp.tip_amount
  , trp.tolls_amount
  , trp.total_amount
  , trp.pickup_date
  , regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$') trip_type
  , parse_datetime('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01') data_start_date
  , last_day(parse_date('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) data_end_date
  , trp.data_source
  , trp.creation_dt
from t1 trp