{{ config(materialized='view') }}

with t1 as 
({% set tbl_query %}
  select distinct table_name
  from {{ ref('vw_column_name_mapping') }}
  where regexp_substr(table_name, '2009|2010') is null
  and regexp_substr(table_name, 'yellow_tripdata') is not null
  and regexp_substr(table_name, 'vw_') is null
  order by 1
{% endset %}

{% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

{% for tbl_name in table_names['table_name'] %}

  {% set col_query %}
  select old_column_name, new_column_name 
  from {{ ref('vw_column_name_mapping') }}
  where table_name = '{{ tbl_name }}'
  order by 2
  {% endset %}

  {% set col_dict = dbt_utils.get_query_results_as_dict(col_query)%}

  select 
    {% for old_name, new_name in zip(col_dict['old_column_name'], col_dict['new_column_name']) %}
      {{ old_name }} as {{ new_name }}
        {% if not loop.last -%} , {% endif -%}
    {% endfor %}
  from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_SCHEMA')}}`.`{{ tbl_name }}`
    {% if not loop.last -%} union all {% endif -%}
{% endfor %}
)
select 
  trp.vendor_id
  , trp.pickup_datetime
  , trp.dropoff_datetime
  , trp.passenger_count
  , trp.trip_distance
  , trp.pickup_location_id
  , trp.ratecode_id
  , trp.store_and_fwd_flag
  , trp.dropoff_location_id
  , trp.payment_type
  , trp.fare_amount
  , trp.mta_tax
  , trp.tip_amount
  , trp.tolls_amount
  , trp.improvement_surcharge
  , trp.total_amount
  , trp.congestion_surcharge
  , trp.pickup_date
  , regexp_replace(regexp_substr(trp.data_source, '[a-z]{1,6}_tripdata'), '_tripdata', '') trip_type
  , parse_datetime('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01') data_start_date
  , last_day(parse_date('%Y-%m-%d', regexp_substr(trp.data_source, '[0-9]{4}-[0-9]{2}$')||'-01'), month) data_end_date
  , trp.data_source
  , trp.creation_dt
from t1 trp