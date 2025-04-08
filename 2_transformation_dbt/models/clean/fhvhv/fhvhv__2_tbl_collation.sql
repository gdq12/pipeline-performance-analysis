with trps as 
({% set tbl_query %}
  select distinct table_name
  from {{ ref('mapping__1_column_name_mapping') }}
  where regexp_substr(table_name, 'fhvhv_tripdata') is not null
  order by 1
{% endset %}

{% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

{% for tbl_name in table_names['table_name'] %}

  {% set col_query %}
  select old_column_name, new_column_name 
  from {{ ref('mapping__1_column_name_mapping') }}
  where table_name = '{{ tbl_name }}'
  order by 2
  {% endset %}

  {% set col_dict = dbt_utils.get_query_results_as_dict(col_query)%}

  select 
    {% for old_name, new_name in zip(col_dict['old_column_name'], col_dict['new_column_name']) %}
            {% if old_name == 'wav_match_flag' %}
                {{ dbt.safe_cast("wav_match_flag", api.Column.translate_type("string")) }} as {{ new_name }}
            {% else %}
                {{ old_name }} as {{ new_name }}
            {% endif %}
        {% if not loop.last -%} , {% endif -%}
    {% endfor %}
  from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}`.`{{ tbl_name }}`
    {% if not loop.last -%} union all {% endif -%}
{% endfor %}
)
select 
    hvfhs_license_number, 
    dispatching_base_number, 
    originating_base_number,
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime, 
    pickup_location_id,
    dropoff_location_id,
    trip_distance, 
    trip_time, 
    base_passenger_fare, 
    toll_amount,
    black_card_fund_amount,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tip_amount,
    driver_pay_amount,
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag, 
    wav_match_flag, 
    pickup_date,
    data_source,
    creation_dt,
    clone_dt
from trps 