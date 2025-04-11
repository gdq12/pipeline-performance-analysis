

  create or replace view `pipeline-analysis-455005`.`nytaxi_mapping`.`mapping__1_column_name_mapping`
  OPTIONS()
  as with t1 as 
(select 
  regexp_substr(table_name, 'yellow|green|fhvhv|fhv') trip_type
  , table_name
  , column_name old_column_name
  , case
      when column_name in ('start_lon', 'pickup_longitude') then 'pickup_longitude'
      when column_name in ('start_lat', 'pickup_latitude') then 'pickup_latitude'
      when column_name in ('end_lon', 'dropoff_longitude') then 'dropoff_longitude'
      when column_name in ('end_lat', 'dropoff_latitude') then 'dropoff_latitude'
      when column_name = 'store_and_forward' then 'store_and_fwd_flag'
      when column_name = 'vendor_name' then 'vendor_id'
      when column_name = 'rate_code' then 'ratecode_id'
      when column_name = 'trip_miles' then 'trip_distance'
      when regexp_substr(column_name, 'num$') is not null then regexp_replace(column_name, 'num$', 'number')
      when column_name = 'dispatching_base_num' then 'dispatching_base_number'
      when column_name in ('extra', 'tips', 'tolls', 'driver_pay') then regexp_replace(column_name, 's$', '')||'_amount'
      when column_name = 'surcharge' then 'congestion_surcharge'
      when column_name = 'bcf' then 'black_card_fund_amount'
      when regexp_substr(column_name, 'do_') is not null then regexp_replace(column_name, 'do_', 'dropoff_')
      when regexp_substr(column_name, 'd_o') is not null then regexp_replace(column_name, 'd_o', 'dropoff_')
      when regexp_substr(column_name, 'drop_off') is not null then regexp_replace(column_name, 'drop_off', 'dropoff')
      when regexp_substr(column_name, 'pu_') is not null then regexp_replace(column_name, 'pu_', 'pickup_')
      when regexp_substr(column_name, 'p_u') is not null then regexp_replace(column_name, 'p_u', 'pickup_')
      when regexp_substr(column_name, 'amt') is not null then regexp_replace(column_name, 'amt', 'amount')
      when regexp_substr(column_name, 'tpep|lpep') is not null then regexp_replace(column_name, '[tl]pep_', '')
      when regexp_substr(column_name, 'trip_') is not null 
        and regexp_substr(column_name, 'date') is not null 
        then regexp_replace(regexp_replace(column_name, 'trip_', ''), 'date_time$', 'datetime')
      else column_name end new_column_name
  , data_type old_data_type
from `pipeline-analysis-455005`.`nytaxi_raw`.`INFORMATION_SCHEMA.COLUMNS`
where column_name != '__index_level_0__'
and regexp_substr(table_name, 'external|mapping') is null
), 
t2 as 
(select 
  trip_type 
  , table_name
  , old_column_name 
  , new_column_name
  , old_data_type
  , case 
    when regexp_substr(new_column_name, 'datetime|date|creation_dt|clone_dt') is not null then 'TIMESTAMP'
    when regexp_substr(new_column_name, 'fee|amount|surcharge|tax|distance|longitude|latitude|fare') is not null then 'FLOAT64'
    when new_column_name = 'sr_flag' then 'INT64'
    when regexp_substr(new_column_name, 'data_source|flag|base_number|license_number') is not null then 'STRING'
    else 'INT64'
    end new_data_type
from t1
)
select 
  trip_type
  , table_name
  , old_column_name 
  , new_column_name 
  , old_data_type
  , new_data_type 
from t2;

