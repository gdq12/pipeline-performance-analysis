

select 
  table_name
  , column_name old_column_name
  , case
      when column_name in ('start_lon', 'pickup_longitude') then 'pickup_longitude'
      when column_name in ('start_lat', 'pickup_latitude') then 'pickup_latitude'
      when column_name in ('end_lon', 'dropoff_longitude') then 'dropoff_longitude'
      when column_name in ('end_lat', 'dropoff_latitude') then 'dropoff_latitide'
      when column_name = 'store_and_forward' then 'store_and_fwd_flag'
      when column_name = 'vendor_name' then 'vendor_id'
      when regexp_substr(column_name, 'do_') is not null then regexp_replace(column_name, 'do_', 'dropoff_')
      when regexp_substr(column_name, 'pu_') is not null then regexp_replace(column_name, 'pu_', 'pickup_')
      when regexp_substr(column_name, 'amt') is not null then regexp_replace(column_name, 'amt', 'amount')
      when regexp_substr(column_name, 'tpep') is not null then regexp_replace(column_name, 'tpep_', '')
      when regexp_substr(column_name, 'trip_') is not null and regexp_substr(column_name, 'date') is not null then regexp_replace(regexp_replace(column_name, 'trip_', ''), 'date_time$', 'datetime')
      else column_name end new_column_name
  , ordinal_position
from `pipeline-analysis-452722`.`nytaxi_stage`.`INFORMATION_SCHEMA.COLUMNS`
where column_name != '__index_level_0__'
order by 1, 4