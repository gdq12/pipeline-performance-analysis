schema_yellow_dict2009 = {
    'vendor_name' : 'string', 
    'trip_pickup_date_time' : 'string',
    'trip_dropoff_date_time' : 'string',
    'passenger_count': 'int64', 
    'trip_distance ' : 'float64', 
    'start_lon': 'float64',
    'start_lat': 'float64',
    'rate_code' : 'float64', 
    'store_and_forward ' : 'float64', 
    'end_lon': 'float64',
    'end_lat': 'float64',
    'payment_type' : 'string', 
    'fare_amt' : 'float64', 
    'surcharge' : 'float64', 
    'mta_tax' : 'float64', 
    'tip_amt' : 'float64', 
    'tolls_amt' : 'float64', 
    'total_amt' : 'float64', 
    'trip_pickup_date': 'timestamp', 
    'data_source': 'string',
    'creation_dt': 'timestamp'
}

schema_yellow_dict2010 = {
    'vendor_id': 'string', 
    'tpep_pickup_datetime' : 'string',
    'tpep_dropoff_datetime' : 'string',
    'passenger_count': 'int64', 
    'trip_distance ' : 'float64', 
    'pickup_longitude': 'float64',
    'pickup_latitude': 'float64',
    'ratecode_id' : 'string', 
    'store_and_fwd_flag ' : 'string', 
    'dropoff_longitude': 'float64',
    'dropoff_latitude': 'float64',
    'payment_type' : 'string', 
    'fare_amount' : 'float64', 
    'surcharge' : 'float64', 
    'mta_tax' : 'float64', 
    'tip_amount' : 'float64', 
    'tolls_amount' : 'float64', 
    'total_amount' : 'float64', 
    'pickup_date': 'timestamp', 
    'data_source': 'string',
    'creation_dt': 'timestamp'
}

schema_yellow_dict = {
    'vendor_id' : 'int64', 
    'tpep_pickup_datetime' : 'timestamp',
    'tpep_dropoff_datetime' : 'timestamp',
    'passenger_count': 'float64', 
    'trip_distance ' : 'float64', 
    'ratecode_id' : 'float64', 
    'store_and_fwd_flag ' : 'string', 
    'pu_location_id' : 'int64', 
    'do_location_id' : 'int64', 
    'payment_type' : 'int64', 
    'fare_amount' : 'float64', 
    'extra' : 'float64', 
    'mta_tax' : 'float64', 
    'tip_amount' : 'float64', 
    'tolls_amount' : 'float64', 
    'improvement_surcharge' : 'float64', 
    'total_amount' : 'float64', 
    'congestion_surcharge' : 'float64', 
    'airport_fee' : 'int64',
    'tpep_pickup_date' : 'timestamp',
    'data_source': 'string',
    'creation_dt': 'timestamp'
}

schema_green_dict = {
    'vendor_id': 'int64', 
    'lpep_pickup_datetime': 'timestamp', 
    'lpep_dropoff_datetime': 'timestamp',
    'store_and_fwd_flag': 'string', 
    'ratecode_id': 'float64', 
    'pu_location_id': 'int64', 
    'do_location_id': 'int64',
    'passenger_count': 'float64', 
    'trip_distance': 'float64', 
    'fare_amount': 'float64', 
    'extra': 'float64', 
    'mta_tax': 'float64',
    'tip_amount': 'float64', 
    'tolls_amount': 'float64', 
    'ehail_fee': 'int64', 
    'improvement_surcharge': 'float64',
    'total_amount': 'float64', 
    'payment_type': 'float64', 
    'trip_type': 'float64', 
    'congestion_surcharge': 'float64', 
    'lpep_pickup_date': 'timestamp',
    'data_source': 'string',
    'creation_dt': 'timestamp'
}

schema_fhv_dict = {
    'dispatching_base_num': 'string', 
    'pickup_datetime': 'int64', 
    'drop_off_datetime': 'int64',
    'p_ulocation_id': 'float64', 
    'd_olocation_id': 'float64', 
    'sr_flag': 'int64',
    'affiliated_base_number': 'string',
    'pickup_date': 'timestamp', 
    'data_source': 'string',
    'creation_dt': 'timestamp'
}

schema_fhvhv_dict = {
    'hvfhs_license_num': 'string', 
    'dispatching_base_num': 'string', 
    'originating_base_num': 'string', 
    'request_datetime': 'int64', 
    'on_scene_datetime': 'int64', 
    'pickup_datetime': 'int64', 
    'dropoff_datetime': 'int64', 
    'pu_location_id': 'int64', 
    'do_location_id': 'int64', 
    'trip_miles': 'float64', 
    'trip_time': 'int64', 
    'base_passenger_fare': 'float64', 
    'tolls': 'float64', 
    'bcf': 'float64', 
    'sales_tax': 'float64', 
    'congestion_surcharge': 'float64', 
    'airport_fee': 'float64', 
    'tips': 'float64', 
    'driver_pay': 'float64', 
    'shared_request_flag': 'string', 
    'shared_match_flag': 'string', 
    'access_a_ride_flag': 'string', 
    'wav_request_flag': 'string', 
    'wav_match_flag': 'string',
    'pickup_date': 'timestamp',
    'data_source': 'string',
    'creation_dt': 'timestamp'
}

schema_log_dict = {
    'data_source': 'string',
    'table_name': 'string',
    'column_name': 'string', 
    'data_type': 'string',
    'ordinal_position': 'int64',
    'num_null': 'int64',
    'num_row': 'int64',
    'num_distinct_values': 'int64',
    'min_value': 'string',
    'max_value': 'string',
    'mean_value': 'string',
    'median_value': 'string',
    'mode_var': 'string',
    'mode_value': 'int64',
    'log_comment': 'string',
    'creation_dt': 'timestamp'
}

q_log_skeleton = """insert into `{}`.`nytaxi_raw`.`extract_load_log`
    ({})
    with t1 as 
    (SELECT column_name, data_type, ordinal_position
    FROM {}.INFORMATION_SCHEMA.COLUMNS
    where table_name = '{}'
    ),
    t2 as 
    (select 
      '{}' column_name
      , cast({} as string) mode_var
      , count(1) mode_value
    from `{}.{}.{}`
    {}
    group by cast({} as string)
    order by 3 desc
    limit 1
    )
    select 
      '{}' data_source
      , '{}.{}.{}' table_name
      , t1.column_name
      , t1.data_type
      , t1.ordinal_position
      , countif(tbl.{} is null) num_null
      , count(1) num_row
      , count(distinct tbl.{}) num_distinct_values
      , cast(min(tbl.{}) as string) min_value
      , cast(max(tbl.{}) as string) max_value
      , cast({} as string) mean_value
      , cast(approx_quantiles(tbl.{}, 2)[offset(1)] as string) median_value
      , t2.mode_var
      , t2.mode_value
      , cast(null as string) log_comment
      , current_timestamp() creation_dt
    from `{}.{}.{}` tbl
    join t1 t1 on t1.column_name = '{}'
    join t2 t2 on t1.column_name = t2.column_name
    {}
    group by t1.column_name, t1.data_type, t1.ordinal_position, t2.mode_var, t2.mode_value
"""

q_history = """insert into `{}.nytaxi_raw.query_history_extract_load_spark`
select 
creation_time, project_id, project_number, user_email, job_id, job_type, statement_type, priority, start_time, end_time, query, state, reservation_id, total_bytes_processed, total_slot_ms, error_result, cache_hit, destination_table, referenced_tables, labels, timeline, job_stages, total_bytes_billed, transaction_id, parent_job_id, session_info, dml_statistics, total_modified_partitions, bi_engine_statistics, query_info, transferred_bytes, materialized_view_statistics, edition, job_creation_reason, metadata_cache_statistics
, '{}' data_source
from region-eu.INFORMATION_SCHEMA.JOBS_BY_USER
where start_time >= '{}'
"""