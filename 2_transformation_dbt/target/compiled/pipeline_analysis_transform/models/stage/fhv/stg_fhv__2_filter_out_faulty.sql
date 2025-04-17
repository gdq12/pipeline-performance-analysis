select 
    tbl.trip_type_start_date,
    tbl.data_source,
    tbl.pickup_date,
    tbl.trip_type_end_date,
    tbl.trip_type_source,
    tbl.trip_id,
    tbl.dispatching_base_number,
    tbl.pickup_datetime, 
    tbl.dropoff_datetime,
    tbl.pickup_location_id,
    tbl.dropoff_location_id,
    tbl.sr_flag, 
    tbl.affiliated_base_number,
    tbl.creation_dt, 
    tbl.clone_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__4_adds_columns` tbl
left join `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhv__1b_id_faulty_trips` ft1 on tbl.data_source = ft1.data_source 
                                                    and tbl.trip_id = ft1.trip_id
left join `pipeline-analysis-455005`.`nytaxi_stage`.`stg_fhv__1a_id_duplicate_records` ft2 on tbl.dispatching_base_number = ft2.dispatching_base_number
                                                            and tbl.pickup_datetime = ft2.pickup_datetime 
                                                            and tbl.dropoff_datetime = ft2.dropoff_datetime
                                                            and tbl.pickup_location_id = ft2.pickup_location_id
                                                            and tbl.dropoff_location_id = ft2.dropoff_location_id
                                                            and tbl.sr_flag = ft2.sr_flag 
                                                            and tbl.affiliated_base_number = ft2.affiliated_base_number
                                                            and ft2.row_count = 2
                                                            and tbl.trip_id = ft2.second_trip_id
where ft1.data_source is null and ft2.pickup_location_id is null