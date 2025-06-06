select 
    tbl.trip_type_start_date,
    tbl.data_source,
    tbl.pickup_date,
    tbl.trip_type_end_date,
    tbl.trip_type_source,
    tbl.trip_id,
    tbl.vendor_id,
    tbl.pickup_datetime,
    tbl.dropoff_datetime,
    tbl.store_and_fwd_flag,
    tbl.ratecode_id,
    tbl.pickup_location_id,
    tbl.dropoff_location_id,
    tbl.passenger_count,
    tbl.trip_distance,
    tbl.fare_amount,
    tbl.extra_amount,
    tbl.mta_tax,
    tbl.tip_amount,
    tbl.tolls_amount,
    tbl.ehail_fee,
    tbl.improvement_surcharge,
    tbl.total_amount,
    tbl.payment_type,
    tbl.trip_type,
    tbl.congestion_surcharge,
    tbl.creation_dt,
    tbl.clone_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`green__4_adds_columns` tbl
left join `pipeline-analysis-455005`.`nytaxi_stage`.`stg_green__1b_id_faulty_trips` ft1 on tbl.data_source = ft1.data_source 
                                                    and tbl.trip_id = ft1.trip_id
left join `pipeline-analysis-455005`.`nytaxi_stage`.`stg_green__1a_id_duplicate_records` ft2 on tbl.pickup_datetime = ft2.pickup_datetime
                                                        and tbl.dropoff_datetime = ft2.dropoff_datetime
                                                        and coalesce(tbl.ratecode_id, 0) = coalesce(ft2.ratecode_id, 0)
                                                        and tbl.pickup_location_id = ft2.pickup_location_id 
                                                        and tbl.dropoff_location_id = ft2.dropoff_location_id 
                                                        and tbl.passenger_count = ft2.passenger_count
                                                        and tbl.trip_distance = ft2.trip_distance 
                                                        and ft2.row_count = 2
                                                        and ft2.total_fare_amount = 0
                                                        and tbl.fare_amount < 0
where ft1.data_source is null and ft2.pickup_location_id is null