select 
  trp.vendor_id,
  trp.pickup_datetime,
  trp.dropoff_datetime,
  trp.passenger_count,
  trp.trip_distance,
  pu.zone_id pickup_location_id,
  trp.ratecode_id,
  trp.store_and_fwd_flag,
  du.zone_id dropoff_location_id,
  trp.payment_type,
  trp.fare_amount,
  trp.mta_tax,
  trp.tip_amount,
  trp.tolls_amount,
  trp.total_amount,
  trp.congestion_surcharge,
  trp.pickup_date,
  trp.data_source,
  trp.creation_dt
from `pipeline-analysis-455005`.`nytaxi_clean`.`yellow__2a_2009_2010_tbl_collation` trp
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_geom` pu on (ST_DWithin(pu.zone_geom,ST_GeogPoint(trp.pickup_longitude, trp.pickup_latitude), 0))
join `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_geom` du on (ST_DWithin(du.zone_geom,ST_GeogPoint(trp.dropoff_longitude, trp.dropoff_latitude), 0))
where trp.pickup_longitude between -180 and 180
and trp.pickup_latitude between -90 and 90
and trp.dropoff_longitude between -180 and 180
and trp.dropoff_latitude between -90 and 90