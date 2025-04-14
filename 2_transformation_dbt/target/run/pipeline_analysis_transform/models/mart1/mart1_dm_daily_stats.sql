-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into `pipeline-analysis-455005`.`nytaxi_mart1`.`mart1_dm_daily_stats` as DBT_INTERNAL_DEST
        using (
        select
        * from `pipeline-analysis-455005`.`nytaxi_mart1`.`mart1_dm_daily_stats__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.pickup_date = DBT_INTERNAL_DEST.pickup_date
                ) and (
                    DBT_INTERNAL_SOURCE.trip_type = DBT_INTERNAL_DEST.trip_type
                )

    
    when matched then update set
        `pickup_borough` = DBT_INTERNAL_SOURCE.`pickup_borough`,`pickup_zone` = DBT_INTERNAL_SOURCE.`pickup_zone`,`pickup_service_zone` = DBT_INTERNAL_SOURCE.`pickup_service_zone`,`dropoff_borough` = DBT_INTERNAL_SOURCE.`dropoff_borough`,`dropoff_zone` = DBT_INTERNAL_SOURCE.`dropoff_zone`,`dropoff_service_zone` = DBT_INTERNAL_SOURCE.`dropoff_service_zone`,`pickup_date` = DBT_INTERNAL_SOURCE.`pickup_date`,`pickup_public_holiday` = DBT_INTERNAL_SOURCE.`pickup_public_holiday`,`payment_description` = DBT_INTERNAL_SOURCE.`payment_description`,`trip_type` = DBT_INTERNAL_SOURCE.`trip_type`,`hvfs_description` = DBT_INTERNAL_SOURCE.`hvfs_description`,`avg_trip_distance` = DBT_INTERNAL_SOURCE.`avg_trip_distance`,`fare_amount` = DBT_INTERNAL_SOURCE.`fare_amount`,`tip_amount` = DBT_INTERNAL_SOURCE.`tip_amount`,`total_amount` = DBT_INTERNAL_SOURCE.`total_amount`,`total_fees` = DBT_INTERNAL_SOURCE.`total_fees`,`passenger_count` = DBT_INTERNAL_SOURCE.`passenger_count`,`num_trips` = DBT_INTERNAL_SOURCE.`num_trips`,`transformation_dt` = DBT_INTERNAL_SOURCE.`transformation_dt`
    

    when not matched then insert
        (`pickup_borough`, `pickup_zone`, `pickup_service_zone`, `dropoff_borough`, `dropoff_zone`, `dropoff_service_zone`, `pickup_date`, `pickup_public_holiday`, `payment_description`, `trip_type`, `hvfs_description`, `avg_trip_distance`, `fare_amount`, `tip_amount`, `total_amount`, `total_fees`, `passenger_count`, `num_trips`, `transformation_dt`)
    values
        (`pickup_borough`, `pickup_zone`, `pickup_service_zone`, `dropoff_borough`, `dropoff_zone`, `dropoff_service_zone`, `pickup_date`, `pickup_public_holiday`, `payment_description`, `trip_type`, `hvfs_description`, `avg_trip_distance`, `fare_amount`, `tip_amount`, `total_amount`, `total_fees`, `passenger_count`, `num_trips`, `transformation_dt`)


    