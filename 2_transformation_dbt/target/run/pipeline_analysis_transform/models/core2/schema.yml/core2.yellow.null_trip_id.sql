select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trip_id
from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_yellow_fact_trips`
where trip_id is null



      
    ) dbt_internal_test