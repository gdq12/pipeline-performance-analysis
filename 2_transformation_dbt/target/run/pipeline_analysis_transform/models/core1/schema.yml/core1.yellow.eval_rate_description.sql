select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        ratecode_description as value_field,
        count(*) as n_records

    from `pipeline-analysis-455005`.`nytaxi_core1`.`core1_yellow_fact_trips`
    group by ratecode_description

)

select *
from all_values
where value_field not in (
    'STANDARD RATE','JFK','NEWARK','NASSAU/WESTCHESTER','NEGOTIATED FARE','GROUP RIDE','UNKNOWN'
)



      
    ) dbt_internal_test