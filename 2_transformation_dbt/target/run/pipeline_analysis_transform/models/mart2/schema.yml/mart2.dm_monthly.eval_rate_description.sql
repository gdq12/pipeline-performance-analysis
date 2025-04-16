select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        applied_rate as value_field,
        count(*) as n_records

    from `pipeline-analysis-455005`.`nytaxi_mart2`.`mart2_dm_monthly_stats`
    group by applied_rate

)

select *
from all_values
where value_field not in (
    'STANDARD RATE','JFK','NEWARK','NASSAU/WESTCHESTER','NEGOTIATED FARE','GROUP RIDE','UNKNOWN'
)



      
    ) dbt_internal_test