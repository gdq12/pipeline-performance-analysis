select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        payment_description as value_field,
        count(*) as n_records

    from `pipeline-analysis-455005`.`nytaxi_core1`.`core1_yellow_fact_trips`
    group by payment_description

)

select *
from all_values
where value_field not in (
    'FLEX FARE TRIP','CREDIT CARD','CASH','NO CHARGE','DISPUTE','UNKNOWN','VOIDED TRIP'
)



      
    ) dbt_internal_test