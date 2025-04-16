
    
    

with all_values as (

    select
        payment_type as value_field,
        count(*) as n_records

    from `pipeline-analysis-455005`.`nytaxi_mart2`.`mart2_dm_monthly_stats`
    group by payment_type

)

select *
from all_values
where value_field not in (
    'FLEX FARE TRIP','CREDIT CARD','CASH','NO CHARGE','DISPUTE','UNKNOWN','VOIDED TRIP'
)


