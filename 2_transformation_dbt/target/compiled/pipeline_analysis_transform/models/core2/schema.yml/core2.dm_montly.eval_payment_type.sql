
    
    

with all_values as (

    select
        payment_type as value_field,
        count(*) as n_records

    from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_dm_monthly_stats`
    group by payment_type

)

select *
from all_values
where value_field not in (
    1,2,3,4,5,6
)


