
    
    

with all_values as (

    select
        ratecode_id as value_field,
        count(*) as n_records

    from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_green_fact_trips`
    group by ratecode_id

)

select *
from all_values
where value_field not in (
    1,2,3,4,5,6,99
)


