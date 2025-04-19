
    
    

with child as (
    select pickup_zone as from_field
    from `pipeline-analysis-455005`.`nytaxi_core1`.`core1_green_fact_trips`
    where pickup_zone is not null
),

parent as (
    select zone as to_field
    from `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


