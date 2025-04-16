select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select dropoff_zone as from_field
    from `pipeline-analysis-455005`.`nytaxi_core1`.`core1_yellow_fact_trips`
    where dropoff_zone is not null
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



      
    ) dbt_internal_test