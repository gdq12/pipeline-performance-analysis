
    
    

with child as (
    select dropoff_location_id as from_field
    from `pipeline-analysis-455005`.`nytaxi_core2`.`core2_dm_daily_stats`
    where dropoff_location_id is not null
),

parent as (
    select location_id as to_field
    from `pipeline-analysis-455005`.`nytaxi_mapping`.`taxi_zone_lookup`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


