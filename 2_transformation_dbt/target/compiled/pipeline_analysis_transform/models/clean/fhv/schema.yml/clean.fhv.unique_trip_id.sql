
    
    

with dbt_test__target as (

  select trip_id as unique_field
  from `pipeline-analysis-455005`.`nytaxi_clean`.`fhv__4_adds_columns`
  where trip_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


