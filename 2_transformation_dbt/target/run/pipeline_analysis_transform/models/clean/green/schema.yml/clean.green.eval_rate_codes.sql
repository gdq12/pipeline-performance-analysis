select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        ratecode_id as value_field,
        count(*) as n_records

    from `pipeline-analysis-455005`.`nytaxi_clean`.`green__4_adds_columns`
    group by ratecode_id

)

select *
from all_values
where value_field not in (
    1,2,3,4,5,6,99
)



      
    ) dbt_internal_test