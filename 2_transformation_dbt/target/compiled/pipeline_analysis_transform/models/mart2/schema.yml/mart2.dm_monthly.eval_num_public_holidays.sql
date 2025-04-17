

with meet_condition as(
  select *
  from `pipeline-analysis-455005`.`nytaxi_mart2`.`mart2_dm_monthly_stats`
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not num_public_holidays >= 0
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not num_public_holidays <= 31
)

select *
from validation_errors

