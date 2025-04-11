{#
    This macro syncronizes ratecode_id to the current data-type and values defined.
    It also considers location_ids in case the rate code is null.
#}

{% macro update_ratecode_id(ratecode_id, pickup_location_id, dropoff_location_id) -%}

    case 
        when ({{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} is null 
            or {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('99', '99.0')
            )
            and ({{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} = 132
            or {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} = 132)
            then 2
        when ({{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} is null 
            or {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('99', '99.0')
            )
            and ({{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} = 1
            or {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} = 1)
            then 3
        when ({{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} is null 
            or {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('99', '99.0')
            )
            and ({{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} = 265
            or {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} = 265)
            then 4
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('1', '1.0') then 1
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('2', '2.0') then 2
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('3', '3.0') then 3
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('4', '4.0') then 4
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('5', '5.0') then 5
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('6', '6.0') then 6
        when ({{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} is null 
            or {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} in ('99', '99.0')
            ) then 99
    end

{%- endmacro %}