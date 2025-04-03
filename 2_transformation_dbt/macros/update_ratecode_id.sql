{#
    This macro syncronizes ratecode_id to the current data-type and values defined in https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
    It also considers location_ids in case the rate code is null.
#}

{% macro update_ratecode_id(ratecode_id, pickup_location_id, dropoff_location_id) -%}

    case 
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} is null 
            and ({{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} = 132
            or {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} = 132)
            then 2
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} is null 
            and ({{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} = 1
            or {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} = 1)
            then 3
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} is null 
            and ({{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} = 265
            or {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} = 265)
            then 4
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '1' then 1
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '1.0' then 1
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '2' then 2
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '2.0' then 2
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '3' then 3
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '3.0' then 3
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '4' then 4
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '4.0' then 4
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '5' then 5
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '5.0' then 5
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '6' then 6
        when {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }} = '6.0' then 6
        else null
    end

{%- endmacro %}