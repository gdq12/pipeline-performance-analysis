{#
    This macro syncronizes ratecode_id across all yellow_tripdata
#}

{% macro update_ratecode_id(ratecode_id) -%}

    case {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("string")) }}
        when '1' then 1
        when '1.0' then 1
        when '2' then 2
        when '2.0' then 2
        when '3' then 3
        when '3.0' then 3
        when '4' then 4
        when '4.0' then 4
        when '5' then 5
        when '5.0' then 5
        when '6' then 6
        when '6.0' then 6
        else null
    end

{%- endmacro %}