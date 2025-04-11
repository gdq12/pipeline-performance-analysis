{#
    This macro syncronizes vendor_id across all yellow_tripdata.
#}

{% macro update_vendor_id(vendor_id) -%}

    case {{ dbt.safe_cast("vendor_id", api.Column.translate_type("string")) }}
        when '1' then 1
        when '2' then 2
        when '3' then 3
        when '4' then 4
        when '5' then 5
        when '6' then 6
        when '7' then 7
        when 'CMT' then 1
        when 'VTS' then 8
        when 'DDS' then 9
        else null
    end

{%- endmacro %}