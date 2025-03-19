{#
    This macro syncronizes vendor_id across all yellow_tripdata
#}

{% macro update_vendor_id(vendor_id) -%}

    case {{ dbt.safe_cast("vendor_id", api.Column.translate_type("string")) }}
        when 'CMT' then 1
        when '1' then 1
        when '2' then 2
        when 'VTS' then 3
        when 'DDS' then 4
        else null
    end

{%- endmacro %}