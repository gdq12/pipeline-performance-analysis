{#
    This macro provides the rate code description based on the input values.
#}

{% macro get_ratecode_description(ratecode_id) -%}

    case {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("integer")) }}
        when 1 then 'STANDARD RATE'
        when 2 then 'JFK'
        when 3 then 'NEWARK'
        when 4 then 'NASSAU/WESTCHESTER'
        when 5 then 'NEGOTIATED FARE'
        when 6 then 'GROUP RIDE'
        when 99 then 'UNKNOWN'
    end

{%- endmacro %}