{#
    This macro cleans up sr_flag according to the latest definition.
    Since only entry `1` is defined in the dictionary, all other values are converted to null.
#}

{% macro update_sr_flag(sr_flag) -%}

    case {{ dbt.safe_cast("sr_flag", api.Column.translate_type("string")) }}
        when '1.0' then 1
        when '1' then 1
        else null
    end

{%- endmacro %}