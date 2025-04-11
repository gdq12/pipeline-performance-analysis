{#
    This macro syncronizes store_and_fwd_flag across all yellow_tripdata.
#}

{% macro update_store_and_fwd_flag(store_and_fwd_flag) -%}

    case 
        when trim({{ dbt.safe_cast("store_and_fwd_flag", api.Column.translate_type("string")) }})
            in ('0', '0.0', 'N') then 'N'
        when trim({{ dbt.safe_cast("store_and_fwd_flag", api.Column.translate_type("string")) }})
            in ('1', '1.0', 'Y') then 'Y'
        else null 
    end

{%- endmacro %}