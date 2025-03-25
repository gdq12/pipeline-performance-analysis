{#
    This macro syncronizes payment_type across all yellow_tripdata
#}

{% macro update_payment_type(payment_type) -%}

    case 
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) 
            in ('cash', 'csh', 'cas') then 2
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) 
            in ('credit', 'cre', 'crd') then 1
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) 
            in ('dispute', 'dis') then 4
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }}))
            in ('no', 'no charge', 'noc') then 3
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }}))
            in ('na', '0') then 5
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }}))
            is null then 5
        else null 
    end

{%- endmacro %}