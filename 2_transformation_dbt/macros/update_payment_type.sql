{#
    This macro syncronizes payment_type to the current data-type and values defined.
    It also coniders tip_maount in case the payment type is null. 
#}

{% macro update_payment_type(payment_type, tip_amount) -%}

    case 
        when 
            lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) is null 
            and cast(tip_amount as float64) > 0 then 1
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) 
            in ('cash', 'csh', 'cas', '2', '2.0') then 2
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) 
            in ('credit', 'cre', 'crd', '1', '1.0') then 1
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) 
            in ('dispute', 'dis', '4', '4.0') then 4
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }}))
            in ('no', 'no charge', 'noc', '3', '3.0') then 3
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }}))
            in ('na', '0', '5', '5.0') then 5
        when lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }}))
            in ('void', 'voi', '6', '6.0') then 6
        else 5
    end

{%- endmacro %}