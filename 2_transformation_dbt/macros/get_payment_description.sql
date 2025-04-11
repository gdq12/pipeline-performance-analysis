{#
    This macro provides the payment type description based on the input values.
#}

{% macro get_payment_description(payment_type) -%}

    case {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("integer")) }}
        when 0 then 'FLEX FARE TRIP'
        when 1 then 'CREDIT CARD'
        when 2 then 'CASH'
        when 3 then 'NO CHARGE'
        when 4 then 'DISPUTE'
        when 5 then 'UNKNOWN'
        when 6 then 'VOIDED TRIP'
    end

{%- endmacro %}