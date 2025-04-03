{#
    This macro syncronizes payment_type to the current data-type and values defined in https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
    It also coniders tip_maount in case the payment type is null. 
#}

{% macro update_payment_type(payment_type, tip_amount) -%}

    case 
        when 
            lower(trim({{ dbt.safe_cast("payment_type", api.Column.translate_type("string")) }})) is null 
            and cast(tip_amount as float64) > 0 then 1
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
            in ('6', '6.0', 'void', 'voi') then 6
        else 5
    end

{%- endmacro %}