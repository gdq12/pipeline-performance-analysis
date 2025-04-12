{#
    This macro determines id the trip timestamps are during nyc rush hour traffic times.
#}

{% macro get_rush_hour_status(datetime_col) -%}

    case 
        when extract(dayofweek from {{datetime_col}}) in (2, 3, 4, 5, 6)
            and extract(hour from {{datetime_col}}) in (6, 7, 8, 9, 10)
            then 'MORNING RUSH HOUR'
        when extract(dayofweek from {{datetime_col}}) in (2, 3, 4, 5, 6)
            and extract(hour from {{datetime_col}}) in (4, 5, 6, 7, 8)
            then 'MORNING RUSH HOUR'
        else null 
    end

{%- endmacro %}