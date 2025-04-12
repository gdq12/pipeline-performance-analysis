{#
    This macro converts the numerical weekday value to weekday string name.
#}

{% macro get_weekday_name(datetime_col) -%}

    case 
        when extract(dayofweek from {{datetime_col}}) = 1
            then 'SUNDAY'
        when extract(dayofweek from {{datetime_col}}) = 2
            then 'MONDAY'
        when extract(dayofweek from {{datetime_col}}) = 3
            then 'TUESDAY'
        when extract(dayofweek from {{datetime_col}}) = 4
            then 'WEDNESDAY'
        when extract(dayofweek from {{datetime_col}}) = 5
            then 'THURSDAY'
        when extract(dayofweek from {{datetime_col}}) = 6
            then 'FRIDAY'
        when extract(dayofweek from {{datetime_col}}) = 7
            then 'SATURDAY'
    end

{%- endmacro %}