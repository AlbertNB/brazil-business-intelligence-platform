{% macro rfb_cnpj_dv_is_valid(col) %}
(
    -- First check digit (position 13): weights [5,4,3,2,9,8,7,6,5,4,3,2]
    cast(substr({{ col }}, 13, 1) as int) =
    case
        when (
            cast(substr({{ col }},  1, 1) as int) * 5 +
            cast(substr({{ col }},  2, 1) as int) * 4 +
            cast(substr({{ col }},  3, 1) as int) * 3 +
            cast(substr({{ col }},  4, 1) as int) * 2 +
            cast(substr({{ col }},  5, 1) as int) * 9 +
            cast(substr({{ col }},  6, 1) as int) * 8 +
            cast(substr({{ col }},  7, 1) as int) * 7 +
            cast(substr({{ col }},  8, 1) as int) * 6 +
            cast(substr({{ col }},  9, 1) as int) * 5 +
            cast(substr({{ col }}, 10, 1) as int) * 4 +
            cast(substr({{ col }}, 11, 1) as int) * 3 +
            cast(substr({{ col }}, 12, 1) as int) * 2
        ) % 11 < 2 then 0
        else 11 - (
            cast(substr({{ col }},  1, 1) as int) * 5 +
            cast(substr({{ col }},  2, 1) as int) * 4 +
            cast(substr({{ col }},  3, 1) as int) * 3 +
            cast(substr({{ col }},  4, 1) as int) * 2 +
            cast(substr({{ col }},  5, 1) as int) * 9 +
            cast(substr({{ col }},  6, 1) as int) * 8 +
            cast(substr({{ col }},  7, 1) as int) * 7 +
            cast(substr({{ col }},  8, 1) as int) * 6 +
            cast(substr({{ col }},  9, 1) as int) * 5 +
            cast(substr({{ col }}, 10, 1) as int) * 4 +
            cast(substr({{ col }}, 11, 1) as int) * 3 +
            cast(substr({{ col }}, 12, 1) as int) * 2
        ) % 11
    end
    and
    -- Second check digit (position 14): weights [6,5,4,3,2,9,8,7,6,5,4,3,2]
    cast(substr({{ col }}, 14, 1) as int) =
    case
        when (
            cast(substr({{ col }},  1, 1) as int) * 6 +
            cast(substr({{ col }},  2, 1) as int) * 5 +
            cast(substr({{ col }},  3, 1) as int) * 4 +
            cast(substr({{ col }},  4, 1) as int) * 3 +
            cast(substr({{ col }},  5, 1) as int) * 2 +
            cast(substr({{ col }},  6, 1) as int) * 9 +
            cast(substr({{ col }},  7, 1) as int) * 8 +
            cast(substr({{ col }},  8, 1) as int) * 7 +
            cast(substr({{ col }},  9, 1) as int) * 6 +
            cast(substr({{ col }}, 10, 1) as int) * 5 +
            cast(substr({{ col }}, 11, 1) as int) * 4 +
            cast(substr({{ col }}, 12, 1) as int) * 3 +
            cast(substr({{ col }}, 13, 1) as int) * 2
        ) % 11 < 2 then 0
        else 11 - (
            cast(substr({{ col }},  1, 1) as int) * 6 +
            cast(substr({{ col }},  2, 1) as int) * 5 +
            cast(substr({{ col }},  3, 1) as int) * 4 +
            cast(substr({{ col }},  4, 1) as int) * 3 +
            cast(substr({{ col }},  5, 1) as int) * 2 +
            cast(substr({{ col }},  6, 1) as int) * 9 +
            cast(substr({{ col }},  7, 1) as int) * 8 +
            cast(substr({{ col }},  8, 1) as int) * 7 +
            cast(substr({{ col }},  9, 1) as int) * 6 +
            cast(substr({{ col }}, 10, 1) as int) * 5 +
            cast(substr({{ col }}, 11, 1) as int) * 4 +
            cast(substr({{ col }}, 12, 1) as int) * 3 +
            cast(substr({{ col }}, 13, 1) as int) * 2
        ) % 11
    end
)
{% endmacro %}
