{% macro rfb_date(col) %}
    to_date(
        nullif(nullif(trim(cast({{ col }} as string)), '00000000'), '0'),
        'yyyyMMdd'
    )
{% endmacro %}
