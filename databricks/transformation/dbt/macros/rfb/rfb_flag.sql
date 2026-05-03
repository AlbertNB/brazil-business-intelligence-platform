{% macro rfb_flag(col) %}
    trim(cast({{ col }} as string)) = 'S'
{% endmacro %}
