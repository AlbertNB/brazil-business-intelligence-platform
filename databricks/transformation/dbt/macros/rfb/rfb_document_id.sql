{% macro rfb_document_id(col) %}
    nullif(trim(cast({{ col }} as string)), '***000000**')
{% endmacro %}
