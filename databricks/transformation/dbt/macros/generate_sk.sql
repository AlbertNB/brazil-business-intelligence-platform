{% macro generate_sk(fields) %}
sha2(
    concat_ws(
        '|'
        {% for field in fields %}
        ,coalesce(cast({{ field }} as string), '__null__')
        {% endfor %}
    ),
    256
)
{% endmacro %}
