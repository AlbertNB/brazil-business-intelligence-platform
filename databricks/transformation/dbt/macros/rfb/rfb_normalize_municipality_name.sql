{% macro rfb_normalize_municipality_name(col) %}
    trim(
        translate(
            upper(cast({{ col }} as string)),
            "ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ-'",
            "AAAAAEEEEIIIIOOOOOUUUUC "
        )
    )
{% endmacro %}
