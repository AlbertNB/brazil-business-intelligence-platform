{% macro rfb_company_size_id(col) %}
    coalesce(nullif(trim(cast({{ col }} as string)), ''), '00')
{% endmacro %}

{% macro rfb_company_size_description(col) %}
    case coalesce(nullif(trim(cast({{ col }} as string)), ''), '00')
        when '00' then 'NOT_INFORMED'
        when '01' then 'MICRO_COMPANY'
        when '03' then 'SMALL_COMPANY'
        when '05' then 'OTHER'
        else 'NOT_INFORMED'
    end
{% endmacro %}
