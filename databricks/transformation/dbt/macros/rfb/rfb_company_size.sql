{% macro rfb_company_size_id(col) %}
    nullif(trim(cast({{ col }} as string)), '')
{% endmacro %}

{% macro rfb_company_size_description(col) %}
    case nullif(trim(cast({{ col }} as string)), '')
        when '00' then 'NOT_INFORMED'
        when '01' then 'MICRO_COMPANY'
        when '03' then 'SMALL_COMPANY'
        when '05' then 'OTHER'
        else null
    end
{% endmacro %}
