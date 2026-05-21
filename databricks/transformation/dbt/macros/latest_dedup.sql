{% macro latest_dedup(
    source_cte,
    partition_by,
    extraction_column = "_extraction_ts",
    use_latest_only_override = none,
    cte_context = none
) %}

{% if cte_context %}
    {% set latest_cte_name = 'latest_' ~ cte_context %}
    {% set dedup_cte_name = 'dedup_' ~ cte_context %}
{% else %}
    {% set latest_cte_name = 'latest' %}
    {% set dedup_cte_name = 'dedup' %}
{% endif %}

{% if use_latest_only_override is none %}
    {% set use_latest_only = not is_incremental() %}
{% else %}
    {% set use_latest_only = use_latest_only_override %}
{% endif %}

{% if use_latest_only %}
{{ latest_cte_name }} as (
    select b.*
    from {{ source_cte }} b
    where b.{{ extraction_column }} = (
        select max({{ extraction_column }})
        from {{ source_cte }}
    )
),
{% endif %}

{{ dedup_cte_name }} as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by {{ partition_by | join(', ') }}
                order by {{ extraction_column }} desc
            ) as rn
        from {% if use_latest_only %}{{ latest_cte_name }}{% else %}{{ source_cte }}{% endif %}
    )
    where rn = 1
)

{% endmacro %}


{% macro latest_only(
    source_cte,
    extraction_column = "_extraction_ts",
    use_latest_only_override = none,
    cte_context = none
) %}

{% if cte_context %}
    {% set latest_cte_name = 'latest_' ~ cte_context %}
{% else %}
    {% set latest_cte_name = 'latest' %}
{% endif %}

{% if use_latest_only_override is none %}
    {% set use_latest_only = not is_incremental() %}
{% else %}
    {% set use_latest_only = use_latest_only_override %}
{% endif %}

{{ latest_cte_name }} as (
    select b.*
    from {{ source_cte }} b
    {% if use_latest_only %}
    where b.{{ extraction_column }} = (
        select max({{ extraction_column }})
        from {{ source_cte }}
    )
    {% endif %}
)

{% endmacro %}
