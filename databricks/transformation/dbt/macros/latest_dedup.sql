{% macro latest_dedup(
    source_cte,
    partition_by,
    extraction_column = "_extraction"
) %}

{% set use_latest_only = not is_incremental() %}

{% if use_latest_only %}
latest as (
    select b.*
    from {{ source_cte }} b
    where b.{{ extraction_column }} = (
        select max({{ extraction_column }})
        from {{ source_cte }}
    )
),
{% endif %}

dedup as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by {{ partition_by | join(', ') }}
                order by {{ extraction_column }} desc
            ) as rn
        from {% if use_latest_only %}latest{% else %}{{ source_cte }}{% endif %}
    )
    where rn = 1
)

{% endmacro %}
