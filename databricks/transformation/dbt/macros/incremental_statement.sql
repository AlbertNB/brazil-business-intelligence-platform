{% macro incremental_statement(ts_col) %}
    {%- if is_incremental() -%}
        -- Incremental filter based on the maximum timestamp in the target table
        {{ ts_col }} > (
            select coalesce(max({{ ts_col }}), cast('1900-01-01' as timestamp))
            from {{ this }}
        )
    {%- else -%}
        -- Full refresh / non-incremental execution
        1 = 1
    {%- endif -%}
{% endmacro %}
