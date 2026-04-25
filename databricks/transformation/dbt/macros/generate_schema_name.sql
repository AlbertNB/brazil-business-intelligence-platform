{% macro generate_schema_name(custom_schema_name, node) -%}
  {# Databricks + Unity Catalog: do NOT prefix target.schema #}
  {%- if custom_schema_name is none or custom_schema_name|trim == '' -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name|trim }}
  {%- endif -%}
{%- endmacro %}
