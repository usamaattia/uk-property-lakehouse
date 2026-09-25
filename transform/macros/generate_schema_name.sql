{#
  Use the configured schema name as-is (bronze / silver / gold) instead of dbt's default
  "<target schema>_<custom schema>". On Databricks these live inside the target catalog.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
