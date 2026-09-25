{# Shared expressions so every gold model computes prices the same way. #}

{% macro median(col) -%}
    cast(round(percentile_cont(0.5) within group (order by {{ col }})) as bigint)
{%- endmacro %}

{% macro quantile(col, q) -%}
    cast(round(percentile_cont({{ q }}) within group (order by {{ col }})) as bigint)
{%- endmacro %}

{% macro months_before(date_expr, n) -%}
    cast({{ dbt.dateadd('month', -n, date_expr) }} as date)
{%- endmacro %}
