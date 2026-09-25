{# Generic data tests, kept local so the project needs no external packages. #}

{% test positive(model, column_name) %}
    select * from {{ model }} where {{ column_name }} <= 0
{% endtest %}

{% test not_after_load_date(model, column_name) %}
    {# A sale cannot be dated after the file that reported it was landed. #}
    select * from {{ model }}
    where {{ column_name }} > cast(
        substr(_load_id, 1, 4) || '-' || substr(_load_id, 5, 2) || '-' || substr(_load_id, 7, 2)
        as date)
{% endtest %}

{% test row_count_between(model, min_rows, max_rows=none) %}
    select count(*) as n from {{ model }}
    having count(*) < {{ min_rows }}
    {% if max_rows is not none %} or count(*) > {{ max_rows }} {% endif %}
{% endtest %}

{% test unique_combination(model, columns) %}
    {# Same as dbt_utils.unique_combination_of_columns, without the package dependency. #}
    select {{ columns | join(', ') }}, count(*) as n
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}
