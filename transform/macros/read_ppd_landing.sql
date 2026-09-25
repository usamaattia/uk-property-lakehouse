{#
  Read every raw PPD file in the landing zone as all-string columns, plus lineage columns.
  Same output on both engines; only the file-reading syntax differs (adapter dispatch).

  Expected layout: <landing_path>/ppd/load_id=<YYYYMMDDTHHMMSSZ>/<file>.csv
#}
{% macro read_ppd_landing(landing_path) -%}
    {{ return(adapter.dispatch('read_ppd_landing')(landing_path)) }}
{%- endmacro %}

{% macro ppd_column_names() -%}
    {{ return(['transaction_id', 'price', 'date_of_transfer', 'postcode', 'property_type',
               'old_new', 'duration', 'paon', 'saon', 'street', 'locality', 'town_city',
               'district', 'county', 'ppd_category', 'record_status']) }}
{%- endmacro %}

{% macro duckdb__read_ppd_landing(landing_path) -%}
    select
        {% for c in ppd_column_names() %}{{ c }},{% endfor %}
        filename as _source_file,
        regexp_extract(filename, 'load_id=([0-9TZ]+)', 1) as _load_id
    from read_csv(
        '{{ landing_path }}/ppd/load_id=*/*.csv',
        header = false,
        all_varchar = true,
        filename = true,
        quote = '"',
        escape = '"',
        columns = { {% for c in ppd_column_names() %}'{{ c }}': 'VARCHAR'{% if not loop.last %}, {% endif %}{% endfor %} }
    )
{%- endmacro %}

{% macro databricks__read_ppd_landing(landing_path) -%}
    select
        {% for c in ppd_column_names() %}{{ c }},{% endfor %}
        _metadata.file_path as _source_file,
        regexp_extract(_metadata.file_path, 'load_id=([0-9TZ]+)', 1) as _load_id
    from read_files(
        '{{ landing_path }}/ppd/',
        format => 'csv',
        header => false,
        pathGlobFilter => '*.csv',
        recursiveFileLookup => true,
        schema => '{% for c in ppd_column_names() %}{{ c }} string{% if not loop.last %}, {% endif %}{% endfor %}'
    )
{%- endmacro %}

{% macro spark__read_ppd_landing(landing_path) -%}
    {#- Plain Spark: needs the temp view created by spark_register_landing() (a pre-hook). -#}
    select
        {% for c in ppd_column_names() %}{{ c }},{% endfor %}
        input_file_name() as _source_file,
        regexp_extract(input_file_name(), 'load_id=([0-9TZ]+)', 1) as _load_id
    from ppd_landing_raw
{%- endmacro %}

{% macro spark_register_landing(landing_path) -%}
    {%- if target.type == 'spark' -%}
    create or replace temporary view ppd_landing_raw
        ({% for c in ppd_column_names() %}{{ c }} string{% if not loop.last %}, {% endif %}{% endfor %})
    using csv
    options (path '{{ landing_path }}/ppd/load_id=*/*.csv', header 'false', quote '"', escape '"')
    {%- else -%}
    select 1
    {%- endif -%}
{%- endmacro %}
