{#
  BRONZE: raw Price Paid Data, one row per line of every landed file.
  - Everything is kept as a string, exactly as delivered (no parsing, no filtering), so
    silver can be rebuilt at any time without re-downloading.
  - Append-only: each run picks up only load_ids newer than anything already loaded.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    pre_hook="{{ spark_register_landing(var('landing_path')) }}",
) }}

with landed as (
    {{ read_ppd_landing(var('landing_path')) }}
)

select
    *,
    {{ dbt.current_timestamp() }} as _ingested_at
from landed
{% if is_incremental() %}
where _load_id > (select coalesce(max(_load_id), '') from {{ this }})
{% endif %}
