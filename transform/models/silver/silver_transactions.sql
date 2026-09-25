{#
  SILVER: one clean, typed row per transaction. The current state of the register.

  Price Paid Data is published as change records:
    A = a new transaction   C = a correction to an earlier one   D = it has been removed
  so this model is a CDC merge keyed on transaction_id:
    1. take only bronze loads newer than what silver already holds
    2. keep the latest version of each transaction within that batch
    3. MERGE (Databricks) / DELETE+INSERT (DuckDB) on transaction_id
  Deletes are kept as soft deletes (is_deleted = true) so history stays auditable; gold
  models filter them out.
#}
{#- Delta MERGE INTO on Databricks. Plain Spark + Parquet has no MERGE, so the `spark`
    target (a CI compatibility check, always --full-refresh) just appends. -#}
{%- set strategy = {'databricks': 'merge', 'duckdb': 'delete+insert', 'spark': 'append'}[target.type] -%}

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy=strategy,
    on_schema_change='fail',
) }}

with batch as (
    select * from {{ ref('bronze_ppd') }}
    {% if is_incremental() %}
    where _load_id > (select coalesce(max(_load_id), '') from {{ this }})
    {% endif %}
),

typed as (
    select
        upper(trim(replace(replace(transaction_id, '{', ''), '}', '')))  as transaction_id,
        try_cast(price as bigint)                                        as price,
        try_cast(substr(trim(date_of_transfer), 1, 10) as date)          as date_of_transfer,
        nullif(upper(trim(postcode)), '')                                as postcode,
        upper(trim(property_type))                                       as property_type_code,
        upper(trim(old_new)) = 'Y'                                       as is_new_build,
        upper(trim(duration))                                            as tenure_code,
        nullif(trim(paon), '')                                           as paon,
        nullif(trim(saon), '')                                           as saon,
        nullif(trim(street), '')                                         as street,
        nullif(trim(locality), '')                                       as locality,
        nullif(trim(town_city), '')                                      as town_city,
        upper(trim(district))                                            as district,
        upper(trim(county))                                              as county,
        upper(trim(ppd_category))                                        as ppd_category,
        upper(trim(record_status))                                       as record_status,
        _source_file,
        _load_id
    from batch
),

latest as (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by _load_id desc, _source_file desc
        ) as _version_rank
    from typed
)

select
    l.transaction_id,
    l.price,
    l.date_of_transfer,
    cast(date_trunc('month', l.date_of_transfer) as date)   as month,
    l.postcode,
    split_part(l.postcode, ' ', 1)                           as postcode_district,
    regexp_extract(l.postcode, '^([A-Z]{1,2})', 1)           as postcode_area,
    l.property_type_code,
    pt.label                                                 as property_type,
    l.is_new_build,
    l.tenure_code,
    tn.label                                                 as tenure,
    l.paon,
    l.saon,
    l.street,
    l.locality,
    l.town_city,
    l.district,
    l.county,
    l.ppd_category,
    l.record_status,
    l.record_status = 'D'                                    as is_deleted,
    l._source_file like '%synthetic-%'                       as is_synthetic,
    l._source_file,
    l._load_id
from latest l
left join {{ ref('ref_property_type') }} pt on pt.code = l.property_type_code
left join {{ ref('ref_tenure') }} tn on tn.code = l.tenure_code
where l._version_rank = 1
