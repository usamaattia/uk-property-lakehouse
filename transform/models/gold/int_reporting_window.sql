{#
  The reporting window every gold model shares.

  Sales take weeks to months to be registered, so the newest months in PPD are always
  incomplete and their medians are biased (quick, simple sales register first). We end
  reports `registration_lag_months` before the latest sale we have seen.
#}
{{ config(materialized='ephemeral') }}

with latest as (
    select max(month) as max_month
    from {{ ref('silver_transactions') }}
    where not is_deleted
)

select
    {{ months_before('max_month', var('registration_lag_months')) }}       as as_of_month,
    {{ months_before('max_month', var('registration_lag_months') + 11) }}  as last_12m_start,
    {{ months_before('max_month', var('registration_lag_months') + 23) }}  as prior_12m_start,
    {{ months_before('max_month', var('registration_lag_months') + 12) }}  as prior_12m_end
from latest
