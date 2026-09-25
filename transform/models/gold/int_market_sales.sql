{#
  The sales that count for market statistics, following ONS / UK HPI practice:
  standard (category A) sales only, deleted records removed, and nothing after the
  reporting window ends.
#}
{{ config(materialized='ephemeral') }}

select s.*
from {{ ref('silver_transactions') }} s
cross join {{ ref('int_reporting_window') }} w
where not s.is_deleted
  and s.ppd_category = 'A'
  and s.price is not null
  and s.month <= w.as_of_month
