{#
  One row per district: the last 12 months against the 12 months before.
  Year-on-year change is only reported when both periods have enough sales to be
  meaningful (min_sales_for_yoy).
#}
{% set min_sales_for_yoy = 20 %}

with w as (select * from {{ ref('int_reporting_window') }}),

sales as (
    select
        s.*,
        case
            when s.month >= w.last_12m_start  then 'last_12m'
            when s.month >= w.prior_12m_start and s.month <= w.prior_12m_end then 'prior_12m'
        end as period
    from {{ ref('int_market_sales') }} s
    cross join w
),

by_period as (
    select
        district, county, period,
        count(*) as sales,
        {{ median('price') }} as median_price,
        cast(avg(case when is_new_build then 1 else 0 end) as double) as new_build_share,
        cast(avg(case when property_type_code = 'F' then 1 else 0 end) as double) as flat_share
    from sales
    where period is not null
    group by district, county, period
),

by_type as (
    select district, property_type_code, {{ median('price') }} as median_price
    from sales
    where period = 'last_12m'
    group by district, property_type_code
),

type_pivot as (
    select
        district,
        max(case when property_type_code = 'D' then median_price end) as median_detached,
        max(case when property_type_code = 'S' then median_price end) as median_semi,
        max(case when property_type_code = 'T' then median_price end) as median_terraced,
        max(case when property_type_code = 'F' then median_price end) as median_flat
    from by_type
    group by district
)

select
    cur.district,
    cur.county,
    w.as_of_month,
    cur.sales                          as sales_12m,
    cur.median_price                   as median_price_12m,
    prev.sales                         as sales_prior_12m,
    prev.median_price                  as median_price_prior_12m,
    case
        when cur.sales >= {{ min_sales_for_yoy }} and prev.sales >= {{ min_sales_for_yoy }}
        then round(cast(cur.median_price as double) / prev.median_price - 1, 4)
    end                                as median_price_yoy,
    round(cur.new_build_share, 4)      as new_build_share,
    round(cur.flat_share, 4)           as flat_share,
    t.median_detached,
    t.median_semi,
    t.median_terraced,
    t.median_flat
from by_period cur
cross join w
left join by_period prev
    on prev.district = cur.district and prev.period = 'prior_12m'
left join type_pivot t
    on t.district = cur.district
where cur.period = 'last_12m'
