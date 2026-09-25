{#
  How much more new builds sell for than existing homes, per county, last 12 months.
  Compared within each property type first, so a county building mostly flats is not
  mistaken for one with cheap new builds (a mix effect). The headline premium is the
  sales-weighted average of the per-type premiums.
#}
{% set min_sales = 10 %}

with w as (select * from {{ ref('int_reporting_window') }}),

by_type as (
    select
        county,
        property_type_code,
        count(case when is_new_build then 1 end)            as new_sales,
        count(case when not is_new_build then 1 end)        as existing_sales,
        cast(round(percentile_cont(0.5) within group (order by case when is_new_build then price end)) as bigint)     as median_new,
        cast(round(percentile_cont(0.5) within group (order by case when not is_new_build then price end)) as bigint) as median_existing
    from {{ ref('int_market_sales') }} s
    cross join w
    where s.month >= w.last_12m_start
    group by county, property_type_code
),

comparable as (
    select *, cast(median_new as double) / median_existing - 1 as premium
    from by_type
    where new_sales >= {{ min_sales }} and existing_sales >= {{ min_sales }}
)

select
    county,
    cast(sum(new_sales) as bigint)                                    as new_sales_12m,
    cast(sum(existing_sales) as bigint)                               as existing_sales_12m,
    round(sum(premium * new_sales) / sum(new_sales), 4)               as new_build_premium,
    count(*)                                                          as property_types_compared
from comparable
group by county
