{# Monthly price statistics per local authority district and property type ('ALL' = every type). #}

with by_type as (
    select
        month, district, county, property_type_code, property_type,
        count(*)                 as sales,
        {{ median('price') }}    as median_price,
        {{ quantile('price', 0.25) }} as p25_price,
        {{ quantile('price', 0.75) }} as p75_price,
        cast(round(avg(price)) as bigint) as mean_price
    from {{ ref('int_market_sales') }}
    group by month, district, county, property_type_code, property_type
),

all_types as (
    select
        month, district, county, 'ALL' as property_type_code, 'All types' as property_type,
        count(*)                 as sales,
        {{ median('price') }}    as median_price,
        {{ quantile('price', 0.25) }} as p25_price,
        {{ quantile('price', 0.75) }} as p75_price,
        cast(round(avg(price)) as bigint) as mean_price
    from {{ ref('int_market_sales') }}
    group by month, district, county
)

select * from by_type
union all
select * from all_types
