{# England & Wales monthly headline series, by property type ('ALL' = every type). #}

with by_type as (
    select
        month, property_type_code, property_type,
        count(*) as sales,
        {{ median('price') }} as median_price,
        cast(round(avg(price)) as bigint) as mean_price,
        cast(avg(case when is_new_build then 1 else 0 end) as double) as new_build_share
    from {{ ref('int_market_sales') }}
    group by month, property_type_code, property_type
),

all_types as (
    select
        month, 'ALL' as property_type_code, 'All types' as property_type,
        count(*) as sales,
        {{ median('price') }} as median_price,
        cast(round(avg(price)) as bigint) as mean_price,
        cast(avg(case when is_new_build then 1 else 0 end) as double) as new_build_share
    from {{ ref('int_market_sales') }}
    group by month
)

select * from by_type
union all
select * from all_types
