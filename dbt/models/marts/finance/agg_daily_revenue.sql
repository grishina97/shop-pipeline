with
orders as (
    select * from {{ ref('fct_orders') }}
),
final as (
    select
        order_date,
        country,
        count(*)                   as orders,
        round(sum(amount_eur), 2)  as revenue_eur
    from orders
    where status = 'paid'                       -- cancelled / refunded are not revenue
    group by order_date, country
)
select * from final