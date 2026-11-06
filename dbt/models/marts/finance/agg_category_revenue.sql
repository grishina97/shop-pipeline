with
orders as (
    select * from {{ ref('fct_orders') }}
),
final as (
    select
        category,
        count(*)                   as orders,
        round(sum(amount_eur), 2)  as revenue_eur
    from orders
    where status = 'paid'
    group by category
)
select * from final