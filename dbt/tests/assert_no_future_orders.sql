-- Fails if any order is dated in the future (returns the offending rows)
with

orders as (
    select * from {{ ref('stg_shop__orders') }}
),

final as (
    select
        order_id,
        order_ts,
        order_date
    from orders
    where order_date > current_date()
)

select * from final
