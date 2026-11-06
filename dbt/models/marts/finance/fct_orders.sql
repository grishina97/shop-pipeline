with

orders as (
    select * from {{ ref('stg_shop__orders') }}
),

rates as (
    select * from {{ ref('int_rates_filled') }}
),

final as (
    select
        orders.order_id,
        orders.order_date,
        orders.customer_id,
        orders.country,
        orders.category,
        orders.product_name,
        orders.status,
        orders.currency,
        orders.quantity * orders.unit_price                          as amount,      -- order currency 
        rates.rate_to_eur,
        round(orders.quantity * orders.unit_price * rates.rate_to_eur, 2) as amount_eur,
        rates.is_filled                                              as is_rate_filled  -- currency filled during weekends 
    from orders
    left join rates
        on  orders.currency   = rates.currency
        and orders.order_date = rates.rate_date
    where orders.is_valid                                            -- business rule: only correct orders 
)

select * from final
