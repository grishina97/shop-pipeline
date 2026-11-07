with

source as (
    select * from {{ source('shop', 'bronze_orders') }}
),

renamed as (
    select
        cast(order_id as bigint)        as order_id,
        to_timestamp(order_ts)          as order_ts,
        to_date(order_ts)               as order_date,      -- to join with the rates by day
        customer_id,
        country,
        product_id,
        product_name,
        category,
        cast(quantity as int)           as quantity,
        cast(unit_price as double)      as unit_price,
        currency,
        status,
        source_file,
        ingested_at
    from source
),

-- duplicates: one row per order_id, keep the latest loaded
deduplicated as (
    select *
    from renamed
    qualify row_number() over (
        partition by order_id
        order by ingested_at desc
    ) = 1
),

-- valid order: has a customer, positive quantity and positive unit price
final as (
    select
        *,
        customer_id 
          is not null
          and quantity > 0
          and unit_price > 0 as is_valid      -- flag
    from deduplicated
)

select * from final