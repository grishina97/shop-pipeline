with

source as (
    select * from {{ source('shop', 'bronze_rates') }}
),

renamed as (
    select
        to_date(rate_date)        as rate_date,     -- string -> date
        currency,
        eur_to_currency,                            -- 1 EUR = X currency
        1.0 / eur_to_currency     as rate_to_eur,   -- 1 currency = Y EUR
        ingested_at
    from source
),

final as (
    select * from renamed
)

select * from final
