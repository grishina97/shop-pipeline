with
rates as (
    select * from {{ ref('stg_shop__rates') }}
),

-- 1. calendar: every date from the first rate date to today
calendar as (
    select explode(sequence(min(rate_date), current_date(), interval 1 day)) as rate_date
    from rates
),

-- all currencies that appear in the rates
currencies as (
    select distinct currency from rates
),

-- 2. grid: every date x every currency
date_currency as (
    select calendar.rate_date, currencies.currency
    from calendar
    cross join currencies
),

-- 3. join the rates; on weekends and holidays the rate is NULL
joined as (
    select
        date_currency.rate_date,
        date_currency.currency,
        rates.rate_to_eur,
        rates.rate_date as published_date          -- date the rate was actually published
    from date_currency
    left join rates
        on  date_currency.rate_date = rates.rate_date
        and date_currency.currency  = rates.currency
),

-- 4. fill the gaps with the last known rate (forward fill)
filled as (
    select
        rate_date,
        currency,
        last_value(rate_to_eur, true) over (           -- true = ignore NULLs
            partition by currency                      -- separately for each currency
            order by rate_date
            rows between unbounded preceding and current row
        ) as rate_to_eur,
        last_value(published_date, true) over (
            partition by currency
            order by rate_date
            rows between unbounded preceding and current row
        ) as rate_published_date,                      -- which day the rate comes from
        rate_to_eur is null as is_filled               -- true = rate was filled in (weekend or holiday)
    from joined
),

-- 5. EUR to EUR is always 1, for every day
eur as (
    select
        rate_date,
        'EUR'      as currency,
        1.0        as rate_to_eur,
        rate_date  as rate_published_date,
        false      as is_filled
    from calendar
),

final as (
    select * from filled
    union all
    select * from eur
)

select * from final