-- Goal: every order + amount in EUR (basis for all reports)
CREATE OR REFRESH MATERIALIZED VIEW gold_orders_eur (
  CONSTRAINT rate_found EXPECT (rate_to_eur IS NOT NULL) ON VIOLATION FAIL UPDATE  -- no rate -> stop, don't lose revenue
) AS
SELECT
  o.order_id,
  DATE(o.order_ts)                              AS order_date,
  o.country, o.category, o.product_name, o.status, o.currency,
  o.quantity * o.unit_price                     AS amount,       -- amount in order currency
  r.rate_to_eur,
  ROUND(o.quantity * o.unit_price * r.rate_to_eur, 2) AS amount_eur  -- amount in EUR
FROM silver_orders o
LEFT JOIN silver_rates r                        -- LEFT: if no rate, row stays with NULL -> CONSTRAINT fires
  ON o.currency = r.currency
 AND DATE(o.order_ts) = r.rate_date;            -- rate on the order day

-- Goal: revenue by day and country (paid orders only)
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue AS
SELECT
  order_date,
  country,
  COUNT(*)                  AS orders,
  ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM gold_orders_eur
WHERE status = 'paid'                           -- cancelled / refunded don't count as revenue
GROUP BY order_date, country;

-- Goal: revenue by product category
CREATE OR REFRESH MATERIALIZED VIEW gold_category_revenue AS
SELECT
  category,
  COUNT(*)                  AS orders,
  ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM gold_orders_eur
WHERE status = 'paid'
GROUP BY category;