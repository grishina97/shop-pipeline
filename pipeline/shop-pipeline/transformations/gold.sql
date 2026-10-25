-- Цель: каждый заказ + сумма в EUR (основа для всех отчётов)
CREATE OR REFRESH MATERIALIZED VIEW gold_orders_eur (
  CONSTRAINT rate_found EXPECT (rate_to_eur IS NOT NULL) ON VIOLATION FAIL UPDATE  -- нет курса → остановить, а не потерять выручку
) AS
SELECT
  o.order_id,
  DATE(o.order_ts)                              AS order_date,
  o.country, o.category, o.product_name, o.status, o.currency,
  o.quantity * o.unit_price                     AS amount,       -- сумма в валюте заказа
  r.rate_to_eur,
  ROUND(o.quantity * o.unit_price * r.rate_to_eur, 2) AS amount_eur  -- сумма в EUR
FROM silver_orders o
LEFT JOIN silver_rates r                        -- LEFT: если курса нет, строка останется с NULL → сработает CONSTRAINT
  ON o.currency = r.currency
 AND DATE(o.order_ts) = r.rate_date;            -- курс на день заказа

-- Цель: выручка по дням и странам (только оплаченные заказы)
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue AS
SELECT
  order_date,
  country,
  COUNT(*)                  AS orders,
  ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM gold_orders_eur
WHERE status = 'paid'                           -- cancelled / refunded не считаем выручкой
GROUP BY order_date, country;

-- Цель: выручка по категориям товаров
CREATE OR REFRESH MATERIALIZED VIEW gold_category_revenue AS
SELECT
  category,
  COUNT(*)                  AS orders,
  ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM gold_orders_eur
WHERE status = 'paid'
GROUP BY category;