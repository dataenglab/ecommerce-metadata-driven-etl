-- Q1: Net revenue by month and product category (EUR orders only)
SELECT d.year, d.month, p.category,
       COUNT(*)                   AS order_lines,
       SUM(f.quantity)            AS units,
       SUM(f.total_amount)        AS revenue_eur
FROM   dwh.fact_order_line f
JOIN   dwh.dim_product p ON p.product_sk = f.product_sk
JOIN   dwh.dim_date    d ON d.date_key   = f.date_key
WHERE  f.currency_code = 'EUR'
GROUP  BY d.year, d.month, p.category
ORDER  BY d.year, d.month, revenue_eur DESC;

-- Q2: Top 10 SKUs by revenue with category at order time vs. current category (SCD Type 2)
SELECT p.product_sku, p.category AS category_at_order, c.category AS category_now,
       SUM(f.total_amount) AS revenue_eur
FROM   dwh.fact_order_line f
JOIN   dwh.dim_product p ON p.product_sk  = f.product_sk
JOIN   dwh.dim_product c ON c.product_sku = p.product_sku AND c.is_current
WHERE  f.currency_code = 'EUR'
GROUP  BY p.product_sku, p.category, c.category
ORDER  BY revenue_eur DESC
LIMIT  10;

-- Q3: Successful charges net of successful refunds per gateway and month
SELECT d.year, d.month, f.gateway,
       SUM(CASE WHEN f.transaction_type = 'charge' THEN f.amount_minor
                ELSE -f.amount_minor END) / 100.0 AS net_amount
FROM   dwh.fact_payment f
JOIN   dwh.dim_date d ON d.date_key = f.date_key
WHERE  f.payment_status = 'succeeded'
GROUP  BY d.year, d.month, f.gateway
ORDER  BY d.year, d.month, f.gateway;

-- Q4: Monthly clickstream funnel: sessions with page_view vs. add_to_cart
SELECT d.year, d.month,
       COUNT(DISTINCT e.session_id)                                      AS sessions,
       COUNT(DISTINCT e.session_id) FILTER (WHERE e.event_type = 'add_to_cart') AS sessions_with_cart,
       ROUND(100.0 * COUNT(DISTINCT e.session_id) FILTER (WHERE e.event_type = 'add_to_cart')
             / COUNT(DISTINCT e.session_id), 1)                          AS cart_rate_pct
FROM   dwh.fact_clickstream_event e
JOIN   dwh.dim_date d ON d.date_key = e.date_key
GROUP  BY d.year, d.month
ORDER  BY d.year, d.month;
