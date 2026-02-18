-- =======================================================
-- FASHION STORE ANALYTICS
-- STAGING-ONLY VERSION (Q1–Q25)
-- PostgreSQL | EXECUTABLE END-TO-END
-- =======================================================

-- Tables used:
-- customers_stage(customer_id TEXT, country TEXT, signup_date TEXT)
-- products_stage(product_id TEXT, brand TEXT, category TEXT, cost_price TEXT)
-- sales_stage(sale_id TEXT, customer_id TEXT, sale_date TEXT, channel TEXT)
-- salesitems_stage(sale_id TEXT, product_id TEXT, quantity TEXT, unit_price TEXT)
ALTER TABLE salesitems_stage
ADD COLUMN IF NOT EXISTS discount NUMERIC DEFAULT 0;

ALTER TABLE sales_stage
ADD COLUMN IF NOT EXISTS campaign TEXT DEFAULT 'NA';


-- =======================================================
-- Q1 Total revenue, discount, items per customer
SELECT
  c.customer_id::INT,
  SUM(
    (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
  ) AS total_revenue,
  SUM(si.discount::NUMERIC) AS total_discount,
  SUM(si.quantity::INT) AS total_items
FROM customers_stage c
JOIN sales_stage s ON s.customer_id = c.customer_id
JOIN salesitems_stage si ON si.sale_id = s.sale_id
WHERE c.customer_id ~ '^[0-9]+$'
GROUP BY c.customer_id
ORDER BY total_revenue DESC;

-- =======================================================
-- Q2 Top 5 customers per country
WITH customer_revenue AS (
  SELECT
    c.country,
    c.customer_id::INT,
    SUM(si.quantity::INT * si.unit_price::NUMERIC) revenue
  FROM customers_stage c
  JOIN sales_stage s ON s.customer_id = c.customer_id
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY c.country, c.customer_id
)
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY country ORDER BY revenue DESC) rn
  FROM customer_revenue
) t
WHERE rn <= 5;

-- =======================================================
-- Q3 Top 3 products per brand per month
WITH product_monthly AS (
  SELECT
    p.brand,
    DATE_TRUNC('month', s.sale_date::date) AS month,
    p.product_id,
    SUM(si.quantity::int * si.unit_price::numeric) AS revenue
  FROM products_stage p
  JOIN salesitems_stage si ON si.product_id = p.product_id
  JOIN sales_stage s ON s.sale_id = si.sale_id
  GROUP BY
    p.brand,
    DATE_TRUNC('month', s.sale_date::date),
    p.product_id
)
SELECT *
FROM (
  SELECT *,
         RANK() OVER (PARTITION BY brand, month ORDER BY revenue DESC) rnk
  FROM product_monthly
) t
WHERE rnk <= 3;

-- =======================================================
-- Q4 Total profit per product (quantity × cost_price)
SELECT
  p.product_id::INT,
  SUM(
    (si.quantity::INT * si.unit_price::NUMERIC)
    - (si.quantity::INT * p.cost_price::NUMERIC)
  ) AS total_profit
FROM products_stage p
JOIN salesitems_stage si ON si.product_id = p.product_id
GROUP BY p.product_id;


-- =======================================================
-- Q5 Negative profit products + customers
SELECT
  s.customer_id::INT,
  si.product_id::INT,
  SUM(
    (si.quantity::INT * si.unit_price::NUMERIC)
    - (si.quantity::INT * p.cost_price::NUMERIC)
  ) AS profit
FROM salesitems_stage si
JOIN products_stage p ON p.product_id = si.product_id
JOIN sales_stage s ON s.sale_id = si.sale_id
GROUP BY s.customer_id, si.product_id
HAVING
  SUM(
    (si.quantity::INT * si.unit_price::NUMERIC)
    - (si.quantity::INT * p.cost_price::NUMERIC)
  ) < 0;

-- =======================================================
-- Q6 Avg profit margin per category & brand
SELECT
  p.category,
  p.brand,
  AVG(
    (
      (si.quantity::INT * si.unit_price::NUMERIC)
      - si.discount::NUMERIC
      - (si.quantity::INT * p.cost_price::NUMERIC)
    )
    /
    NULLIF(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC,
      0
    )
  ) AS avg_profit_margin
FROM products_stage p
JOIN salesitems_stage si ON si.product_id = p.product_id
GROUP BY p.category, p.brand;


-- =======================================================
-- Q7 Revenue & items per channel
SELECT
  s.channel,
  s.campaign,
  SUM(
    (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
  ) AS revenue,
  SUM(si.quantity::INT) AS total_items
FROM sales_stage s
JOIN salesitems_stage si ON si.sale_id = s.sale_id
GROUP BY s.channel, s.campaign;


-- =======================================================
-- Q8 Most effective channel (revenue / items)
WITH campaign_perf AS (
  SELECT
    s.channel,
    s.campaign,
    SUM(
  (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
) / NULLIF(SUM(si.quantity::INT), 0) AS effectiveness
  FROM sales_stage s
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY s.channel, s.campaign
)
SELECT *
FROM (
  SELECT *,
         RANK() OVER (
           PARTITION BY channel
           ORDER BY effectiveness DESC
         ) AS rnk
  FROM campaign_perf
) t
WHERE rnk = 1;

-- =======================================================
-- Q9 Rank channels per month
WITH channel_monthly AS (
  SELECT
    DATE_TRUNC('month', s.sale_date::DATE) AS sale_month,
    s.channel,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC)
      - si.discount::NUMERIC
    ) AS revenue
  FROM sales_stage s
  JOIN salesitems_stage si
    ON si.sale_id = s.sale_id
  GROUP BY
    DATE_TRUNC('month', s.sale_date::DATE),
    s.channel
)
SELECT
  sale_month,
  channel,
  revenue,
  RANK() OVER (
    PARTITION BY sale_month
    ORDER BY revenue DESC
  ) AS channel_rank
FROM channel_monthly
ORDER BY sale_month, channel_rank;

-- =======================================================
-- Q10 Monthly revenue growth (last 12 months)
WITH monthly_rev AS (
  SELECT
    DATE_TRUNC('month', s.sale_date::DATE) AS month,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
    ) AS revenue
  FROM sales_stage s
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY DATE_TRUNC('month', s.sale_date::DATE)
)
SELECT
  month,
  revenue,
  revenue - LAG(revenue) OVER (ORDER BY month) AS growth
FROM monthly_rev
ORDER BY month DESC
LIMIT 12;

-- =======================================================
-- Q11 New vs Returning customers
WITH base AS (
  SELECT
    DATE_TRUNC('month', s.sale_date::date) AS month,
    s.customer_id,
    c.signup_date::date
  FROM sales_stage s
  JOIN customers_stage c ON c.customer_id = s.customer_id
)
SELECT
  month,
  CASE
    WHEN DATE_TRUNC('month', signup_date) = month THEN 'New'
    ELSE 'Returning'
  END AS customer_type,
  COUNT(DISTINCT customer_id)
FROM base
GROUP BY month, customer_type;


-- =======================================================
-- Q12 Avg purchase frequency per cohort
WITH cohort AS (
  SELECT customer_id,
         DATE_TRUNC('month', signup_date::DATE) cohort_month
  FROM customers_stage
),
purchases AS (
  SELECT
    c.customer_id,
    cohort_month,
    COUNT(s.sale_id) purchase_count
  FROM cohort c
  JOIN sales_stage s ON s.customer_id = c.customer_id
  GROUP BY c.customer_id, cohort_month
)
SELECT
  cohort_month,
  AVG(purchase_count) avg_purchase_frequency
FROM purchases
GROUP BY cohort_month;

-- =======================================================
-- Q13 High / Medium / Low revenue customers
WITH ranked AS (
  SELECT
    c.country,
    c.customer_id::INT,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
    ) AS revenue,
    PERCENT_RANK() OVER (
      PARTITION BY c.country
      ORDER BY SUM(
        (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
      )
    ) pr
  FROM customers_stage c
  JOIN sales_stage s ON s.customer_id = c.customer_id
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY c.country, c.customer_id
)
SELECT *,
  CASE
    WHEN pr >= 0.66 THEN 'High'
    WHEN pr >= 0.33 THEN 'Medium'
    ELSE 'Low'
  END AS revenue_segment
FROM ranked;


-- =======================================================
-- Q14 VIP customers (all products in category)
SELECT
  s.customer_id::INT
FROM sales_stage s
JOIN salesitems_stage si ON si.sale_id = s.sale_id
JOIN products_stage p ON p.product_id = si.product_id
WHERE p.category = 'HighMargin'
GROUP BY s.customer_id
HAVING COUNT(DISTINCT p.product_id) =
  (
    SELECT COUNT(DISTINCT product_id)
    FROM products_stage
    WHERE category = 'HighMargin'
  );


-- =======================================================
-- Q15 Revenue per category + % contribution
WITH total_rev AS (
  SELECT
    SUM(
      quantity::INT * unit_price::NUMERIC
      - discount::NUMERIC
    ) AS total_revenue
  FROM salesitems_stage
)
SELECT
  p.category,
  SUM(
    si.quantity::INT * si.unit_price::NUMERIC
    - si.discount::NUMERIC
  ) AS discounted_revenue,
  ROUND(
  SUM(
    si.quantity::INT * si.unit_price::NUMERIC
    - si.discount::NUMERIC
  ) / NULLIF((SELECT total_revenue FROM total_rev), 0) * 100,
  2
) AS pct_contribution
FROM products_stage p
JOIN salesitems_stage si ON si.product_id = p.product_id
GROUP BY p.category;
-- =======================================================
-- Q16 Pareto 80% products per category
WITH prod_rev AS (
  SELECT
    p.category,
    p.product_id,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
    ) AS revenue
  FROM products_stage p
  JOIN salesitems_stage si ON si.product_id = p.product_id
  GROUP BY p.category, p.product_id
),
cum_rev AS (
  SELECT *,
    SUM(revenue) OVER (PARTITION BY category ORDER BY revenue DESC)
    / SUM(revenue) OVER (PARTITION BY category) AS cum_pct
  FROM prod_rev
)
SELECT *
FROM cum_rev
WHERE cum_pct <= 0.8;

-- =======================================================
-- Q17 Top 10% customers per country

WITH ranked AS (
  SELECT
    c.country,
    c.customer_id,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
    ) AS revenue,
    PERCENT_RANK() OVER (
      PARTITION BY c.country
      ORDER BY SUM(
        (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
      )
    ) pr
  FROM customers_stage c
  JOIN sales_stage s ON s.customer_id = c.customer_id
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY c.country, c.customer_id
)
SELECT *
FROM ranked
WHERE pr >= 0.9;


-- =======================================================
-- Q18 Customers with ≥20% MoM quantity growth
WITH RECURSIVE growth AS (

  -- Anchor query (first month per customer)
  SELECT
    customer_id,
    month,
    qty,
    NULL::BIGINT AS prev_qty
  FROM (
    SELECT
      s.customer_id,
      DATE_TRUNC('month', s.sale_date::DATE) AS month,
      SUM(si.quantity::INT)::BIGINT AS qty
    FROM sales_stage s
    JOIN salesitems_stage si ON si.sale_id = s.sale_id
    GROUP BY s.customer_id, DATE_TRUNC('month', s.sale_date::DATE)
  ) base
  WHERE month = (
    SELECT MIN(month)
    FROM (
      SELECT DATE_TRUNC('month', sale_date::DATE) AS month
      FROM sales_stage
    ) m
  )

  UNION ALL

  -- Recursive part
  SELECT
    m.customer_id,
    m.month,
    m.qty,
    g.qty AS prev_qty
  FROM (
    SELECT
      s.customer_id,
      DATE_TRUNC('month', s.sale_date::DATE) AS month,
      SUM(si.quantity::INT)::BIGINT AS qty
    FROM sales_stage s
    JOIN salesitems_stage si ON si.sale_id = s.sale_id
    GROUP BY s.customer_id, DATE_TRUNC('month', s.sale_date::DATE)
  ) m
  JOIN growth g
    ON m.customer_id = g.customer_id
   AND m.month = g.month + INTERVAL '1 month'
)

SELECT *
FROM growth
WHERE prev_qty IS NOT NULL
  AND qty >= prev_qty * 1.2;


-- =======================================================
-- Q19 Rank products by category per month
WITH product_cat_month AS (
  SELECT
    p.category,
    DATE_TRUNC('month', s.sale_date::date) AS month,
    p.product_id,
    SUM(si.quantity::int * si.unit_price::numeric) AS revenue
  FROM products_stage p
  JOIN salesitems_stage si ON si.product_id = p.product_id
  JOIN sales_stage s ON s.sale_id = si.sale_id
  GROUP BY
    p.category,
    DATE_TRUNC('month', s.sale_date::date),
    p.product_id
)
SELECT *,
       RANK() OVER (
         PARTITION BY category, month
         ORDER BY revenue DESC
       ) AS rank
FROM product_cat_month;

-- =======================================================
-- Q20 Cumulative spend & MoM growth
WITH daily_rev AS (
  SELECT
    s.customer_id,
    s.sale_date::DATE,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
    ) AS revenue
  FROM sales_stage s
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY s.customer_id, s.sale_date
)
SELECT
  customer_id,
  sale_date,
  SUM(revenue) OVER (PARTITION BY customer_id ORDER BY sale_date)
    AS cumulative_spend,
  revenue - LAG(revenue) OVER (PARTITION BY customer_id ORDER BY sale_date)
    AS mom_growth
FROM daily_rev;


-- =======================================================
-- Q21 3-month moving average revenue per product
SELECT
  product_id,
  sale_month,
  AVG(revenue) OVER (
    PARTITION BY product_id
    ORDER BY sale_month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS moving_avg
FROM (
  SELECT
    si.product_id::INT AS product_id,
    DATE_TRUNC('month', s.sale_date::DATE) AS sale_month,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
    ) AS revenue
  FROM sales_stage s
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY si.product_id, DATE_TRUNC('month', s.sale_date::DATE)
) t;

-- =======================================================
-- Q22 Customers buying >3 categories in last 6 months
SELECT
  s.customer_id
FROM sales_stage s
JOIN salesitems_stage si ON si.sale_id = s.sale_id
JOIN products_stage p ON p.product_id = si.product_id
WHERE s.sale_date::DATE >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY s.customer_id
HAVING COUNT(DISTINCT p.category) > 3;

-- =======================================================
-- Q23 Products sold to ≥5 customers per month
SELECT
  si.product_id::INT AS product_id,
  DATE_TRUNC('month', s.sale_date::DATE) AS sale_month
FROM sales_stage s
JOIN salesitems_stage si
  ON si.sale_id = s.sale_id
GROUP BY
  si.product_id,
  DATE_TRUNC('month', s.sale_date::DATE)
HAVING
  COUNT(DISTINCT s.customer_id) >= 5
  AND SUM((si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC) > 10000;



-- =======================================================
-- Q24 Top-selling product per channel
SELECT *
FROM (
  SELECT
    s.channel,
    s.campaign,
    si.product_id,
    SUM(si.quantity::INT) AS total_qty,
    SUM(
      (si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC
    ) AS revenue,
    RANK() OVER (
  PARTITION BY s.channel, s.campaign
  ORDER BY
    SUM((si.quantity::INT * si.unit_price::NUMERIC) - si.discount::NUMERIC) DESC
) AS rnk
  FROM sales_stage s
  JOIN salesitems_stage si ON si.sale_id = s.sale_id
  GROUP BY s.channel, s.campaign, si.product_id
) t
WHERE rnk = 1;

-- =======================================================
-- Q25 Customers buying >1 product per brand
SELECT
  p.brand,
  s.customer_id,
  COUNT(DISTINCT p.product_id) AS product_count,
  SUM(
    si.quantity::INT * si.unit_price::NUMERIC
    - si.discount::NUMERIC
  ) AS revenue,
  SUM(si.discount::NUMERIC) AS total_discount
FROM products_stage p
JOIN salesitems_stage si ON si.product_id = p.product_id
JOIN sales_stage s ON s.sale_id = si.sale_id
GROUP BY p.brand, s.customer_id
HAVING COUNT(DISTINCT p.product_id) > 1;

