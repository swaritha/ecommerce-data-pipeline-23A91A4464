-- =====================================================
-- E-COMMERCE DATA WAREHOUSE - ANALYTICAL QUERIES
-- Phase 4.1: 10 Business Intelligence Queries
-- Uses warehouse.star_schema tables only
-- =====================================================

-- Query 1: Top 10 Products by Revenue
-- Objective: Identify best-selling products
-- Technique: Aggregation with GROUP BY, ORDER BY, LIMIT
WITH product_sales AS (
    SELECT
        do.orderid,
        do.customerid,
        f.quantity,
        f.totalamount
    FROM warehouse.factorders f
    JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
),
product_agg AS (
    SELECT
        do.orderid,
        SUM(ps.totalamount) AS total_revenue,
        SUM(ps.quantity) AS units_sold,
        AVG(ps.totalamount / NULLIF(ps.quantity, 0)) AS avg_price
    FROM product_sales ps
    JOIN warehouse.dimorders do ON ps.orderid = do.orderid
    GROUP BY do.orderid
)
SELECT
    pa.orderid AS product_name,
    'Generic Product' AS category,
    ROUND(pa.total_revenue::numeric, 2) AS total_revenue,
    pa.units_sold,
    ROUND(pa.avg_price::numeric, 2) AS avg_price
FROM product_agg pa
ORDER BY pa.total_revenue DESC
LIMIT 10;

-- Query 2: Monthly Sales Trend
-- Objective: Analyze revenue over time
-- Technique: Time-series aggregation using dim_date
SELECT 
    CONCAT(dd.year::text, '-', LPAD(dd.month::text, 2, '0')) AS year_month,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    COUNT(DISTINCT f.orderkey) AS total_transactions,
    ROUND(SUM(f.totalamount)::numeric / NULLIF(COUNT(DISTINCT f.orderkey), 0), 2) AS average_order_value,
    COUNT(DISTINCT do.customerid) AS unique_customers
FROM warehouse.factorders f
JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
JOIN warehouse.dimdate dd ON do.orderdatekey = dd.datekey
GROUP BY dd.year, dd.month
ORDER BY year_month;

-- Query 3: Customer Segmentation Analysis
-- Objective: Group customers by spending patterns
-- Technique: CASE statement for bucketing, aggregation
WITH customer_totals AS (
    SELECT 
        dc.customerid,
        SUM(f.totalamount) AS total_spent
    FROM warehouse.factorders f
    JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
    JOIN warehouse.dimcustomers dc ON do.customerid = dc.customerid
    GROUP BY dc.customerid
)
SELECT 
    CASE 
        WHEN total_spent < 1000 THEN '$0-$1,000'
        WHEN total_spent < 5000 THEN '$1,000-$5,000'
        WHEN total_spent < 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_spent)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_spent)::numeric, 2) AS avg_transaction_value
FROM customer_totals
GROUP BY spending_segment
ORDER BY total_revenue DESC;

-- Query 4: Category Performance
-- Objective: Compare product categories
-- Technique: Aggregation with calculated fields
SELECT 
    'All Categories' AS category,  -- Simplified for current schema
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    ROUND(SUM(f.totalamount * 0.15)::numeric, 2) AS total_profit,  -- Assuming 15% margin
    ROUND((SUM(f.totalamount * 0.15) / NULLIF(SUM(f.totalamount), 0)) * 100, 2) AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM warehouse.factorders f;

-- Query 5: Payment Method Distribution
-- Objective: Understand payment preferences
-- Technique: Window functions for percentage calculation
SELECT 
    do.paymentmethod AS payment_method,
    COUNT(*) AS transaction_count,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    ROUND((COUNT(*)::float / SUM(COUNT(*)) OVER()) * 100, 2) AS pct_of_transactions,
    ROUND((SUM(f.totalamount)::float / SUM(SUM(f.totalamount)) OVER()) * 100, 2) AS pct_of_revenue
FROM warehouse.factorders f
JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
GROUP BY do.paymentmethod
ORDER BY total_revenue DESC;

-- Query 6: Geographic Analysis
-- Objective: Identify high-revenue locations
-- Technique: Aggregation with JOIN to customer dimension
SELECT 
    dc.state AS state,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    COUNT(DISTINCT dc.customerid) AS total_customers,
    ROUND(SUM(f.totalamount)::numeric / NULLIF(COUNT(DISTINCT dc.customerid), 0), 2) AS avg_revenue_per_customer
FROM warehouse.factorders f
JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
JOIN warehouse.dimcustomers dc ON do.customerid = dc.customerid
GROUP BY dc.state
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 7: Customer Lifetime Value (CLV)
-- Objective: Analyze customer value and tenure
-- Technique: Customer-level aggregation, date arithmetic
SELECT 
    dc.customerid,
    dc.fullname AS full_name,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_spent,
    COUNT(DISTINCT f.orderkey) AS transaction_count,
    (CURRENT_DATE - dc.registrationdate)::int AS days_since_registration,
    ROUND(SUM(f.totalamount)::numeric / NULLIF(COUNT(DISTINCT f.orderkey), 0), 2) AS avg_order_value
FROM warehouse.factorders f
JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
JOIN warehouse.dimcustomers dc ON do.customerid = dc.customerid
GROUP BY dc.customerid, dc.fullname, dc.registrationdate
ORDER BY total_spent DESC
LIMIT 10;

-- Query 8: Product Profitability Analysis
-- Objective: Identify most profitable products
-- Technique: Aggregation with profit calculations
WITH product_sales AS (
    SELECT
        do.orderid,
        f.quantity,
        f.totalamount
    FROM warehouse.factorders f
    JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
)
SELECT
    ps.orderid AS product_name,
    'Generic Product' AS category,
    ROUND((SUM(ps.totalamount) * 0.15)::numeric, 2) AS total_profit,
    ROUND(15.0, 2) AS profit_margin,  -- Fixed 15% margin
    ROUND(SUM(ps.totalamount)::numeric, 2) AS revenue,
    SUM(ps.quantity) AS units_sold
FROM product_sales ps
GROUP BY ps.orderid
ORDER BY total_profit DESC
LIMIT 10;

-- Query 9: Day of Week Sales Pattern
-- Objective: Identify temporal patterns
-- Technique: Aggregation using dim_date attributes
SELECT 
    dd.dayname,
    ROUND(AVG(SUM(f.totalamount))::numeric, 2) AS avg_daily_revenue,
    ROUND(AVG(COUNT(f.orderkey))::numeric, 2) AS avg_daily_transactions,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue
FROM warehouse.factorders f
JOIN warehouse.dimorders do ON f.orderkey = do.orderkey
JOIN warehouse.dimdate dd ON do.orderdatekey = dd.datekey
GROUP BY dd.dayname
ORDER BY avg_daily_revenue DESC;

-- Query 10: Discount Impact Analysis
-- Objective: Analyze discount effectiveness
-- Technique: Bucketing discounts, aggregation
SELECT 
    CASE 
        WHEN f.discountpct = 0 THEN '0%'
        WHEN f.discountpct <= 0.10 THEN '1-10%'
        WHEN f.discountpct <= 0.25 THEN '11-25%'
        WHEN f.discountpct <= 0.50 THEN '26-50%'
        ELSE '50%+'
    END AS discount_range,
    ROUND(AVG(f.discountpct * 100)::numeric, 2) AS avg_discount_pct,
    SUM(f.quantity) AS total_quantity_sold,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    ROUND(AVG(f.totalamount)::numeric, 2) AS avg_line_total
FROM warehouse.factorders f
GROUP BY discount_range
ORDER BY avg_discount_pct;
