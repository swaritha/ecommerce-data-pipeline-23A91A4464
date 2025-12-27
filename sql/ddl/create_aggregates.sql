CREATE TABLE IF NOT EXISTS warehouse.agg_daily_sales (
    datekey INTEGER PRIMARY KEY REFERENCES warehouse.dimdate(datekey),
    total_transactions INTEGER NOT NULL,
    total_revenue NUMERIC(14,2) NOT NULL,
    total_profit NUMERIC(14,2),
    unique_customers INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.agg_product_performance (
    productid VARCHAR(20) PRIMARY KEY,
    total_quantity_sold INTEGER NOT NULL,
    total_revenue NUMERIC(14,2) NOT NULL,
    avg_discount_percentage NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS warehouse.agg_customer_metrics (
    customerkey INTEGER PRIMARY KEY REFERENCES warehouse.dimcustomers(customerkey),
    total_transactions INTEGER NOT NULL,
    total_spent NUMERIC(14,2) NOT NULL,
    avg_order_value NUMERIC(14,2),
    last_purchase_date DATE
);
