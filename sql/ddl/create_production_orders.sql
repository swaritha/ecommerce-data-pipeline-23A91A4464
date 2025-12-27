-- production.orders (Source data for warehouse.dimorders)
CREATE TABLE IF NOT EXISTS production.orders (
    orderid VARCHAR(20) PRIMARY KEY,
    customerid VARCHAR(20) NOT NULL,
    orderdate DATE NOT NULL,
    orderstatus VARCHAR(50) NOT NULL
);

-- Generate 5000 sample orders linked to customers
INSERT INTO production.orders (orderid, customerid, orderdate, orderstatus)
SELECT 
    'ORD' || LPAD(row_num::text, 4, '0'),
    'CUST' || LPAD((row_num % 1000 + 1)::text, 4, '0'),
    DATE '2024-01-01' + (row_num % 365) * INTERVAL '1 day',
    CASE (row_num % 5)
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Processing' 
        WHEN 2 THEN 'Shipped'
        WHEN 3 THEN 'Delivered'
        ELSE 'Cancelled'
    END
FROM generate_series(1, 5000) AS t(row_num)
ON CONFLICT (orderid) DO NOTHING;

COMMENT ON TABLE production.orders IS 'Production orders source table for warehouse ETL';
