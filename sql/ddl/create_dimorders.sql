-- warehouse.dimorders (Order Dimension Table)
CREATE TABLE IF NOT EXISTS warehouse.dimorders (
    orderkey SERIAL PRIMARY KEY,
    orderid VARCHAR(20) NOT NULL,
    customerid VARCHAR(20) NOT NULL,
    orderdate DATE NOT NULL,
    orderstatus VARCHAR(50) NOT NULL
);

COMMENT ON TABLE warehouse.dimorders IS 'Order dimension for fact table joins';
