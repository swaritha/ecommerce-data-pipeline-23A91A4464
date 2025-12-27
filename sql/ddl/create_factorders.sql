-- factorders (Central fact table for analytics)
CREATE TABLE IF NOT EXISTS warehouse.factorders (
    orderkey INTEGER REFERENCES warehouse.dimorders(orderkey),
    customerkey INTEGER REFERENCES warehouse.dimcustomers(customerkey),
    datekey INTEGER REFERENCES warehouse.dimdate(datekey),
    quantity INTEGER NOT NULL,
    unitprice DECIMAL(10,2) NOT NULL,
    totalamount DECIMAL(10,2) NOT NULL,
    orderstatus VARCHAR(50) NOT NULL
);

COMMENT ON TABLE warehouse.factorders IS 'Fact table for order analytics';
