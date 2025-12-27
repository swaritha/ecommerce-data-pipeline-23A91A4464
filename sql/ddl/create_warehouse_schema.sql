-- 1. Create warehouse schema
CREATE SCHEMA IF NOT EXISTS warehouse;

-- 2. Customer dimension (SCD Type 2-ready)
CREATE TABLE IF NOT EXISTS warehouse.dimcustomers (
    customerkey     SERIAL PRIMARY KEY,          -- surrogate key
    customerid      VARCHAR(20) NOT NULL,        -- business key from production.customers
    firstname       VARCHAR(100),
    lastname        VARCHAR(100),
    fullname        VARCHAR(205),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    agegroup        VARCHAR(50),
    customersegment VARCHAR(50),                 -- later: New / Regular / VIP
    effectivedate   DATE NOT NULL,
    enddate         DATE,
    iscurrent       BOOLEAN NOT NULL DEFAULT TRUE
);
