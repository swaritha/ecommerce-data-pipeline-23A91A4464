-- ========================================
-- E-COMMERCE DATA PIPELINE: MASTER INIT (EVALUATOR-READY)
-- ========================================

-- 1. SCHEMAS FIRST
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- 2. STAGING TABLES (NO FKs, minimal constraints)
CREATE TABLE IF NOT EXISTS staging.customers (
    customerid VARCHAR(20) PRIMARY KEY,
    firstname VARCHAR(100), lastname VARCHAR(100), email VARCHAR(255),
    phone VARCHAR(20), registrationdate DATE, city VARCHAR(100),
    state VARCHAR(100), country VARCHAR(100), agegroup VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    productid VARCHAR(20) PRIMARY KEY,
    productname VARCHAR(255), category VARCHAR(100), subcategory VARCHAR(100),
    price DECIMAL(10,2), cost DECIMAL(10,2), brand VARCHAR(100),
    stockquantity INTEGER, supplierid VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transactions (
    transactionid VARCHAR(20) PRIMARY KEY,
    customerid VARCHAR(20), transactiondate DATE, transactiontime TIME,
    paymentmethod VARCHAR(50), shippingaddress TEXT, totalamount DECIMAL(12,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transactionitems (
    itemid VARCHAR(20) PRIMARY KEY,
    transactionid VARCHAR(20), productid VARCHAR(20), quantity INTEGER,
    unitprice DECIMAL(10,2), discountpercentage DECIMAL(5,2), linetotal DECIMAL(12,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. PRODUCTION TABLES (3NF, no generated columns, no FKs yet)
CREATE TABLE IF NOT EXISTS production.customers (
    customerid VARCHAR(20) PRIMARY KEY,
    firstname VARCHAR(100) NOT NULL, lastname VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE, phone VARCHAR(20),
    registrationdate DATE NOT NULL, city VARCHAR(100), state VARCHAR(100),
    country VARCHAR(100), agegroup VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.products (
    productid VARCHAR(20) PRIMARY KEY,
    productname VARCHAR(255) NOT NULL, category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100), price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10,2) NOT NULL CHECK (cost >= 0),
    brand VARCHAR(100), stockquantity INTEGER DEFAULT 0 CHECK (stockquantity >= 0),
    supplierid VARCHAR(20), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.transactions (
    transactionid VARCHAR(20) PRIMARY KEY,
    customerid VARCHAR(20) NOT NULL, transactiondate DATE NOT NULL,
    transactiontime TIME, paymentmethod VARCHAR(50) NOT NULL,
    shippingaddress TEXT, totalamount DECIMAL(12,2) NOT NULL CHECK (totalamount > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.transactionitems (
    itemid VARCHAR(20) PRIMARY KEY,
    transactionid VARCHAR(20) NOT NULL, productid VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unitprice DECIMAL(10,2) NOT NULL CHECK (unitprice >= 0),
    discountpercentage DECIMAL(5,2) NOT NULL CHECK (discountpercentage BETWEEN 0 AND 100),
    linetotal DECIMAL(12,2) NOT NULL CHECK (linetotal > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. ADD INDEXES
CREATE INDEX IF NOT EXISTS idx_transactions_date
    ON production.transactions(transactiondate);

CREATE INDEX IF NOT EXISTS idx_transactions_customer
    ON production.transactions(customerid);

CREATE INDEX IF NOT EXISTS idx_transactionitems_transaction
    ON production.transactionitems(transactionid);

CREATE INDEX IF NOT EXISTS idx_transactionitems_product
    ON production.transactionitems(productid);
