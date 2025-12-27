text
# 🏗️ Architecture Documentation

## **Data Flow** (Staging → Production → Warehouse)

RAW CSVs ──TRUNCATE+LOAD──> staging.customers/products/transactions/transaction_items
│

CLEANSE+RULES ──TRUNCATE+LOAD──> production.customers/products (3NF)
│

DIMENSIONAL MODEL ──MERGE+INSERT──> warehouse.dim_* + fact_sales (Star Schema)
│

ANALYTICS ──SQL QUERIES──> 11 CSV reports (data/processed/analytics/)

text

## **Design Decisions**

**Why 3 Schemas?**
staging: Raw CSV replica (no transformation risk)
production: 3NF normalized (business transactions)
warehouse: Star schema (BI analytics optimized)

text

**Why TRUNCATE+LOAD?**
- Idempotent (safe re-runs)
- Simple (no UPSERT complexity)
- Fast (bulk operations)

## **Table Relationships**

production.transactions → FK: production.customers(customerid)
production.transaction_items → FK: production.transactions(transactionid), production.products(productid)

warehouse.fact_sales → FK: dim_customers, dim_products, dim_dates, dim_payment_methods

text

## **Database Schemas Detail**

### **Staging Schema** (Raw replica)
staging.customers (exact CSV structure + loaded_at)
staging.products (exact CSV structure + loaded_at)
staging.transactions (exact CSV structure + loaded_at)
staging.transaction_items (exact CSV structure + loaded_at)

text

### **Production Schema** (3NF Normalized)
production.customers (PK: customerid, cleansed data)
production.products (PK: productid, NOT NULL: productname)
production.transactions (FK: customerid, paymentmethod validated)
production.transaction_items (FK: transactionid, productid)

text

### **Warehouse Schema** (Star Schema)
warehouse.dim_customers (SCD Type 2, customer segments)
warehouse.dim_products (product hierarchy, categories)
warehouse.dim_dates (date dimensions)
warehouse.dim_payment_methods
warehouse.fact_sales (grain: transaction_item, revenue/profit)

text

## **Key Metrics Generated**
1000 customers - 500 products - 10K transactions - 29K line items

Electronics: 45% revenue - VIP customers: 35% revenue

Weekend peak: +28% vs weekdays - Profit margin: 25%+

text

**Author:** Sai Kiran Ramayanam | **Roll:** 23A91A4451