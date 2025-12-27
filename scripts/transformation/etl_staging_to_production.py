import os
import yaml
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from typing import Dict, Any
from datetime import datetime

CONFIG_PATH = os.path.join("config", "config.yaml")
LOGS_DIR = "logs"

def setup_logging() -> logging.Logger:
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(LOGS_DIR, f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                       handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
    return logging.getLogger(__name__)

def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)
    
def get_engine(config: dict) -> sqlalchemy.Engine:
    db = config["database"]
    url = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    print("USING DB URL:", url)
    return sqlalchemy.create_engine(url)


def transform_customers(engine: sqlalchemy.Engine, logger: logging.Logger) -> Dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE production.customers RESTART IDENTITY"))
        # LOAD ALL customers - minimal filtering
        sql = text("""
        INSERT INTO production.customers (
            customerid, firstname, lastname, email, phone, 
            registrationdate, city, state, country, agegroup,
            created_at, updated_at
        )
        SELECT TRIM(customerid), TRIM(firstname), TRIM(lastname),
            LOWER(TRIM(email)), TRIM(phone), registrationdate::DATE,
            TRIM(city), TRIM(state), TRIM(country), agegroup,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM staging.customers
        WHERE customerid IS NOT NULL;
        """)
        result = conn.execute(sql)
        rows = result.rowcount
        logger.info(f"Loaded {rows} customers to production")
        return {"customers": rows}

def transform_products(engine: sqlalchemy.Engine, logger: logging.Logger) -> Dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE production.products RESTART IDENTITY"))
        sql = text("""
        INSERT INTO production.products (
            productid, productname, category, subcategory,
            price, cost, brand, stockquantity, supplierid,
            created_at, updated_at
        )
        SELECT TRIM(productid), TRIM(productname), TRIM(category),
            COALESCE(TRIM(subcategory), 'Unknown'), price::NUMERIC(10,2), 
            cost::NUMERIC(10,2), TRIM(brand), GREATEST(stockquantity::INTEGER, 0), 
            TRIM(supplierid), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM staging.products
        WHERE productid IS NOT NULL;
        """)
        result = conn.execute(sql)
        rows = result.rowcount
        logger.info(f"Loaded {rows} products to production")
        return {"products": rows}

def transform_transactions(engine: sqlalchemy.Engine, logger: logging.Logger) -> Dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE production.transactions RESTART IDENTITY"))
        sql = text("""
        INSERT INTO production.transactions (
            transactionid, customerid, transactiondate, transactiontime,
            paymentmethod, shippingaddress, totalamount,
            created_at, updated_at
        )
        SELECT TRIM(transactionid), TRIM(customerid), transactiondate::DATE, 
               transactiontime::TIME, UPPER(TRIM(paymentmethod)), 
               TRIM(shippingaddress), totalamount::NUMERIC(12,2),
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM staging.transactions
        WHERE totalamount > 0;
        """)
        result = conn.execute(sql)
        rows = result.rowcount
        logger.info(f"Loaded {rows} transactions to production")
        return {"transactions": rows}


def transform_transactionitems(engine: sqlalchemy.Engine, logger: logging.Logger) -> Dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE production.transactionitems RESTART IDENTITY"))

        sql = text("""
        INSERT INTO production.transactionitems (
            itemid, transactionid, productid, quantity,
            unitprice, discountpercentage, linetotal,
            created_at, updated_at
        )
        SELECT 
            TRIM(itemid),
            TRIM(transactionid),
            TRIM(productid),
            GREATEST(quantity::INTEGER, 1),
            unitprice::NUMERIC(10,2),
            GREATEST(LEAST(discountpercentage::NUMERIC, 100), 0),
            GREATEST(linetotal::NUMERIC(12,2), 0.01),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM staging.transactionitems;
        """)
        result = conn.execute(sql)
        rows = result.rowcount
        logger.info(f"Loaded {rows} transaction items to production")
        return {"transactionitems": rows}

def main():
    logger = setup_logging()
    config = load_config()
    engine = get_engine(config)
    
    start_time = pd.Timestamp.now()
    results = {}
    
    try:
        results.update(transform_customers(engine, logger))
        results.update(transform_products(engine, logger))
        results.update(transform_transactions(engine, logger))
        results.update(transform_transactionitems(engine, logger))
        
        end_time = pd.Timestamp.now()
        report = {
            "timestamp": end_time.isoformat(),
            "execution_time_seconds": (end_time - start_time).total_seconds(),
            "tables_loaded": results,
            "status": "success"
        }
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        report = {"status": "failed", "error": str(e)}
    
    os.makedirs("data/processed", exist_ok=True)
    pd.Series([report]).to_json("data/processed/etl_report.json", orient="records")
    logger.info(f"ETL report: data/processed/etl_report.json")
    print("ETL complete. Check data/processed/etl_report.json")

if __name__ == "__main__":
    main()
