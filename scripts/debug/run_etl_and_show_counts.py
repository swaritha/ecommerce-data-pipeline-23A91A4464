import os
import yaml
import logging
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

CONFIG_PATH = os.path.join("config", "config.yaml")
LOGS_DIR = "logs"

# ---------- shared helpers ----------

def setup_logging() -> logging.Logger:
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(
        LOGS_DIR, f"etl_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)

def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_engine(config: dict) -> sqlalchemy.Engine:
    db = config["database"]
    url = (
        f"postgresql+psycopg2://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['name']}"
    )
    print("USING DB URL:", url)
    return sqlalchemy.create_engine(url)

# ---------- your existing ETL steps, but using a passed connection ----------

def run_staging_to_production(conn, logger):
    # customers
    conn.execute(text("TRUNCATE production.customers RESTART IDENTITY"))
    res = conn.execute(text("""
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
    """))
    logger.info("Loaded %s customers", res.rowcount)

    # products
    conn.execute(text("TRUNCATE production.products RESTART IDENTITY"))
    res = conn.execute(text("""
        INSERT INTO production.products (
            productid, productname, category, subcategory,
            price, cost, brand, stockquantity, supplierid,
            created_at, updated_at
        )
        SELECT TRIM(productid), TRIM(productname), TRIM(category),
               COALESCE(TRIM(subcategory), 'Unknown'),
               price::NUMERIC(10,2), cost::NUMERIC(10,2),
               TRIM(brand), GREATEST(stockquantity::INTEGER, 0),
               TRIM(supplierid), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM staging.products
        WHERE productid IS NOT NULL;
    """))
    logger.info("Loaded %s products", res.rowcount)

    # transactions
    conn.execute(text("TRUNCATE production.transactions RESTART IDENTITY"))
    res = conn.execute(text("""
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
    """))
    logger.info("Loaded %s transactions", res.rowcount)

    # transactionitems
    conn.execute(text("TRUNCATE production.transactionitems RESTART IDENTITY"))
    res = conn.execute(text("""
        INSERT INTO production.transactionitems (
            itemid, transactionid, productid, quantity,
            unitprice, discountpercentage, linetotal,
            created_at, updated_at
        )
        SELECT TRIM(itemid), TRIM(transactionid), TRIM(productid),
               GREATEST(quantity::INTEGER, 1),
               unitprice::NUMERIC(10,2),
               GREATEST(LEAST(discountpercentage::NUMERIC, 100), 0),
               GREATEST(linetotal::NUMERIC(12,2), 0.01),
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM staging.transactionitems;
    """))
    logger.info("Loaded %s transaction items", res.rowcount)

def show_counts(conn):
    for table in [
        "production.customers",
        "production.products",
        "production.transactions",
        "production.transactionitems",
    ]:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()
        print(f"{table}: {count}")

def main():
    logger = setup_logging()
    config = load_config()
    engine = get_engine(config)

    with engine.begin() as conn:  # BEGIN/COMMIT automatically
        print("=== RUNNING ETL IN THIS TRANSACTION ===")
        run_staging_to_production(conn, logger)
        print("=== COUNTS INSIDE SAME CONNECTION ===")
        show_counts(conn)

if __name__ == "__main__":
    main()
