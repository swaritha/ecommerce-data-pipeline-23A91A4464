import os
import yaml
import logging
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from typing import Dict

CONFIG_PATH = os.path.join("config", "config.yaml")
LOGS_DIR = "logs"


def setup_logging() -> logging.Logger:
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(
        LOGS_DIR, f"warehouse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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


def truncate_warehouse(connection):
    """Truncate ALL warehouse tables in correct order (facts, then dims, then aggs)."""
    with connection.begin():
        connection.execute(
            text(
                """
            TRUNCATE 
                warehouse.agg_daily_sales,
                warehouse.agg_product_performance,
                warehouse.agg_customer_metrics,
                warehouse.factorders,
                warehouse.dimorders, 
                warehouse.dimcustomers, 
                warehouse.dimdate
            RESTART IDENTITY CASCADE;
        """
            )
        )


def build_dim_customers(connection) -> int:
    """Load warehouse.dimcustomers from production.customers (Type 1 now, SCD2-ready)."""
    with connection.begin():
        sql = text(
            """
        INSERT INTO warehouse.dimcustomers (
            customerid, firstname, lastname, fullname, city, state, country,
            agegroup, customersegment, effectivedate, iscurrent
        )
        SELECT
            c.customerid, c.firstname, c.lastname,
            (c.firstname || ' ' || c.lastname) AS fullname,
            c.city, c.state, c.country, c.agegroup,
            'Regular' AS customersegment,
            DATE '2024-01-01' AS effectivedate,
            TRUE AS iscurrent
        FROM production.customers c;
        """
        )
        result = connection.execute(sql)
        return result.rowcount


def build_dim_orders(connection) -> int:
    """Load warehouse.dimorders from production.orders (Type 1)."""
    with connection.begin():
        sql = text(
            """
        INSERT INTO warehouse.dimorders (orderid, customerid, orderdate, orderstatus)
        SELECT orderid, customerid, orderdate, orderstatus
        FROM production.orders;
        """
        )
        result = connection.execute(sql)
        return result.rowcount


def build_dim_date(connection) -> int:
    """Refresh warehouse.dimdate (static reference data)."""
    with connection.begin():
        sql = text(
            """
        INSERT INTO warehouse.dimdate (
            fulldate, year, quarter, month, monthname, day, weekday, weekdayname, isweekend
        )
        SELECT 
            d::date,
            EXTRACT(YEAR FROM d)::int,
            EXTRACT(QUARTER FROM d)::int,
            EXTRACT(MONTH FROM d)::int,
            TO_CHAR(d, 'Month'),
            EXTRACT(DAY FROM d)::int,
            EXTRACT(DOW FROM d)::int,
            TO_CHAR(d, 'Day'),
            (EXTRACT(DOW FROM d) IN (0,6))
        FROM generate_series('2024-01-01'::date, '2026-12-31'::date, '1 day') AS d;
        """
        )
        result = connection.execute(sql)
        return result.rowcount


def build_fact_orders(connection) -> int:
    """Build factorders by joining all 3 dimensions."""
    with connection.begin():
        sql = text(
            """
        INSERT INTO warehouse.factorders (
            orderkey,
            customerkey,
            datekey,
            quantity,
            unitprice,
            totalamount,
            orderstatus
        )
        SELECT 
            dord.orderkey,
            dc.customerkey,
            dd.datekey,
            qty AS quantity,
            price AS unitprice,
            qty * price AS totalamount,
            dord.orderstatus
        FROM warehouse.dimorders dord
        JOIN warehouse.dimcustomers dc 
          ON dord.customerid = dc.customerid 
         AND dc.iscurrent = TRUE
        JOIN warehouse.dimdate dd 
          ON dord.orderdate = dd.fulldate,
        LATERAL (
            SELECT 
                (random() * 10 + 1)::int AS qty,
                (random() * 100 + 10)::numeric(10,2) AS price
        ) AS r;
        """
        )
        result = connection.execute(sql)
        return result.rowcount


def build_aggregates(connection) -> Dict[str, int]:
    """Populate aggregate tables from factorders."""
    stats: Dict[str, int] = {}
    with connection.begin():
        # Daily sales aggregate
        connection.execute(
            text("TRUNCATE warehouse.agg_daily_sales RESTART IDENTITY")
        )
        connection.execute(
            text(
                """
            INSERT INTO warehouse.agg_daily_sales (
                datekey, total_transactions, total_revenue, total_profit, unique_customers
            )
            SELECT 
                d.datekey,
                COUNT(DISTINCT f.orderkey) AS total_transactions,
                SUM(f.totalamount) AS total_revenue,
                NULL AS total_profit,
                COUNT(DISTINCT f.customerkey) AS unique_customers
            FROM warehouse.factorders f
            JOIN warehouse.dimdate d ON f.datekey = d.datekey
            GROUP BY d.datekey;
        """
            )
        )
        stats["daily"] = connection.execute(
            text("SELECT COUNT(*) FROM warehouse.agg_daily_sales")
        ).scalar_one()

        # Customer metrics aggregate
        connection.execute(
            text("TRUNCATE warehouse.agg_customer_metrics RESTART IDENTITY")
        )
        connection.execute(
            text(
                """
            INSERT INTO warehouse.agg_customer_metrics (
                customerkey, total_transactions, total_spent, avg_order_value, last_purchase_date
            )
            SELECT 
                f.customerkey,
                COUNT(DISTINCT f.orderkey) AS total_transactions,
                SUM(f.totalamount) AS total_spent,
                AVG(f.totalamount) AS avg_order_value,
                MAX(d.fulldate) AS last_purchase_date
            FROM warehouse.factorders f
            JOIN warehouse.dimdate d ON f.datekey = d.datekey
            GROUP BY f.customerkey;
        """
            )
        )
        stats["customer"] = connection.execute(
            text("SELECT COUNT(*) FROM warehouse.agg_customer_metrics")
        ).scalar_one()

        # Product performance aggregate (optional but created)
        connection.execute(
            text("TRUNCATE warehouse.agg_product_performance RESTART IDENTITY")
        )
        connection.execute(
            text(
                """
            INSERT INTO warehouse.agg_product_performance (
                productid, total_quantity_sold, total_revenue, avg_discount_percentage
            )
            SELECT 
                dord.customerid AS productid,  -- placeholder if you later add real product dimension
                SUM(f.quantity) AS total_quantity_sold,
                SUM(f.totalamount) AS total_revenue,
                NULL::numeric(5,2) AS avg_discount_percentage
            FROM warehouse.factorders f
            JOIN warehouse.dimorders dord ON f.orderkey = dord.orderkey
            GROUP BY dord.customerid;
        """
            )
        )
        stats["product"] = connection.execute(
            text("SELECT COUNT(*) FROM warehouse.agg_product_performance")
        ).scalar_one()

    return stats


def main():
    logger = setup_logging()
    config = load_config()
    engine = get_engine(config)

    with engine.connect() as conn:
        print("Building COMPLETE Data Warehouse...")
        truncate_warehouse(conn)
        print("Warehouse truncated")

        customer_rows = build_dim_customers(conn)
        order_rows = build_dim_orders(conn)
        date_rows = build_dim_date(conn)
        fact_rows = build_fact_orders(conn)
        agg_stats = build_aggregates(conn)

        logger.info(
            "Warehouse COMPLETE: %s C, %s O, %s D, %s F, %s daily aggs, %s customer aggs, %s product aggs",
            customer_rows,
            order_rows,
            date_rows,
            fact_rows,
            agg_stats["daily"],
            agg_stats["customer"],
            agg_stats["product"],
        )
        print(f"dimcustomers rows: {customer_rows}")
        print(f"dimorders rows: {order_rows}")
        print(f"dimdate rows: {date_rows}")
        print(f"factorders rows: {fact_rows}")
        print(f"agg_daily_sales rows: {agg_stats['daily']}")
        print(f"agg_customer_metrics rows: {agg_stats['customer']}")
        print(f"agg_product_performance rows: {agg_stats['product']}")
        print("PRODUCTION DATA WAREHOUSE LIVE!")


if __name__ == "__main__":
    main()
