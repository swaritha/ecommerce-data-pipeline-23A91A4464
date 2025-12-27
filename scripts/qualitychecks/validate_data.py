import os
import json
import yaml
import sqlalchemy
from sqlalchemy import text
from typing import Dict, Any
import pandas as pd

CONFIG_PATH = os.path.join("config", "config.yaml")


def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_engine(config: dict) -> sqlalchemy.Engine:
    db = config["database"]
    url = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    return sqlalchemy.create_engine(url)


def check_null_values(connection, schema: str) -> Dict[str, Any]:
    """Completeness: NULLs in mandatory columns."""
    results: Dict[str, Any] = {}
    mandatory_columns = {
        "customers": ["customerid", "firstname", "lastname", "email", "registrationdate"],
        "products": ["productid", "productname", "category", "price", "cost"],
        "transactions": ["transactionid", "customerid", "transactiondate", "paymentmethod"],
        "transactionitems": ["itemid", "transactionid", "productid", "quantity", "unitprice", "linetotal"],
    }

    for table, cols in mandatory_columns.items():
        table_key = f"{schema}.{table}"
        results[table_key] = {}
        for col in cols:
            sql = text(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {col} IS NULL")
            count = connection.execute(sql).scalar_one()
            results[table_key][col] = count
    return results


def check_duplicates(connection, schema: str) -> Dict[str, Any]:
    """Uniqueness: duplicate primary keys."""
    results: Dict[str, Any] = {}
    pk_columns = {
        "customers": "customerid",
        "products": "productid",
        "transactions": "transactionid",
        "transactionitems": "itemid",
    }

    for table, pk in pk_columns.items():
        sql = text(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk}, COUNT(*) AS c
                FROM {schema}.{table}
                GROUP BY {pk}
                HAVING COUNT(*) > 1
            ) AS dup;
        """)
        dup_count = connection.execute(sql).scalar_one()
        results[f"{schema}.{table}"] = {"duplicate_pk_count": dup_count}
    return results


def check_referential_integrity(connection, schema: str) -> Dict[str, Any]:
    """Referential integrity between tables."""
    results: Dict[str, Any] = {}

    # transactions.customerid -> customers.customerid
    sql_txn_cust = text(f"""
        SELECT COUNT(*) FROM {schema}.transactions t
        LEFT JOIN {schema}.customers c ON t.customerid = c.customerid
        WHERE c.customerid IS NULL;
    """)
    results["transactions_customer_orphans"] = connection.execute(sql_txn_cust).scalar_one()

    # transactionitems.transactionid -> transactions.transactionid
    sql_items_txn = text(f"""
        SELECT COUNT(*) FROM {schema}.transactionitems ti
        LEFT JOIN {schema}.transactions t ON ti.transactionid = t.transactionid
        WHERE t.transactionid IS NULL;
    """)
    results["items_transaction_orphans"] = connection.execute(sql_items_txn).scalar_one()

    # transactionitems.productid -> products.productid
    sql_items_prod = text(f"""
        SELECT COUNT(*) FROM {schema}.transactionitems ti
        LEFT JOIN {schema}.products p ON ti.productid = p.productid
        WHERE p.productid IS NULL;
    """)
    results["items_product_orphans"] = connection.execute(sql_items_prod).scalar_one()

    results["total_orphans"] = (
        results["transactions_customer_orphans"]
        + results["items_transaction_orphans"]
        + results["items_product_orphans"]
    )
    return results


def check_data_ranges(connection, schema: str) -> Dict[str, Any]:
    """Validity: business rule violations."""
    results: Dict[str, Any] = {}

    # products: price >= 0, cost >= 0, cost <= price
    sql_bad_products = text(f"""
        SELECT COUNT(*) FROM {schema}.products
        WHERE price < 0 OR cost < 0 OR cost > price;
    """)
    results["products_invalid_price_cost"] = connection.execute(sql_bad_products).scalar_one()

    # transactionitems: quantity > 0, unitprice >= 0, discount 0-100, linetotal > 0
    sql_bad_items = text(f"""
        SELECT COUNT(*) FROM {schema}.transactionitems
        WHERE quantity <= 0 OR unitprice < 0 
           OR discountpercentage < 0 OR discountpercentage > 100
           OR linetotal <= 0;
    """)
    results["transactionitems_invalid_ranges"] = connection.execute(sql_bad_items).scalar_one()

    return results


def table_exists(connection, schema_table: str) -> bool:
    """Check if a fully-qualified table (schema.table) exists."""
    schema, table = schema_table.split(".")
    sql = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name   = :table
        )
    """)
    return connection.execute(sql, {"schema": schema, "table": table}).scalar_one()


def check_warehouse_integrity(connection) -> Dict[str, Any]:
    """Warehouse-specific referential integrity between fact and dims."""
    results: Dict[str, Any] = {}

    results["fact_customers_orphans"] = connection.execute(text("""
        SELECT COUNT(*) FROM warehouse.factorders f 
        LEFT JOIN warehouse.dimcustomers dc
               ON f.customerkey = dc.customerkey AND dc.iscurrent = TRUE
        WHERE dc.customerkey IS NULL;
    """)).scalar_one()

    results["fact_orders_orphans"] = connection.execute(text("""
        SELECT COUNT(*) FROM warehouse.factorders f 
        LEFT JOIN warehouse.dimorders dord
               ON f.orderkey = dord.orderkey
        WHERE dord.orderkey IS NULL;
    """)).scalar_one()

    results["fact_date_orphans"] = connection.execute(text("""
        SELECT COUNT(*) FROM warehouse.factorders f 
        LEFT JOIN warehouse.dimdate dd
               ON f.datekey = dd.datekey
        WHERE dd.datekey IS NULL;
    """)).scalar_one()

    results["total_orphans"] = (
        results["fact_customers_orphans"]
        + results["fact_orders_orphans"]
        + results["fact_date_orphans"]
    )
    return results


def check_warehouse_ranges(connection) -> Dict[str, Any]:
    """Warehouse range / consistency checks on factorders."""
    results: Dict[str, Any] = {}
    results["negative_quantity"] = connection.execute(
        text("SELECT COUNT(*) FROM warehouse.factorders WHERE quantity <= 0")
    ).scalar_one()
    results["negative_price"] = connection.execute(
        text("SELECT COUNT(*) FROM warehouse.factorders WHERE unitprice <= 0")
    ).scalar_one()
    results["total_mismatch"] = connection.execute(text("""
        SELECT COUNT(*) 
        FROM warehouse.factorders 
        WHERE ABS(totalamount - (quantity * unitprice)) > 0.01
    """)).scalar_one()
    return results


def calculate_quality_score(check_results: Dict[str, Any]) -> float:
    """Aggregate into 0-100 quality score."""
    nulls = check_results.get("nulls", {})
    duplicates = check_results.get("duplicates", {})
    ri = check_results.get("referential_integrity", {})
    ranges = check_results.get("ranges", {})

    # flatten null counts
    total_nulls = 0
    for table_dict in nulls.values():
        total_nulls += sum(table_dict.values())

    total_dups = sum(v["duplicate_pk_count"] for v in duplicates.values())
    total_orphans = ri.get("total_orphans", 0)
    range_violations = sum(ranges.values())

    score = 100.0
    score -= min(total_orphans * 2.0, 40.0)       # Heavy penalty for orphans
    score -= min(total_nulls * 0.5, 30.0)         # Moderate for nulls
    score -= min(range_violations * 0.5, 20.0)    # Moderate for ranges
    score -= min(total_dups * 1.0, 10.0)          # Light for duplicates

    return max(score, 0.0)


def main() -> None:
    """Run all checks and write JSON report."""
    config = load_config()
    engine = get_engine(config)

    with engine.connect() as conn:
        # Staging checks
        nulls_staging = check_null_values(conn, "staging")
        duplicates_staging = check_duplicates(conn, "staging")
        ri_staging = check_referential_integrity(conn, "staging")
        ranges_staging = check_data_ranges(conn, "staging")

        # Warehouse checks (only if dimcustomers exists)
        if table_exists(conn, "warehouse.dimcustomers"):
            nulls_wh = {}
            duplicates_wh = {}
            ri_wh = check_warehouse_integrity(conn)
            ranges_wh = check_warehouse_ranges(conn)
        else:
            nulls_wh = {}
            duplicates_wh = {}
            ri_wh = {}
            ranges_wh = {}

    check_results = {
        "nulls": {**nulls_staging, **nulls_wh},
        "duplicates": {**duplicates_staging, **duplicates_wh},
        "referential_integrity": {**ri_staging, **ri_wh},
        "ranges": {**ranges_staging, **ranges_wh},
    }

    # totals for status fields
    total_nulls = 0
    for table_dict in check_results["nulls"].values():
        total_nulls += sum(table_dict.values())
    total_dups = sum(v["duplicate_pk_count"] for v in check_results["duplicates"].values())
    total_orphans = check_results["referential_integrity"].get("total_orphans", 0)
    total_ranges = sum(check_results["ranges"].values())

    score = calculate_quality_score(check_results)
    grade = "A" if score >= 95 else "B" if score >= 85 else "C" if score >= 70 else "D" if score >= 50 else "F"

    report = {
        "check_timestamp": pd.Timestamp.now().isoformat(),
        "checks_performed": {
            "null_checks": {
                "status": "passed" if total_nulls == 0 else "failed",
                "tables_checked": list(check_results["nulls"].keys()),
                "null_violations": total_nulls,
                "details": check_results["nulls"],
            },
            "duplicate_checks": {
                "status": "passed" if total_dups == 0 else "failed",
                "duplicates_found": total_dups,
                "details": check_results["duplicates"],
            },
            "referential_integrity": {
                "status": "passed" if total_orphans == 0 else "failed",
                "orphan_records": total_orphans,
                "details": check_results["referential_integrity"],
            },
            "range_checks": {
                "status": "passed" if total_ranges == 0 else "failed",
                "violations": total_ranges,
                "details": check_results["ranges"],
            },
        },
        "overall_quality_score": round(score, 2),
        "quality_grade": grade,
    }

    os.makedirs("reports", exist_ok=True)
    with open(os.path.join("reports", "quality_report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("QUALITY REPORT:")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
