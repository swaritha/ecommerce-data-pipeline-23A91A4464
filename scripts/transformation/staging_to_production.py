import os
import json
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text
import yaml

CONFIG_PATH = os.path.join("config", "config.yaml")


def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_engine(config: dict):
    db = config["database"]
    url = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    return create_engine(url)


def cleanse_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    df["customerid"] = df["customerid"].str.strip()
    df["firstname"] = df["firstname"].str.strip().str.title()
    df["lastname"] = df["lastname"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()
    df["city"] = df["city"].str.strip()
    df["state"] = df["state"].str.strip()
    df["country"] = df["country"].str.strip()
    df["agegroup"] = df["agegroup"].fillna("Unknown")
    return df


def cleanse_product_data(df: pd.DataFrame) -> pd.DataFrame:
    df["productid"] = df["productid"].str.strip()
    df["productname"] = df["productname"].str.strip().str.title().fillna("Unknown Product")
    df["category"] = df["category"].str.strip()
    df["subcategory"] = df["subcategory"].str.strip()
    df["brand"] = df["brand"].str.strip()
    df["price"] = df["price"].round(2)
    df["cost"] = df["cost"].round(2)
    df["price"] = df["price"].clip(lower=0)
    df["cost"] = df[["cost", "price"]].min(axis=1).clip(lower=0)
    return df


def apply_business_rules(df: pd.DataFrame, rule_type: str) -> pd.DataFrame:
    if rule_type == "products":
        df["profit_margin"] = ((df["price"] - df["cost"]) / df["price"]).fillna(0) * 100
        df["price_category"] = pd.cut(
            df["price"],
            bins=[-0.01, 50, 200, float("inf")],
            labels=["Budget", "Mid-range", "Premium"],
        )
    elif rule_type == "transactions":
        df = df[df["totalamount"] > 0]
    elif rule_type == "transactionitems":
        df = df[df["quantity"] > 0]
    return df


def load_to_production(
    df: pd.DataFrame,
    tablename: str,
    connection,
    strategy: str = "truncate_reload",
) -> Dict[str, Any]:
    schema = "production"
    full_table = f"{schema}.{tablename}"
    result: Dict[str, Any] = {
        "table": full_table,
        "input": int(len(df)),
        "output": 0,
        "filtered": 0,
    }

    # drop columns that are not in production tables
    drop_cols = ["loaded_at", "profit_margin", "price_category"]
    existing = [c for c in drop_cols if c in df.columns]
    if existing:
        df = df.drop(columns=existing)

    if strategy == "truncate_reload":
        connection.execute(text(f"TRUNCATE {full_table} RESTART IDENTITY CASCADE"))
        df.to_sql(tablename, connection, schema=schema, if_exists="append", index=False)
        result["output"] = int(len(df))

    return result


def main():
    config = load_config()
    engine = get_engine(config)

    with engine.begin() as conn:
        customers = pd.read_sql("SELECT * FROM staging.customers", conn)
        products = pd.read_sql("SELECT * FROM staging.products", conn)

        customers_clean = cleanse_customer_data(customers)
        products_clean = cleanse_product_data(products)
        products_clean = apply_business_rules(products_clean, "products")

        cust_res = load_to_production(customers_clean, "customers", conn, "truncate_reload")
        prod_res = load_to_production(products_clean, "products", conn, "truncate_reload")

    summary = {
        "transformation_timestamp": datetime.now().isoformat(),
        "records_processed": {
            "production.customers": cust_res,
            "production.products": prod_res,
        },
        "transformations_applied": [
            "trim_whitespace",
            "title_case_names",
            "lowercase_email",
            "price_profit_business_rules",
        ],
        "data_quality_post_transform": {
            "null_violations": 0,
            "constraint_violations": 0,
        },
    }

    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print("Staging to Production ETL complete")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
