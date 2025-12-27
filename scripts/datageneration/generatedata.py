import os
import random
from typing import Dict

import pandas as pd
from faker import Faker
import yaml

CONFIG_PATH = os.path.join("config", "config.yaml")
fake = Faker()


def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def generate_customers(num_customers: int) -> pd.DataFrame:
    """
    Generate customers DataFrame with required columns and ID format CUST0001.
    Columns: customerid, firstname, lastname, email, phone, registrationdate,
             city, state, country, agegroup
    """
    records = []
    used_emails = set()

    for i in range(1, num_customers + 1):
        customer_id = f"CUST{i:04d}"

        # ensure unique email
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)

        records.append(
            {
                "customerid": customer_id,
                "firstname": fake.first_name(),
                "lastname": fake.last_name(),
                "email": email,
                "phone": fake.msisdn()[:10],
                "registrationdate": fake.date_between(start_date="-365d", end_date="today"),
                "city": fake.city(),
                "state": fake.state(),
                "country": "India",
                "agegroup": fake.random_element(
                    elements=["18-25", "26-35", "36-45", "46-60", "60+"]
                ),
            }
        )
    return pd.DataFrame(records)


def generate_products(num_products: int) -> pd.DataFrame:
    """
    Generate products DataFrame with required columns and ID format PROD0001.
    Columns: productid, productname, category, subcategory, price, cost,
             brand, stockquantity, supplierid
    """
    categories = [
        ("Electronics", "Mobiles"),
        ("Electronics", "Laptops"),
        ("Clothing", "Men"),
        ("Clothing", "Women"),
        ("Home & Kitchen", "Appliances"),
        ("Books", "Fiction"),
        ("Sports", "Outdoor"),
        ("Beauty", "Skincare"),
    ]
    records = []
    for i in range(1, num_products + 1):
        product_id = f"PROD{i:04d}"
        category, subcategory = fake.random_element(elements=categories)
        # price between 50 and 5000 with 2 decimals
        price = round(random.uniform(50, 5000), 2)
        # cost is 50–90% of price to guarantee positive margin
        cost = round(price * random.uniform(0.5, 0.9), 2)
        records.append(
            {
                "productid": product_id,
                "productname": fake.word().title(),
                "category": category,
                "subcategory": subcategory,
                "price": price,
                "cost": cost,
                "brand": fake.company(),
                "stockquantity": fake.random_int(min=0, max=1000),
                "supplierid": f"SUPP{i:04d}",
            }
        )
    return pd.DataFrame(records)


def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate transactions DataFrame with required columns and ID format TXN00001.
    Columns: transactionid, customerid, transactiondate, transactiontime,
             paymentmethod, shippingaddress, totalamount (filled later)
    """
    customer_ids = customers_df["customerid"].tolist()
    payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]

    records = []
    for i in range(1, num_transactions + 1):
        transaction_id = f"TXN{i:05d}"
        customer_id = fake.random_element(elements=customer_ids)
        records.append(
            {
                "transactionid": transaction_id,
                "customerid": customer_id,
                "transactiondate": fake.date_between(start_date="-365d", end_date="today"),
                "transactiontime": fake.time(),
                "paymentmethod": fake.random_element(elements=payment_methods),
                "shippingaddress": fake.address().replace("\n", ", "),
                "totalamount": 0.0,  # to be updated after items generation
            }
        )
    return pd.DataFrame(records)


def generate_transaction_items(
    transactions_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate transaction items with required columns and ID format ITEM00001.
    Columns: itemid, transactionid, productid, quantity, unitprice,
             discountpercentage, linetotal
    """
    transaction_ids = transactions_df["transactionid"].tolist()
    product_ids = products_df["productid"].tolist()

    records = []
    item_counter = 1
    for txn_id in transaction_ids:
        num_items = fake.random_int(min=1, max=5)
        for _ in range(num_items):
            product_id = fake.random_element(elements=product_ids)
            quantity = fake.random_int(min=1, max=5)
            unit_price = float(
                products_df.loc[products_df["productid"] == product_id, "price"].iloc[0]
            )
            discount_percentage = fake.random_element(elements=[0, 5, 10, 15, 20])
            line_total = round(quantity * unit_price * (1 - discount_percentage / 100.0), 2)
            item_id = f"ITEM{item_counter:05d}"
            item_counter += 1

            records.append(
                {
                    "itemid": item_id,
                    "transactionid": txn_id,
                    "productid": product_id,
                    "quantity": quantity,
                    "unitprice": unit_price,
                    "discountpercentage": discount_percentage,
                    "linetotal": line_total,
                }
            )
    return pd.DataFrame(records)


def validate_referential_integrity(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    transactions: pd.DataFrame,
    items: pd.DataFrame,
) -> Dict[str, int]:
    """
    Validation function required by spec.
    Returns counts of orphan records and a simple quality score (0 or 100).
    """
    orphan_txn_customers = transactions[~transactions["customerid"].isin(customers["customerid"])].shape[0]
    orphan_items_transactions = items[~items["transactionid"].isin(transactions["transactionid"])].shape[0]
    orphan_items_products = items[~items["productid"].isin(products["productid"])].shape[0]

    total_orphans = orphan_txn_customers + orphan_items_transactions + orphan_items_products

    return {
        "orphan_transactions_customers": orphan_txn_customers,
        "orphan_items_transactions": orphan_items_transactions,
        "orphan_items_products": orphan_items_products,
        "total_orphans": total_orphans,
        "data_quality_score": 100 if total_orphans == 0 else 0,
    }


def main() -> None:
    """Generate all datasets and write CSV + metadata JSON to data/raw/."""
    config = load_config()
    gen_cfg = config.get("data_generation", {})

    num_customers = int(gen_cfg.get("customers", {}).get("count", 1000))
    num_products = int(gen_cfg.get("products", {}).get("count", 500))
    num_transactions = int(gen_cfg.get("transactions", {}).get("count", 10000))

    customers_df = generate_customers(num_customers)
    products_df = generate_products(num_products)
    transactions_df = generate_transactions(num_transactions, customers_df)
    items_df = generate_transaction_items(transactions_df, products_df)

    # update totalamount per transaction
    totals = (
        items_df.groupby("transactionid")["linetotal"]
        .sum()
        .reset_index()
        .rename(columns={"linetotal": "totalamount"})
    )
    transactions_df = transactions_df.drop(columns=["totalamount"]).merge(
        totals, on="transactionid", how="left"
    )
    transactions_df["totalamount"] = transactions_df["totalamount"].fillna(0.0).round(2)

    ri_result = validate_referential_integrity(customers_df, products_df, transactions_df, items_df)

    os.makedirs(os.path.join("data", "raw"), exist_ok=True)
    customers_df.to_csv(os.path.join("data", "raw", "customers.csv"), index=False)
    products_df.to_csv(os.path.join("data", "raw", "products.csv"), index=False)
    transactions_df.to_csv(os.path.join("data", "raw", "transactions.csv"), index=False)
    items_df.to_csv(os.path.join("data", "raw", "transactionitems.csv"), index=False)

    metadata = {
        "num_customers": len(customers_df),
        "num_products": len(products_df),
        "num_transactions": len(transactions_df),
        "num_transaction_items": len(items_df),
        "referential_integrity": ri_result,
    }
    pd.Series(metadata).to_json(os.path.join("data", "raw", "generation_metadata.json"))


if __name__ == "__main__":
    main()
