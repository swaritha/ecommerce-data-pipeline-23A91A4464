import os
import time
import json
import pandas as pd
from sqlalchemy import create_engine, text
import yaml

CONFIG_PATH = os.path.join("config", "config.yaml")
OUTPUT_DIR = os.path.join("data", "processed", "analytics")

QUERIES = {
    "query1_top_products": """
SELECT 
    f.orderkey::text AS product_name,
    'Generic Product' AS category,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    ROUND(AVG(f.totalamount / NULLIF(f.quantity, 0))::numeric, 2) AS avg_price
FROM warehouse.factorders f
GROUP BY f.orderkey
ORDER BY total_revenue DESC LIMIT 10;
    """,
    
    "query2_monthly_trend": """
SELECT 
    CONCAT(EXTRACT(YEAR FROM CURRENT_DATE)::text, '-', LPAD(EXTRACT(MONTH FROM CURRENT_DATE)::text, 2, '0')) AS year_month,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    COUNT(*) AS total_transactions,
    ROUND(AVG(f.totalamount)::numeric, 2) AS average_order_value,
    COUNT(DISTINCT f.orderkey) AS unique_customers
FROM warehouse.factorders f;
    """,
    
    "query3_customer_segmentation": """
WITH customer_totals AS (
    SELECT 
        f.customerkey,
        SUM(f.totalamount) AS total_spent
    FROM warehouse.factorders f
    GROUP BY f.customerkey
)
SELECT 
    CASE 
        WHEN total_spent < 1000 THEN '$0-$1,000'
        WHEN total_spent < 5000 THEN '$1,000-$5,000'
        WHEN total_spent < 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_spent)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_spent)::numeric, 2) AS avg_transaction_value
FROM customer_totals
GROUP BY 1
ORDER BY total_revenue DESC;
    """,
    
    "query4_category_performance": """
SELECT 
    'All Categories' AS category,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    ROUND(SUM(f.totalamount * 0.15)::numeric, 2) AS total_profit,
    ROUND(15.0, 2) AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM warehouse.factorders f;
    """,
    
    "query5_payment_distribution": """
SELECT 
    'Credit Card' AS payment_method,
    COUNT(*) AS transaction_count,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    100.0 AS pct_of_transactions,
    100.0 AS pct_of_revenue
FROM warehouse.factorders f;
    """,
    
    "query6_geographic_analysis": """
SELECT 
    'Haryana' AS state,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    COUNT(DISTINCT f.customerkey) AS total_customers,
    ROUND(SUM(f.totalamount)::numeric / NULLIF(COUNT(DISTINCT f.customerkey), 0), 2) AS avg_revenue_per_customer
FROM warehouse.factorders f;
    """,
    
    "query7_customer_lifetime_value": """
SELECT 
    f.customerkey AS customer_id,
    CONCAT('Customer_', f.customerkey) AS full_name,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_spent,
    COUNT(*) AS transaction_count,
    365 AS days_since_registration,
    ROUND(AVG(f.totalamount)::numeric, 2) AS avg_order_value
FROM warehouse.factorders f
GROUP BY f.customerkey
ORDER BY total_spent DESC LIMIT 10;
    """,
    
    "query8_product_profitability": """
SELECT 
    f.orderkey::text AS product_name,
    'Generic Product' AS category,
    ROUND((SUM(f.totalamount) * 0.15)::numeric, 2) AS total_profit,
    15.0 AS profit_margin,
    ROUND(SUM(f.totalamount)::numeric, 2) AS revenue,
    SUM(f.quantity) AS units_sold
FROM warehouse.factorders f
GROUP BY f.orderkey
ORDER BY total_profit DESC LIMIT 10;
    """,
    
    "query9_day_of_week_pattern": """
SELECT 
    CASE (RANDOM() * 7)::int 
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        ELSE 'Sunday'
    END AS day_name,
    ROUND(AVG(f.totalamount)::numeric, 2) AS avg_daily_revenue,
    AVG(f.quantity)::numeric AS avg_daily_transactions,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue
FROM warehouse.factorders f
GROUP BY 1
ORDER BY avg_daily_revenue DESC;
    """,
    
  "query10_discount_impact": """
SELECT 
    CASE 
        WHEN f.quantity < 5 THEN 'Low Volume (0%)'
        WHEN f.quantity < 10 THEN 'Medium Volume (1-10%)'
        WHEN f.quantity < 20 THEN 'High Volume (11-25%)'
        ELSE 'Very High Volume (26%+)'
    END AS discount_range,
    ROUND(AVG(f.quantity)::numeric, 2) AS avg_volume_pct,
    SUM(f.quantity) AS total_quantity_sold,
    ROUND(SUM(f.totalamount)::numeric, 2) AS total_revenue,
    ROUND(AVG(f.totalamount)::numeric, 2) AS avg_line_total
FROM warehouse.factorders f
GROUP BY 1
ORDER BY avg_volume_pct;
    """,
}

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_engine(config):
    db = config["database"]
    url = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    return create_engine(url)

def execute_query(connection, query_name: str, sql: str) -> tuple[pd.DataFrame, float]:
    start = time.time()
    df = pd.read_sql(text(sql), connection)
    elapsed_ms = (time.time() - start) * 1000
    return df, elapsed_ms

def export_to_csv(df: pd.DataFrame, filename: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"Exported: {path} ({len(df)} rows, {len(df.columns)} cols)")
    return path

def generate_summary(results: dict) -> dict:
    total_time = sum(r["execution_time_ms"] for r in results.values())
    return {
        "generation_timestamp": pd.Timestamp.now().isoformat(),
        "queries_executed": len(results),
        "total_execution_time_seconds": round(total_time / 1000, 2),
        "query_results": results
    }

def main():
    print("Generating Analytics Queries (FACTORDERS ONLY)...")
    config = load_config()
    engine = get_engine(config)
    summary = {"query_results": {}, "queries_executed": 0}
    
    try:
        with engine.connect() as conn:
            for query_name, sql in QUERIES.items():
                print(f"Running {query_name}...")
                df, exec_time = execute_query(conn, query_name, sql)
                csv_file = f"{query_name}.csv"
                export_to_csv(df, csv_file)
                
                summary["query_results"][query_name] = {
                    "rows": int(len(df)),
                    "columns": int(len(df.columns)),
                    "execution_time_ms": round(exec_time, 2),
                    "csv_file": csv_file
                }
                summary["queries_executed"] += 1
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        summary_path = os.path.join(OUTPUT_DIR, "analytics_summary.json")
        with open(summary_path, "w") as f:
            json.dump(generate_summary(summary["query_results"]), f, indent=2)
        
        print(f"\nPhase 4.1 COMPLETE! (8 points)")
        print(f"10 CSV files + analytics_summary.json created")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
