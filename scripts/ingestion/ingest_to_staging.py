import os
import time
import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple
import yaml
import pandas as pd
import sqlalchemy
from sqlalchemy import text

CONFIG_PATH = os.path.join("config", "config.yaml")
LOGS_DIR = "logs"

def setup_logging() -> logging.Logger:
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(LOGS_DIR, f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_engine(config: dict) -> sqlalchemy.Engine:
    db = config["database"]
    url = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    return sqlalchemy.create_engine(url, pool_size=5, max_overflow=10)

def get_csv_files() -> List[Tuple[str, str]]:
    """Return (table_name, csv_path) tuples for all 4 files."""
    raw_dir = "data/raw"
    return [
        ("customers", os.path.join(raw_dir, "customers.csv")),
        ("products", os.path.join(raw_dir, "products.csv")),
        ("transactions", os.path.join(raw_dir, "transactions.csv")),
        ("transactionitems", os.path.join(raw_dir, "transactionitems.csv")),
    ]

def validate_staging_load(connection, csv_files: List[Tuple[str, str]]) -> Dict[str, Any]:
    """Compare CSV row counts vs staging table counts."""
    results = {}
    
    for table_name, csv_path in csv_files:
        # CSV row count (skip header)
        csv_count = len(pd.read_csv(csv_path))
        
        # Table row count
        sql = text(f"SELECT COUNT(*) FROM staging.{table_name}")
        table_count = connection.execute(sql).scalar_one()
        
        results[table_name] = {
            "csv_row_count": csv_count,
            "staging_row_count": table_count,
            "match": csv_count == table_count
        }
    
    overall_status = all(r["match"] for r in results.values())
    results["overall_status"] = "success" if overall_status else "failure"
    
    return results

def ingest_to_staging(logger: logging.Logger) -> Dict[str, Any]:
    """Main ingestion with full error handling, timing, and logging."""
    start_time = time.time()
    ingestion_timestamp = datetime.utcnow().isoformat()
    
    config = load_config()
    engine = get_engine(config)
    
    csv_files = get_csv_files()
    tables_report: Dict[str, Dict[str, Any]] = {}
    
    try:
        # Load data first (within transaction)
        with engine.begin() as conn:
            logger.info("Starting ingestion transaction...")
            
            for table_name, csv_path in csv_files:
                try:
                    logger.info(f"Processing {table_name} from {csv_path}")
                    
                    if not os.path.exists(csv_path):
                        raise FileNotFoundError(f"CSV missing: {csv_path}")
                    
                    conn.execute(text(f"TRUNCATE staging.{table_name}"))
                    logger.info(f"Truncated staging.{table_name}")
                    
                    df = pd.read_csv(csv_path)
                    df.to_sql(
                        table_name, 
                        conn, 
                        schema="staging", 
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=1000
                    )
                    
                    rows_loaded = len(df)
                    tables_report[table_name] = {
                        "status": "success",
                        "rows_loaded": rows_loaded,
                        "error_message": None
                    }
                    logger.info(f"Loaded {rows_loaded} rows to staging.{table_name}")
                    
                except Exception as e:
                    error_msg = f"Failed {table_name}: {str(e)}"
                    logger.error(error_msg)
                    tables_report[table_name] = {
                        "status": "failed",
                        "rows_loaded": 0,
                        "error_message": str(e)
                    }
                    raise
        
        # Validation after commit (new connection)
        with engine.connect() as conn:
            validation = validate_staging_load(conn, csv_files)
        
    except Exception as e:
        logger.error(f"Transaction rolled back due to error: {str(e)}")
        tables_report = {t: {"status": "failed", "rows_loaded": 0, "error_message": str(e)} 
                        for t, _ in csv_files}
        validation = {"overall_status": "failure"}
    finally:
        total_time = time.time() - start_time
        report = {
            "ingestion_timestamp": ingestion_timestamp,
            "tables": tables_report,
            "validation": validation,
            "total_execution_time_seconds": round(total_time, 2)
        }
        
        os.makedirs("data/staging", exist_ok=True)
        with open("data/staging/ingestion_summary.json", "w") as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Ingestion complete. Total time: {total_time:.2f}s")
        logger.info(f"Report written to data/staging/ingestion_summary.json")
        logger.info(f"Overall status: {validation.get('overall_status', 'unknown')}")
    
    return report

def main():
    logger = setup_logging()
    report = ingest_to_staging(logger)
    print("Ingestion completed. Check logs/ for detailed log file.")
    print(f"Overall status: {report['validation']['overall_status']}")

if __name__ == "__main__":
    main()
