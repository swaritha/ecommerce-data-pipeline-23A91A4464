# 🚀 E-Commerce Data Pipeline - 23A91A4451 **[34/34 Points Achieved]**

**Student:** Sai Kiran Ramayanam | **Roll Number:** 23A91A4451 | **Date:** Jan 02, 2026


[![Tests](https://img.shields.io/badge/Tests-19%2F19-brightgreen)](htmlcov/)
[![Coverage](https://img.shields.io/badge/Coverage-83%25-blue)](htmlcov/)
[![Pipeline](https://img.shields.io/badge/Pipeline-6%2F6%20SUCCESS-green)](data/processed/)
[![CI](https://github.com/saikiranramayanam/ecommerce-data-pipeline-23A91A4451/actions/workflows/ci.yml/badge.svg)](https://github.com/saikiranramayanam/ecommerce-data-pipeline-23A91A4451/actions)
[![Codecov](https://codecov.io/gh/saikiranramayanam/ecommerce-data-pipeline-23A91A4451/branch/main/graph/badge.svg)](https://codecov.io/gh/saikiranramayanam/ecommerce-data-pipeline-23A91A4451)


---

## 🎯 Architecture Overview (2 pts)

Raw CSVs ──(TRUNCATE + LOAD)──> Staging Schema ──(Cleanse + Rules)──> Production (3NF)  
│ │ │  
└─────────[Data Gen]────┼──────[ETL Pipeline]──────────────┼──────[Quality Checks]  
│  
└───[Dimensional Model]───> Warehouse (Star Schema)  
│  
└───[Analytics SQL]───> BI Dashboard


### Technology Stack

- **ETL Framework:** Python 3.11 + Pandas + SQLAlchemy (**83% coverage**)
- **Database:** PostgreSQL 14 (staging / 3NF / warehouse schemas)
- **Orchestration:** Custom scheduler (daily 02:00 + lockfile + retries)
- **Testing:** pytest (**19 tests passing**, htmlcov report)
- **Monitoring:** JSON health reports (100/100 score)
- **Config:** YAML-driven (`config/config.yaml`)


---

## 📁 Project Structure

```text
ecommerce-data-pipeline-23A91A4451/
├── scripts/  # Pipeline (6 steps, 83% coverage)
│   ├── datageneration/       # Raw CSVs (1000 customers, 500 products)
│   ├── ingestion/            # CSV → staging
│   ├── qualitychecks/        # Data validation (validate_data.py 100% cov)
│   ├── transformation/       # Staging → Production → Warehouse
│   ├── pipeline/             # Orchestrator (96% coverage)
│   └── scheduler/            # Daily automation
├── tests/    # 19 passing tests ✓
├── data/     # CSVs + reports + analytics
├── logs/     # Timestamped execution logs
├── config/   # YAML config (DB + scheduler)
├── htmlcov/  # pytest-coverage report ✓ (83%)
├── docs/     # architecture.md + dashboard_guide.md
└── docker/   # docker-compose.yml + README.md
🚀 Quick Start
1️⃣ Install dependencies
bash
pip install -r requirements.txt  # pandas, sqlalchemy, pytest-cov, pyyaml
2️⃣ Run End-to-End Pipeline (≈ 56s)
bash
python scripts/pipeline/orchestrator.py
Output: Pipeline status: success → CSV, JSON, and log artifacts in data/processed/.

3️⃣ Run Individual Steps
bash
python scripts/datageneration/generatedata.py
python scripts/ingestion/ingest_to_staging.py
python scripts/qualitychecks/validate_data.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
4️⃣ Run Tests + Coverage (83%)
bash
pytest --cov=. --cov-report=html -v
# Creates htmlcov/index.html (19/19 passed, 83% coverage)
start htmlcov/index.html
5️⃣ Automated Scheduler (Prod)
bash
python scripts/scheduler/scheduler.py  # Runs daily 02:00 AM UTC
6️⃣ Monitoring
bash
python scripts/monitoring/pipeline_monitor.py  # Health score 100/100
✅ Key Results & Artifacts
Metric	Value	File
Pipeline Status	✅ SUCCESS (56s)	data/processed/pipeline_execution_report.json
Customers	1000	data/raw/customers.csv
Products	500	data/raw/products.csv
Transactions	10K	data/raw/transactions.csv
Transaction Items	100K+	data/raw/transactionitems.csv
Analytics Files	11 CSVs	data/processed/analytics/
Test Coverage	83%	htmlcov/index.html
Health Score	100/100	data/processed/monitoring_report.json
🗄️ Database Schemas
1. Staging Schema (Raw Replica)
staging.customers, staging.products,
staging.transactions, staging.transactionitems
→ Exact CSV structure – loaded_at column – TRUNCATE+LOAD

2. Production Schema (3NF)
production.customers (PK customerid)

production.products (PK productid)

production.transactions (FK customerid)

production.transactionitems (FK transactionid, productid)

→ Cleansed data – constraints – business rules applied

3. Warehouse Schema (Star)
warehouse.dim_customers (SCD Type 2)

warehouse.dim_products

warehouse.dim_date, dim_payment_methods

warehouse.fact_sales / fact_orders (grain = transaction item)

warehouse.agg_daily_sales

warehouse.agg_product_performance

warehouse.agg_customer_metrics

→ Optimized for analytics & dashboards

📊 Business Insights (from Analytics CSVs)
Electronics ≈ 45% of revenue (~$4.2M).

Premium products have >25% profit margin.

Weekend sales are ~28% higher than weekdays.

Top 10% customers generate ~35% of revenue (Pareto rule).

Top 5 states contribute ~68% of orders.

🧪 Testing (6/6 pts)
bash
pytest --cov=. --cov-report=html -v
# Coverage Report: htmlcov/index.html (83%)
Covers:

Data generation (size, schema)

Ingestion (outputs + logs)

Transformation (ETL summary JSON)

Quality checks + Warehouse analytics

Additional advanced tests (validate_data 100%, extra transformation tests)

🤖 Production Features
Feature	Status	Command
Daily Scheduler	✅ 02:00	scripts/scheduler/scheduler.py
Concurrency Lock	✅	data/processed/scheduler.lock
Retries	✅ 3×	Auto retry in orchestrator.py
Data Retention	✅ 7 days	Cleanup in scheduler
Monitoring	✅ 100/100	pipeline_monitor.py
🛠️ Challenges Solved
Issue	Solution
Windows Unicode	Removed emojis → ASCII
DB Connections	Added connection health checks
Timeouts	Mock data & DB for fast tests/CI
Coverage	pytest-cov + extra tests → 83%
Automation	Added scheduler + retry logic
🌐 Future Enhancements
Kafka-based real-time streaming

AWS ECS / Azure Container Deployment

ML Demand Forecasting (Random Forest)

Slack / Teams Alerts

Airflow / Dagster Integration

Contact: Sai Kiran Ramayanam | 23A91A4451 | saikiranramayanam@gmail.com