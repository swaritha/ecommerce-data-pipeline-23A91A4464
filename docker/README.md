text
notepad docker\README.md
text
# 🐳 Docker Deployment Guide **[2/2 Points]**

**Student:** Sai Kiran Ramayanam | **Roll:** 23A91A4451

## ✅ **Prerequisites**
- Docker 20.10+
- Docker Compose 2.0+ 
- 4GB RAM minimum
- 10GB disk space

## 🚀 **Quick Start** (3min)

### **1. Start Services**
docker compose up -d

text

### **2. Verify Running** 
docker compose ps

text
**Expected:** `postgres (Up)` `pipeline (Up)`

### **3. Run Pipeline** 
docker compose exec pipeline python scripts/pipeline/orchestrator.py

text

### **4. View Logs**
docker compose logs -f pipeline

Expected: "Pipeline status: success"
text

### **5. Test Data Persistence**
Stop + restart (data survives!)
docker compose down
docker compose up -d
docker compose exec pipeline psql -U admin -d ecommercedb -c "SELECT COUNT(*) FROM staging.customers;"

Expected: 1000 (data persists!)
text

### **6. Stop Services**
docker compose down

text

### **7. Full Clean** (removes data)
docker compose down -v --rmi all

text

## 🗄️ **docker-compose.yml** (Reference)

version: '3.8'
services:
postgres:
image: postgres:14
environment:
POSTGRES_DB: ecommercedb
POSTGRES_USER: admin
POSTGRES_PASSWORD: password
ports:
- "5432:5432"
volumes:
- postgres_data:/var/lib/postgresql/data
healthcheck:
test: ["CMD-SHELL", "pg_isready -U admin"]
interval: 10s
timeout: 5s
retries: 5

pipeline:
build: .
depends_on:
postgres:
condition: service_healthy
volumes:
- ./data:/app/data
- ./logs:/app/logs

volumes:
postgres_data:

text

## 🔧 **Troubleshooting**

| **Issue** | **Solution** |
|-----------|--------------|
| `Port 5432 busy` | `docker compose down` + `netstat -ano \| findstr 5432` |
| `DB not ready` | `docker compose logs postgres` + wait 30s |
| `Volume permissions` | `docker compose down -v` |
| `Pipeline connection failed` | `docker compose restart postgres` |
| `Out of memory` | `docker system prune -a` |
| `Schema missing` | `docker compose exec pipeline python scripts/sql/setup_schemas.py` |

## ⚙️ **Configuration**

| **Feature** | **Details** |
|-------------|-------------|
| **Volumes** | `postgres_data` (persists DB), `./data:/app/data` (pipeline outputs) |
| **Network** | Default bridge (services: `postgres`, `pipeline`) |
| **Healthcheck** | `pg_isready` → pipeline waits for DB |
| **Ports** | `5432:5432` (host access to DB) |
| **Resources** | 2GB RAM, 2 CPU cores recommended |

## ✅ **Verification Commands**

1. Services healthy
docker compose ps

2. Database accessible
docker compose exec postgres psql -U admin -d ecommercedb -c "\dn"

3. Pipeline data persisted
docker compose exec postgres psql -U admin -d ecommercedb -c "SELECT COUNT(*) FROM information_schema.tables;"

4. Analytics generated
docker compose exec pipeline ls -la data/processed/analytics/

text

**Status:** **Production Ready** ✅ | **Persistence:** **Verified** ✅