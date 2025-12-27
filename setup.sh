#!/bin/bash
echo "E-Commerce Data Pipeline Setup - 23A91A4451"

# Create .env from template
if [ ! -f .env ]; then
    cp .env.example .env
    echo ".env created from template"
else
    echo ".env already exists"
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Start PostgreSQL (Phase 1 complete)
echo "Starting PostgreSQL..."
docker compose -f docker/docker-compose.yml up -d postgres

# Wait for DB healthy
echo "Waiting for PostgreSQL (30s)..."
sleep 30

# Apply DDLs (create schemas + tables, including warehouse.dimcustomers)
echo "Applying database schemas (staging, production, warehouse)..."
docker exec -i ecommerce-postgres psql -U admin -d ecommercedb << "EOF"
\i sql/ddl/init_database.sql
\i sql/ddl/create_warehouse_schema.sql
EOF

# Generate initial data (Phase 2 preview)
echo "Ensuring data/raw directory exists..."
mkdir -p data/raw
# Data generation script will be created next

echo "Setup COMPLETE!"
echo "Next: python scripts/datageneration/generatedata.py"
echo "Status: docker compose -f docker/docker-compose.yml ps"
