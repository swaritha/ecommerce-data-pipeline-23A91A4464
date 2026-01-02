## Architecture Design

The pipeline follows a three-layer architecture:
- Staging layer for raw ingested data
- Production layer for cleaned and validated data
- Warehouse layer optimized for analytics

Data flows sequentially through ingestion, transformation, quality checks, and warehouse loading.
