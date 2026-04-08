# NYC Taxi Lakehouse

A production-ready **Modern Data Lakehouse** built with Apache Iceberg, Spark, and the Medallion Architecture.

## Architecture

```
                                    +------------------+
                                    |     Metabase     |
                                    |   (BI Dashboard) |
                                    +--------+---------+
                                             |
+------------------+                +--------v---------+
|   Landing Zone   |                |      Trino       |
|  (Parquet/CSV)   |                |  (SQL Engine)    |
+--------+---------+                +--------+---------+
         |                                   |
         v                                   v
+--------+---------+     +----------+--------+---------+----------+
|                  |     |          |                  |          |
|   BRONZE Layer   +---->+  SILVER  +----------------->+   GOLD   |
|   (Raw Data)     |     |  (Clean) |                  |  (Dims)  |
|                  |     |          |                  |          |
+--------+---------+     +----------+------------------+----------+
         |                          |                  |
         |            +-------------+------------------+
         |            |
         v            v
+--------+------------+---------+
|       Apache Iceberg          |
|   (Table Format + Catalog)    |
+--------+----------------------+
         |
+--------v---------+        +------------------+
|     Nessie       |        |   MinIO (S3)     |
|  (Git-like Cat.) |        |   (Storage)      |
+------------------+        +------------------+

+----------------------------------------------------------+
|                    ORCHESTRATION                          |
|  +------------+    +------------+    +-----------------+  |
|  |  Airflow   +--->+ Spark SSH  +--->+ Spark Cluster   |  |
|  | Scheduler  |    |  Submit    |    | (Master+Worker) |  |
|  +------------+    +------------+    +-----------------+  |
+----------------------------------------------------------+

+----------------------------------------------------------+
|                      MONITORING                           |
|  +------------+    +------------+    +-----------------+  |
|  | Prometheus +<---+ Exporters  +<---+ JMX / StatsD    |  |
|  +------+-----+    +------------+    +-----------------+  |
|         |                                                 |
|  +------v-----+                                           |
|  |  Grafana   |                                           |
|  +------------+                                           |
+----------------------------------------------------------+
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Storage | MinIO | S3-compatible object storage |
| Table Format | Apache Iceberg | ACID transactions, time travel |
| Catalog | Nessie | Git-like version control for data |
| Processing | Apache Spark | Distributed ETL engine |
| Query Engine | Trino | Fast SQL analytics |
| Orchestration | Apache Airflow | Pipeline scheduling |
| BI Dashboard | Metabase | Data visualization |
| Monitoring | Prometheus + Grafana | Metrics & alerts |

## Quick Start

```bash
# 1. Setup environment
cp .env.example .env
# Edit .env with your credentials

# 2. Start all services
docker-compose up -d

# 3. Access UIs
# Airflow:   http://localhost:8080
# Spark:     http://localhost:8082
# Trino:     http://localhost:8083
# MinIO:     http://localhost:9001
# Metabase:  http://localhost:3000
# Grafana:   http://localhost:3001
```

## Data Pipeline (Medallion)

```
Landing --> Bronze --> Silver --> Gold
  |           |          |         |
  Raw       Ingest     Clean    Aggregate
Parquet    Iceberg   Validate   Dim Tables
```

| Layer | Input | Output | Operations |
|-------|-------|--------|------------|
| Bronze | `data/landing/*.parquet` | Iceberg raw tables | Schema enforcement |
| Silver | Bronze tables | Cleaned Iceberg | Validation, dedup, null handling |
| Gold | Silver tables | Dim/Fact tables | Aggregation, business logic |

## Project Structure

```
lakehouse/
├── src/
│   ├── pipeline/
│   │   ├── bronze/          # Raw data ingestion
│   │   ├── silver/          # Data cleaning & validation
│   │   └── gold/            # Dimensional modeling
│   ├── airflow/dags/        # DAG definitions
│   ├── maintenance/         # Table maintenance (compact, expire)
│   ├── config/              # Path configurations
│   └── utils/               # Date utilities, data download
├── conf/                    # Service configs (Spark, Prometheus, Trino)
├── docker/                  # Dockerfiles
├── data/                    # Sample data (NYC Taxi)
└── docker-compose.yml
```

## Key Features

- **WAP (Write-Audit-Publish)**: Each ingestion creates isolated branch via Nessie
- **Incremental Processing**: Date-based partitioning with CLI args `--start/--end`
- **Table Maintenance**: Automated compaction, snapshot expiry, orphan cleanup
- **Full Observability**: JMX metrics, StatsD, Prometheus alerts

## License

MIT
