<div align="center">

# 🏗️ Modern Data Lakehouse Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-20.10+-blue.svg)](https://www.docker.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.0-black.svg)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-blue.svg)](https://airflow.apache.org/)

**A production-ready data lakehouse with real-time CDC streaming, distributed processing, and Git-like data versioning — fully containerized with Docker Compose.**

</div>

---

## 🏛️ Architecture

The platform runs two parallel pipelines over the same Iceberg storage layer:

```
┌─────────────────────────────────────────────────────────────────────┐
│  BATCH PATH                         STREAMING PATH                  │
│                                                                     │
│  PostgreSQL ──► Airflow ──► Spark   PostgreSQL WAL                  │
│                              │           │                          │
│                              ▼           ▼                          │
│                           Bronze    Debezium CDC                    │
│                              │           │                          │
│                              ▼           ▼                          │
│                           Silver     Kafka 3.9                      │
│                              │           │                          │
│                              ▼           ▼                          │
│                    dbt-trino (Gold)  Spark Streaming (Gold MERGE)   │
│                                                                     │
│              ─────────── MinIO + Iceberg + Nessie ────────────      │
│                                                                     │
│                        Trino (Query Engine)                         │
└─────────────────────────────────────────────────────────────────────┘
```

**Batch** flows through Airflow → Spark → Bronze → Silver → **dbt-trino** (Gold).  
**Streaming** flows through Debezium CDC → Kafka → Spark Structured Streaming → Bronze → Silver → **Spark MERGE** (Gold). dbt is not used in the streaming path.

---

## 🧰 Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.7.3 |
| Distributed Processing | Apache Spark 3.5.0 |
| Event Streaming | Apache Kafka 3.9.0 (KRaft) |
| Change Data Capture | Debezium 2.7.3 |
| Object Storage | MinIO |
| Table Format | Apache Iceberg |
| Data Catalog | Project Nessie |
| Query Engine | Trino |
| Transformation | dbt-trino |
| Source Databases | PostgreSQL 13, MongoDB 7 |
| CI/CD | GitLab CI |

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop 20.10+ with Compose
- 16 GB RAM recommended
- 20 GB free disk space

### Setup

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd lakehouse-platform

# 2. Copy and fill in environment variables
cp .env.example .env

# 3. Start the platform
docker-compose up -d

# 4. Check service health
docker-compose ps
```

> All credentials and endpoints are configured via `.env`. See `.env.example` for required variables.

---

## ⚙️ Services & Ports

| Service | Port | Notes |
|---|---|---|
| Airflow UI | `8085` | Workflow orchestration |
| Spark Master UI | `8088` | Cluster monitoring |
| Spark Worker UI | `8081` | Worker metrics |
| Trino UI | `8080` | SQL query engine |
| MinIO Console | `9001` | Object storage UI |
| Nessie API | `19120` | Data catalog |
| Kafka (internal) | `9092` | Inter-service broker |
| Kafka (external) | `9094` | External producers/consumers |
| Kafka UI | `8084` | Topic & consumer monitoring |
| Debezium Connect | `8083` | CDC connector REST API |
| PostgreSQL | `5432` | Source & Airflow metadata DB |
| MongoDB | `27017` | Document source |

---

## 🗂️ Project Structure

```
lakehouse-platform/
├── Dockerfile                  # Custom Airflow image (Spark + Java)
├── Dockerfile.spark            # Custom Spark image (Iceberg JARs)
├── docker-compose.yml          # Full platform definition
├── .env.example                # Required environment variables
├── .gitlab-ci.yml              # CI/CD pipeline
├── requirements.txt            # Python dependencies
├── init-db.sh                  # PostgreSQL seed script
│
├── airflow/
│   └── dags/                   # Airflow DAGs
│       ├── postgres_bronze.py          # Batch: PG → Bronze
│       ├── sales_transactions_bronze.py # Streaming: Kafka CDC → Bronze
│       ├── sales_transactions_silver.py # Streaming: Bronze → Silver
│       ├── sales_transactions_gold.py   # Streaming: Silver → Gold (Spark MERGE)
│       ├── bronze_to_silver.py         # Batch: Bronze → Silver
│       └── silver_to_gold.py           # Batch: Silver → dbt Gold trigger
│
├── spark/
│   └── jars/                   # Iceberg, Nessie, AWS SDK JARs
│
├── trino/
│   └── catalog/                # Trino connector configs
│
└── dbt/
    └── dbt_datalakehouse/      # dbt project (Silver → Gold, batch only)
        └── models/
            ├── silver/         # customers.sql, products.sql
            └── gold/           # fct_customer_metrics, fct_daily_summary,
                                #   fct_product_performance
```

---

## 🔄 Data Flow Detail

### Medallion Layers

| Layer | Owner (Batch) | Owner (Streaming) |
|---|---|---|
| **Bronze** | Airflow + Spark | Spark Structured Streaming |
| **Silver** | Airflow + Spark | Airflow + Spark |
| **Gold** | dbt-trino | Spark MERGE |

### Debezium CDC Setup

Debezium runs as a Kafka Connect worker and captures row-level changes from the PostgreSQL WAL. To register a connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector.json
```

See `connector.json` for the connector definition. Ensure Debezium environment variables are set in `.env`.

---

## 🔁 CI/CD Pipeline (GitLab)

Three stages defined in `.gitlab-ci.yml`:

| Stage | Jobs |
|---|---|
| **lint** | `flake8` on DAGs · `sqlfluff` on dbt models (Trino dialect) |
| **validate** | Python syntax check · `dbt parse` + `dbt compile` · `docker-compose config` |
| **build** | Docker images for Airflow, Spark, dbt — pushed to GitLab Registry, tagged with commit SHA |

- Lint and validate jobs trigger only on relevant file changes (path-based rules).
- Build jobs run on `main` branch only.

---

## 📚 References

- [Apache Iceberg Docs](https://iceberg.apache.org/docs/latest/)
- [Project Nessie Docs](https://projectnessie.org/docs/)
- [Debezium Docs](https://debezium.io/documentation/)
- [dbt-trino Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup)
- [Trino Docs](https://trino.io/docs/current/)
- [Airflow Docs](https://airflow.apache.org/docs/)

---

<div align="center">

MIT License · Built with open-source ❤️

</div>
