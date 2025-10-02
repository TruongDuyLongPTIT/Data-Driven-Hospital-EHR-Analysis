# 🩻 [Healthcare] Hospital EHR Data Pipeline🩻
- I built a data pipeline project using the **MIMIC dataset** with a **lakehouse architecture**.
- The goal of this project is to **practice and strengthen my data engineering skill**s.
- It demonstrates my ability to design and implement modern data workflows for **real-world healthcare data**.

## 🛰️ System Architecture
![finalllllllll](https://github.com/user-attachments/assets/58fe9ecb-798a-41b5-b84d-a4990f58ce3c)

## Infrastructure

| Thành Phần | Phiên Bản | Container | Ports | Chức Năng Chính |
|------------|-----------|-----------|-------|-----------------|
| **Apache Spark Master** | 3.5.0 | `spark-master` | 8080 (UI), 7077 (Master) | Điều phối cluster, quản lý workers |
| **Spark Worker 1** | 3.5.0 | `spark-worker-1` | 8081 | Xử lý dữ liệu phân tán |
| **Spark Worker 2** | 3.5.0 | `spark-worker-2` | 8082 | Xử lý dữ liệu phân tán |
| **Spark Worker 3** | 3.5.0 | `spark-worker-3` | 8083 | Xử lý dữ liệu phân tán |
| **MinIO Node 1** | Latest | `minio1` | 9000 (API), 9001 (Console) | Object storage (S3-compatible) |
| **MinIO Node 2** | Latest | `minio2` | - | Distributed storage node |
| **MinIO Node 3** | Latest | `minio3` | - | Distributed storage node |
| **PostgreSQL** | 15.6 | `postgres_db` | 5432 | Relational database, metadata store |
| **Iceberg REST Catalog** | 0.10.0 | `iceberg-rest` | 8181 | Table format, schema management |
| **Trino** | 435 | `trino` | 8084 | Distributed SQL query engine |
| **Apache Airflow** | 2.8.1 | `airflow_standalone` | 8090 | Workflow orchestration |
| **DBT** | 1.7.8 | `dbt_service` | - | Data transformation framework |

## Tech Stack
| Component | Purpose | Technology |
|-----------|---------|------------|
| **ETL Pipeline** | Data extraction, transformation, loading | Apache Spark, Python |
| **Workflow Orchestration** | Schedule & monitor data pipelines | Apache Airflow, dbt |
| **Query Engine** | High-performance SQL analytics | Trino |
| **Lakehouse** | Scalable object storage | MinIO + Apache Iceberg |
| **Database Source** | Ingest\Extract data source to Lakehouse | PostgreSQL |
| **Visualization** | Business intelligence dashboards | Tableau |

## 🗃️ Repository Structure
<details>
<summary>📋 View Full Directory Tree</summary>

```shell
Healthcare-Data-Driven-Hospital-EHR-Analysis/
│
├── 📂 scripts/                          # ETL Pipeline
│   ├── Extract/                         # Data ingestion from MIMIC dataset
│   │   ├── ingest_mimic.py
│   │   └── config.py
│   ├── Transform/                       # Data transformation & cleaning
│   │   ├── chartevents_transform.py
│   │   ├── patients_transform.py
│   │   ├── icustays_transform.py
│   │   ├── d_items_transform.py
│   │   └── time_normalization.py
│   └── Load/                            # Load to Data Warehouse
│       ├── dimPatients.py
│       ├── dimICUStay.py
│       ├── dimTime.py
│       ├── dimEventType.py
│       └── factICUVitalSignEvent.py
│
├── 📂 airflow/                          # Workflow orchestration
│   ├── dags/
│   ├── logs/
│   └── plugins/
│
├── 📂 data_information/                 # Analysis notebooks
│   ├── Data_modeling.ipynb
│   ├── Phân_tích_bộ_dữ_liệu.ipynb
│   └── Thống_kê_và_chuẩn_hóa.ipynb
│
├── 📂 studyhistory/                     # Research notebooks
│   ├── MinIO_Iceberg.ipynb
│   ├── csv_gz_to_parquet.ipynb
│   └── Spark_load_data_parallel.ipynb
│
├── 📂 spark-jars/                       # Spark dependencies
│   ├── aws-java-sdk-bundle-1.12.262.jar
│   ├── hadoop-aws-3.3.4.jar
│   └── iceberg-spark-runtime-3.5_2.12-1.6.0.jar
│
├── 📂 trino/                            # Query engine
│   └── etc/catalog/
│       ├── iceberg.properties
│       └── postgres.properties
│
├── 📂 conf/                             # Metastore config
│   └── metastore-site.xml
│
├── 📂 etc/                              # Trino config
│   ├── config.properties
│   ├── jvm.config
│   └── catalog/
│
├── 📂 data/                             # Raw data & scripts
│   ├── Ingest.py
│   ├── test_spark.py
│   └── people.csv
│
├── 📂 storage/                          # Data lake storage
│
├── 📂 command/                          # Command references
│   ├── Lệnh hay dùng.txt
│   └── Trino command.txt
│
├── 📂 image/                            # Documentation images
│   ├── finaldb.gif
│   ├── Spark Master UI.png
│   ├── Tableau Dashboard.png
│   └── Spark load data.drawio.svg
│
├── 📂 log/                              # Processing logs
│   ├── Ingest file 40 by Spark.txt
│   └── ingest_full_data_successful.txt
│
├── docker-compose.yml
├── start.bat
├── stop.bat
└── README.md
```
<\details>

##
