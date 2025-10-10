## 🩻 Healthcare-Data-Driven-Hospital-EHR-Analysis🩻 
*(The project is being upgraded and refined...)*
- I built a data pipeline project using the **MIMIC dataset** with a **lakehouse architecture**.
- The goal of this project is to **practice and strengthen my data engineering skill**s.
- It demonstrates my ability to design and implement modern data workflows for **real-world healthcare data**.
  
## 🧬 Dataset: MIMIC-IV Dataset

**MIMIC-IV (Medical Information Mart for Intensive Care IV)** is a **large-scale real-world** medical dataset published by the MIT Lab for Computational Physiology. It contains anonymized data from tens of thousands of patients at Beth Israel Deaconess Medical Center (Boston).

#### Key Features

| Features | Description |
|----------|-------|
| **Real-world Data** | Clinical data from a **real hospital**, not synthetic or simulated data |
| **Scale** | **~100GB** when stored in an RDBMS, with hundreds of millions of records |
| **Number of Patients** | **Tens of thousands** of ICU and inpatient cases |
| **Security** | Fully de-identified data (HIPAA compliant) |
| **Version** | MIMIC-IV v3.1 (latest) |

#### Dataset Contents

**The dataset includes::**
- **🤒 Patient demographics**: age, gender, ethnicity
- **🩺 Clinical diagnoses**: ICD-9, ICD-10 codes
- **💊 Medication prescriptions**: drug type, dosage, timing
- **🔬 Laboratory tests**: lab test results, vital measurements
- **🧪 Medical procedures**: procedures, surgeries
- **🧫 Clinical notes**: clinical notes (text data)
- **🩸 ICU data**: vital signs, ventilator settings, intensive care treatments
- **⚗️ Billing & Insurance**: treatment billing information, insurance codes
- See detailed dataset analysis here: [Documentation](https://colab.research.google.com/drive/14MG0qrJvCDtgT5EgvIRKHGU17OW_T3l0?usp=sharing)

#### Sources and Related Materials:
- Official site: https://physionet.org/content/mimiciv/
- Academic Journal: [Nature Paper](https://www-nature-com.translate.goog/articles/s41597-022-01899-x?error=cookies_not_supported&code=24abe187-8088-40fc-9ade-eae7426b86a1&_x_tr_sl=en&_x_tr_tl=vi&_x_tr_hl=vi&_x_tr_pto=tc)

## 🩹 System Architecture
![EHR Final drawio](https://github.com/user-attachments/assets/5cfdf71c-ae5f-4d52-a592-51f29dc98362)

## 🌡️ Infrastructure

| Component | Version | Container | Ports | Role |
|------------|-----------|-----------|-------|-----------------|
| **Spark Master** | 3.5.0 | `spark-master` | 8080 (UI), 7077 (Master) | Cluster coordination and worker management |
| **Spark Worker 1** | 3.5.0 | `spark-worker-1` | 8081 | Distributed data processing |
| **Spark Worker 2** | 3.5.0 | `spark-worker-2` | 8082 | Distributed data processing |
| **Spark Worker 3** | 3.5.0 | `spark-worker-3` | 8083 | Distributed data processing |
| **MinIO Node 1** | Latest | `minio1` | 9000 (API), 9001 (Console) | Object storage (S3-compatible) |
| **MinIO Node 2** | Latest | `minio2` | - | Distributed storage node |
| **MinIO Node 3** | Latest | `minio3` | - | Distributed storage node |
| **PostgreSQL** | 15.6 | `postgres_db` | 5432 | Relational database, metadata store |
| **Zookeeper** | 7.5.3 | `zookeeper` | 2181 | Required for Kafka |
| **Kafka** | 7.5.3 | `kafka` | 9092-9093 | Distributed event streaming platform |
| **Debezium** | 2.5 | `debezium` | 8087 | Change Data Capture |
| **Apache Flink** | 15.6 | `flink-jobmanager` | 8086 | Process streaming data |
| **Iceberg REST Catalog** | 0.10.0 | `iceberg-rest` | 8181 | Table format, schema management |
| **Trino** | 435 | `trino` | 8084 | Distributed SQL query engine |
| **Apache Airflow** | 2.8.1 | `airflow_standalone` | 8090 | Workflow orchestration |
| **DBT** | 1.7.8 | `dbt_service` | - | Data transformation framework |

<img width="1689" height="346" alt="image" src="https://github.com/user-attachments/assets/4bbc3eff-71c1-4752-ab67-9679f2aae759" />

## 🧬 Tech Stack
| Component | Purpose | Technology |
|-----------|---------|------------|
| **Streaming Data** | Data streaming extraction | Apache Flink, Kafka, Debezium |
| **ETL Pipeline** | Data transformation, loading | Apache Spark, Python |
| **Workflow Orchestration** | Schedule & monitor data pipelines | Apache Airflow, dbt |
| **Query Engine** | High-performance SQL analytics | Trino |
| **Lakehouse** | Scalable object storage | MinIO + Apache Iceberg |
| **Database Source** | Ingest\Extract data source to Lakehouse | PostgreSQL |
| **Data Quality** | Verify data quality | Great Expectation |
| **Visualization** | Business intelligence dashboards | Tableau |
| **Other tool** | Support for develop process | Git, DBeaver, Docker, Visual Studio Code, Claude/Grok/ChatGpt |

## 💉 Repository Structure
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

</details>

## 🚑 Getting Started
1. **Download Dataset**
  <pre>
    Download dataset on link: https://physionet.org/content/mimiciv/</pre>
2. **Clone my repository**
  <pre>
    git clone https://github.com/TruongDuyLongPTIT/Healthcare-Data-Driven-Hospital-EHR-Analysis.git</pre>
3. **Download JAR files for Apache Flink and Apache Spark**
   
  -----------Jars for Apache Flink-------------
  <pre>
    aws-java-sdk-bundle-1.12.648.jar
    bundle-2.20.18.jar
    flink-connector-base-1.18.1.jar
    flink-s3-fs-hadoop-1.18.1.jar
    flink-sql-connector-kafka-3.1.0-1.18.jar
    hadoop-aws-3.3.4.jar
    hadoop-common-3.3.4.jar
    hadoop-hdfs-3.3.4.jar
    hadoop-hdfs-client-3.3.4.jar
    iceberg-aws-1.5.0.jar
    iceberg-flink-runtime-1.18-1.5.0.jar</pre>  
    
  -----------Jars for Apache Spark-------------
  <pre>
    aws-java-sdk-bundle-1.12.262.jar
    hadoop-aws-3.3.4.jar
    iceberg-spark-runtime-3.5_2.12-1.6.0.jar</pre>
4. **Start and Stop**
  <pre>
    Double-click start.bat file to start all docker container
    Similar, double-click stop.bat to shut-down all docker containers.</pre>
5. **Run pipeline**
  <pre>
    Now, I running scripts manually.
    But, I will use Airflow to schedule running time (Comming soon...)</pre>
    
## 🩺 Guide project

**1. Extract data (Database Source -> Bronze Bucket) and Using Apache Iceberg to manage Parquet files as database-like tables.**
<pre>
    scripts/Extract/config.py
    scripts/Extract/ingest_mimic.py</pre>
**! Converting to streaming extract data (use Debezium, Kafka, Apache Flink)...**


https://github.com/user-attachments/assets/478877cc-fe15-4ede-90f2-0342fce44ab3


**2. Tranform data (Bronze Bucket -> Silver Bucket)**
<pre>
    scripts/Transform/chartevents_transform.py
    scripts/Transform/d_items_transform.py
    scripts/Transform/icustays_transform.py
    scripts/Transform/patients_transform.py
    scripts/Transform/time_normalization.py</pre>
    
**3. Load data (Silver Bucket -> Gold Bucket)**
<pre>
    scripts/Load/dimEventType.py
    scripts/Load/dimICUStay.py
    scripts/Load/dimPatients.py
    scripts/Load/dimTime.py
    scripts/Load/factICUVitalSignEvent
</pre>
! Right now, the **star schema only includes a few basic tables**. I will expand it further to fully leverage the value of the dataset.

<details>
<summary><b>🎥 Click to watch videos</b></summary>
<br>
  
https://github.com/user-attachments/assets/1fd65fa1-9d60-4751-9cd0-e76834551e57

https://github.com/user-attachments/assets/7a022613-043b-4254-921c-b646e0f88f99

</video>
</details>

**4. Query for Analytics**

~ I use **Trino as an intermediary to query data from the Data Model**, then feed it into Tableau to create dashboards.

![Dashboard](https://github.com/user-attachments/assets/674b9079-7766-4736-aa0e-8871db032fd5)

## 🔜 Check list
- [ ] I just do full load at first running time, need update logic ETL data from the second time onwards
- [ ] Set up dbt for manage SQL query
- [ ] Update transform logic 
- [ ] Set up Airflow for orchestration pipeline
- [ ] Logging and monitoring
- [ ] Feature engineering and training model
- [ ] Set up Great Expectation for Data Quality Verify
- [x] Set up Debezium for CDC and Kafka for streaming processing.
- [x] Build basic data pipeline

