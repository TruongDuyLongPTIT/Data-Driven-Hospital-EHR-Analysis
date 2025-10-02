# 🩻 [Healthcare] Hospital EHR Data Pipeline🩻
- I built a data pipeline project using the **MIMIC dataset** with a **lakehouse architecture**.
- The goal of this project is to **practice and strengthen my data engineering skill**s.
- It demonstrates my ability to design and implement modern data workflows for **real-world healthcare data**.

## 🛰️ System Architecture
![finalllllllll](https://github.com/user-attachments/assets/58fe9ecb-798a-41b5-b84d-a4990f58ce3c)

## 🗃️ Repository Structure
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
