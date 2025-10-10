# 🩻 [Healthcare] Hospital EHR Data Pipeline🩻 
*(The project is being upgraded and refined...)*
- I built a data pipeline project using the **MIMIC dataset** with a **lakehouse architecture**.
- The goal of this project is to **practice and strengthen my data engineering skill**s.
- It demonstrates my ability to design and implement modern data workflows for **real-world healthcare data**.
  
## 🧬 Bộ dữ liệu: MIMIC-IV Dataset

**MIMIC-IV (Medical Information Mart for Intensive Care IV)** là bộ dữ liệu y tế thực tế quy mô lớn, được công bố bởi MIT Lab for Computational Physiology, chứa dữ liệu ẩn danh của hàng chục nghìn bệnh nhân tại Beth Israel Deaconess Medical Center (Boston, MA).

#### Đặc Điểm Nổi Bật

| Đặc Điểm | Mô Tả |
|----------|-------|
| **Dữ Liệu Thực Tế** | Dữ liệu lâm sàng **thực tế** từ bệnh viện, không phải dữ liệu tổng hợp hay mô phỏng |
| **Quy Mô** | **~100GB** khi lưu trong RDBMS, hàng trăm triệu bản ghi |
| **Số Bệnh Nhân** | Hàng chục nghìn bệnh nhân ICU và nội trú |
| **Bảo Mật** | Dữ liệu đã được khử định danh hoàn toàn (HIPAA compliant) |
| **Phiên Bản** | MIMIC-IV v3.1 (latest) |

#### Nội Dung Dataset

**Bộ dữ liệu bao gồm:**
- **🤒 Thông tin nhân khẩu học của bệnh nhân**: Tuổi, giới tính, dân tộc
- **🩺 Chẩn đoán lâm sàng**: ICD-9, ICD-10 codes
- **💊 Kê đơn thuốc**: Loại thuốc, liều lượng, thời gian
- **🔬 Xét nghiệm**: Kết quả lab tests, đo lường sinh hiệu
- **🧪 Thủ thuật y tế**: Procedures, surgeries
- **🧫 Ghi chú lâm sàng**: Clinical notes (text data)
- **🩸 Dữ liệu ICU**: Vital signs, ventilator settings, điều trị hồi sức
- **⚗️ Billing & Insurance**: Thông tin dịch vụ tính phí điều trị, mã bảo hiểm
- Xem phân tích chi tiết bộ dữ liệu tại đây: [Documentation](https://colab.research.google.com/drive/14MG0qrJvCDtgT5EgvIRKHGU17OW_T3l0?usp=sharing)

#### Nguồn và tài liệu liên quan:
- Official site: https://physionet.org/content/mimiciv/
- Academic Journal: [Nature Paper](https://www-nature-com.translate.goog/articles/s41597-022-01899-x?error=cookies_not_supported&code=24abe187-8088-40fc-9ade-eae7426b86a1&_x_tr_sl=en&_x_tr_tl=vi&_x_tr_hl=vi&_x_tr_pto=tc)

## 🩹 System Architecture
![EHR final drawio](https://github.com/user-attachments/assets/f72fc993-9bac-4335-9c38-6cabdf86b6fa)

## 🌡️ Infrastructure

| Thành Phần | Phiên Bản | Container | Ports | Chức Năng Chính |
|------------|-----------|-----------|-------|-----------------|
| **Spark Master** | 3.5.0 | `spark-master` | 8080 (UI), 7077 (Master) | Điều phối cluster, quản lý workers |
| **Spark Worker 1** | 3.5.0 | `spark-worker-1` | 8081 | Xử lý dữ liệu phân tán |
| **Spark Worker 2** | 3.5.0 | `spark-worker-2` | 8082 | Xử lý dữ liệu phân tán |
| **Spark Worker 3** | 3.5.0 | `spark-worker-3` | 8083 | Xử lý dữ liệu phân tán |
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

## 🧬 Tech Stack
| Component | Purpose | Technology |
|-----------|---------|------------|
| **ETL Pipeline** | Data extraction, transformation, loading | Apache Spark, Python |
| **Workflow Orchestration** | Schedule & monitor data pipelines | Apache Airflow, dbt |
| **Query Engine** | High-performance SQL analytics | Trino |
| **Lakehouse** | Scalable object storage | MinIO + Apache Iceberg |
| **Database Source** | Ingest\Extract data source to Lakehouse | PostgreSQL |
| **Visualization** | Business intelligence dashboards | Tableau |

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
3. **Download JAR files for Spark**
  <pre>
    aws-java-sdk-bundle-1.12.262.jar
    hadoop-aws-3.3.4.jar
    iceberg-spark-runtime-3.5_2.12-1.6.0.jar</pre>
5. **Start and Stop**
  <pre>
    Double-click start.bat file to start all docker container
    Similar, double-click stop.bat to shut-down all docker containers.</pre>
6. **Run pipeline**
  <pre>
    Now, I running scripts manually.
    But, I will use Airflow to schedule running time (Comming soon...)</pre>
    
## 🩺 Guide project


<details>
<summary><b>🎥 Click để xem video</b></summary>
<br>
  
https://github.com/user-attachments/assets/1fd65fa1-9d60-4751-9cd0-e76834551e57

https://github.com/user-attachments/assets/7a022613-043b-4254-921c-b646e0f88f99

</video>
</details>


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
! Bây giờ, star schema mới chỉ có 1 số bảng cơ bản. Tôi sẽ mở rộng thêm nữa để khai thác tối đa giá trị của bộ dữ liệu.

**4. Query for Analytics**

~ Tôi dùng Trino làm trung gian để query dữ liệu từ Data Model rồi đưa vào Tableau để làm Dashboard.

![Dashboard](https://github.com/user-attachments/assets/674b9079-7766-4736-aa0e-8871db032fd5)

## 🔜 Check list
- [ ] Set up Debezium for CDC and Kafka for streaming processing.
- [ ] I just do full load at first running time, need update logic ETL data from the second time onwards
- [ ] Set up dbt for manage SQL query
- [ ] Update transform logic 
- [ ] Set up Airflow for orchestration pipeline
- [ ] Logging and monitoring
- [x] Build basic data pipeline

