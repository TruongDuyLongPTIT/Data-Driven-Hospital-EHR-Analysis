from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Database connection config
DB_CONFIG = {
    "url": "jdbc:postgresql://postgres_db:5432/postgres",
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

MINIO_CONFIG = {
    "bronze_path": "s3a://mimic-lakehouse/bronze",
    "silver_path": "s3a://mimic-lakehouse/silver", 
    "gold_path": "s3a://mimic-lakehouse/gold"
}

MIMIC_TABLES = [
    "admissions",
    "caregiver",
    "chartevents",
    "d_hcpcs",
    "d_icd_diagnoses",
    "d_icd_procedures",
    "d_items",
    "d_labitems",
    "datetimeevents",
    "diagnoses_icd",
    "drgcodes",
    "emar",
    "emar_detail",
    "hcpcsevents",
    "icustays",
    "ingredientevents",
    "inputevents",
    "labevents",
    "microbiologyevents",
    "omr",
    "outputevents",
    "patients",
    "pharmacy",
    "poe",
    "poe_detail",
    "prescriptions",
    "procedureevents",
    "procedures_icd",
    "services",
    "transfers"
]

TABLE_CONFIG = {
    # Bảng to thì mới cần chia ra để load song song, bảng có ít records thì không cần
    
    "admissions": { # Hơn 500k records
        "partition_column": "hadm_id", 
        "partition_bounds": (20000000, 30000000),
        "num_partition": 2,
        "fetch_size": 50000
    },
    "caregiver": {  # Hơn 100k records
        "partition_column": "caregiver_id", 
        "partition_bounds": (1, 100005),
        "num_partition": 2,
        "fetch_size": 50000
    },
    "chartevents": {  # Gan 40gb Hơn 400 triệu records
        "partition_column": "charttime", 
        "partition_bounds": ("2109-07-25 00:00:00.000", "2214-07-26 16:00:00.000"),
        "num_partition": 200, # Thử chia 60 partition thì nó bị tràn ram, đang thử tăng x10 thì được. Sau tìm hiểu kỹ hơn phần chia partition này
        "fetch_size": 50000
    },
    "d_hcpcs": { # Ít record, không có cột phù hợp để chia partition => Không chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    },
    "d_icd_diagnoses": { # Ít record, không có cột phù hợp để chia partition => Không chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    },
    "d_icd_procedures": { # Ít record, không có cột phù hợp để chia partition => Không chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    },
    "d_items": { # Ít record, không có cột phù hợp để chia partition => Không chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 5000
    },
    "d_labitems": { # Ít record, không có cột phù hợp để chia partition => Không chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 1000
    },
    "datetimeevents": { # 900Mb, 10 triệu records
        "partition_column": "charttime", 
        "partition_bounds": ("2110-01-11 12:40:00.000", "2214-07-26 08:00:00.000"),
        "num_partition": 12,
        "fetch_size": 50000
    },
    "diagnoses_icd": { # 300Mb, 6.4 triệu records, cột subject_id không lý tưởng lắm để làm partition column nhưng không còn cột nào tốt hơn
        "partition_column": "subject_id", 
        "partition_bounds": (10000000, 20000000),
        "num_partition": 12,
        "fetch_size": 50000
    },
    "drgcodes": { # 67Mb, không có cột lý tưởng => Không chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    },
    "emar": { # 5.6GB, 42 triệu bản ghi, emar_id là dạng string ko dùng làm partition column được, dùng charttime
        "partition_column": "charttime", 
        "partition_bounds": ("2109-03-17 10:15:00.000", "2215-01-03 12:48:00.000"),
        "num_partition": 72,
        "fetch_size": 50000
    },
    "emar_detail": { # 9GB, gần 90 triệu bản ghi, dùng subject_id cũng tạm ổn mặc dù trùng nhiều
        "partition_column": "subject_id", 
        "partition_bounds": (10000000, 20000000),
        "num_partition": 150,
        "fetch_size": 50000
    },
    "hcpcsevents": { # Hơn 100k bản ghi, có 14 Mb thôi
        "partition_column": "chartdate", 
        "partition_bounds": ("2106-02-06", "2214-08-15"),
        "num_partition": 2,
        "fetch_size": 20000
    },
    "icustays": { # 12Mb, 100k bản ghi
        "partition_column": "intime", 
        "partition_bounds": ("2110-01-11 10:16:06.000", "2214-07-22 17:05:53.000"),
        "num_partition": 2,
        "fetch_size": 20000
    },
    "ingredientevents": { # 1.9GB, 15 triệu records
        "partition_column": "storetime", 
        "partition_bounds": ("2110-01-11 12:48:00.000", "2211-11-27 08:49:00.000"),
        "num_partition": 18,
        "fetch_size": 50000
    },
    "inputevents": { # 2.5gb 11 triệu records
        "partition_column": "storetime", 
        "partition_bounds": ("2110-01-11 12:48:00.000", "2211-11-27 08:49:00.000"),
        "num_partition": 36,
        "fetch_size": 50000
    },
    "labevents": { # 18gb 160 triệu records
        "partition_column": "storetime", 
        "partition_bounds": ("2105-01-19 12:26:00.000", "2215-01-12 15:59:00.000"),
        "num_partition": 152,
        "fetch_size": 50000
    },
    "microbiologyevents": { # ~ 800mb 4 triệu records
        "partition_column": "chartdate", 
        "partition_bounds": ("2105-01-19 00:00:00.000", "2215-01-16 00:00:00.000"),
        "num_partition": 12,
        "fetch_size": 50000
    },
    "omr": { # ~ 500mb 8 triệu records
        "partition_column": "chartdate", 
        "partition_bounds": ("2105-01-19", "2215-04-14"),
        "num_partition": 12,
        "fetch_size": 50000
    },
    "outputevents": { # ~ 500mb 6 triệu records
        "partition_column": "charttime", 
        "partition_bounds": ("2110-01-11 12:30:00.000", "2214-07-26 14:00:00.000"),
        "num_partition": 12,
        "fetch_size": 50000
    },
    "patients": { # ~ 21mb 400k records
        "partition_column": "subject_id", 
        "partition_bounds": (10000000, 20000000),
        "num_partition": 2,
        "fetch_size": 50000
    },
    "pharmacy": { # 3.4gb 18 triệu records
        "partition_column": "entertime", 
        "partition_bounds": ("2105-10-04 17:47:05.000", "2214-12-24 11:26:21.000"),
        "num_partition": 64,
        "fetch_size": 50000
    },
    "poe": { # ~ 5.8gb 52 triệu records
        "partition_column": "order_time", 
        "partition_bounds": ("2105-10-04 17:34:15.000", "2214-12-24 13:29:21.000"),
        "num_partition": 98,
        "fetch_size": 50000
    },
    "poe_detail": { # ~700mb 9 triệu records nhưng không có cột hợp lý làm partition column, nhiều record nma cũng ko nặng lắm => Không chia partition
        # "partition_column":
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    },
    "prescriptions": { # ~ 3.5gb 20 triệu records
        "partition_column": "starttime", 
        "partition_bounds": ("2105-10-04 18:00:00.000", "2214-12-24 12:00:00.000"),
        "num_partition": 64,
        "fetch_size": 50000
    },
    "procedureevents": { # Kich thước nhỏ, ít bản ghi => Không càn chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition":
        "fetch_size": 50000
    },
    "procedures_icd": { # Kich thước nhỏ, ít bản ghi => Không càn chia partition
        # "partition_column":
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    },
    "services": { # Kich thước nhỏ, ít bản ghi => Không càn chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    },
    "transfers": { # Kich thước nhỏ, ít bản ghi => Không càn chia partition
        # "partition_column": 
        # "partition_bounds": 
        # "num_partition": 
        "fetch_size": 50000
    }
}


def create_spark_session():
    conf = SparkConf()
    
    # Spark App Configuration
    conf.set("spark.app.name", "MIMIC-IV-Dataset-Ingestion")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enable", "true")

    # Iceberg configuration
    conf.set("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.bronze.type", "hadoop")
    conf.set("spark.sql.catalog.bronze.warehouse", "s3a://mimic-lakehouse/bronze")

    conf.set("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.silver.type", "hadoop")
    conf.set("spark.sql.catalog.silver.warehouse", "s3a://mimic-lakehouse/silver")

    conf.set("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.gold.type", "hadoop")
    conf.set("spark.sql.catalog.gold.warehouse", "s3a://mimic-lakehouse/gold")
    
    # MinIO S3 Configuration
    conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1:9000")
    conf.set("spark.hadoop.fs.s3a.access.key", "minio")
    conf.set("spark.hadoop.fs.s3a.secret.key", "minio123")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    # Performance tuning
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
    
    return spark

