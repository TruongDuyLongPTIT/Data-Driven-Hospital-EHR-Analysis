from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Database connection config
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/postgres",
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

MINIO_CONFIG = {
    "bucket": 'bronze-layer',
    "bronze_path": "s3a://mimic-lakehouse/bronze",
    "silver_path": "s3a://mimic-lakehouse/silver", 
    "gold_path": "s3a://mimic-lakehouse/gold"
}



def create_spark_session():
    conf = SparkConf()

    # Spark App Configuration
    conf.set("spark.app.name", "MIMIC-IV-Dataset-Ingestion")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalesceParrtitions.enable", "true")

    # Delta Lake Configuration
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # MinIO S3 Configuration
    conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    conf.set("spark.hadoop.fs.s3a.access.key", "minio")
    conf.set("spark.hadoop.fs.s3a.secret.key", "minio123")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    # Performance tuning
    conf.set("spark.sql.execution.arrow.pyspark.enable", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
    
    return spark