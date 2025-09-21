from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataPipeline") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .getOrCreate()


file_path_hosp_module = "C:/DataUser/MIMIC Dataset/mimic-iv-3.1/hosp"


df = spark.read.csv(
    file_path_hosp_module + "/*.csv.gz",
    header=True,
    inferSchema=True
)

df.show(5)

df.write \
    .mode("overwrite") \
    .parquet("s3a://bronze-layer/")
