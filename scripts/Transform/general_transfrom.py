from Extract.config import create_spark_session

spark = create_spark_session()

df = spark.read.parquet("s3a://mimic-lakehouse/bronze/caregiver")

print(df.count())
df.printSchema()