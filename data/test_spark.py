from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestApp").getOrCreate()

df = spark.read.csv("/opt/bitnami/spark/data/people.csv", header=True, inferSchema=True)
df.show()

df.groupBy("city").count().show()

spark.stop()
