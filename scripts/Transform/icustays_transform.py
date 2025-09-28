from config import create_spark_session
from time_normalization import normalize_time
from pyspark.sql.functions import trim, col

def transform_icustays():
    # Tạo Spark Session
    spark = create_spark_session()


    print("🚀 icustays table processing...")
    # Table schema
    spark.sql("""DESCRIBE TABLE bronze.icustays""").show()

    # Load table lên Spark DataFrame
    df_icustays = spark.table("bronze.icustays")

    df_icustays.printSchema()
    # Remove duplicate records
    df_icustays = df_icustays.drop_duplicates()

    # Time normalization
    print("🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀 Printing bronze icustays schema...")
    spark.sql("DESCRIBE TABLE bronze.icustays").show(truncate=False)

    df_icustays = df_icustays.withColumn('intime', normalize_time('intime'))
    df_icustays = df_icustays.withColumn('outtime', normalize_time('outtime'))

    # Xử lý NULL
    df_icustays = df_icustays.dropna(subset=['stay_id', 'subject_id', 'hadm_id'])
    df_icustays = df_icustays.fillna({'first_careunit': 'ICU', 'last_careunit': 'ICU'})

    # Loại bỏ 1 số case bị thừa khoảng trắng đầu/cuối chuỗi
    string_cols = ["first_careunit", "last_careunit"]

    for column_name in string_cols:
        df_icustays = df_icustays.withColumn(column_name, trim(col(column_name)))

    df_icustays.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("silver.icustays")
    
    spark.table("silver.icustays").show()
    print("📊In bronze")
    spark.sql("SELECT * FROM bronze.icustays LIMIT 100").show()
    print("📊In silver")
    spark.sql("SELECT * FROM silver.icustays LIMIT 100").show()

    print("🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀 Printing silver icustays schema...")
    spark.sql("DESCRIBE TABLE bronze.icustays").show(truncate=False)
    # Shutdown Spark Session
    spark.stop()


def main():
    transform_icustays()

if __name__ == "__main__":
    main()