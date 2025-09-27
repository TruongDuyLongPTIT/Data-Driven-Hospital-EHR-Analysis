from config import create_spark_session
from pyspark.sql.functions import trim, col
<<<<<<< HEAD
from time_normalization import normalize_time
=======
>>>>>>> 6586a69ee0b67430d94871a3046d9fa38d12ce86

def transform_patients():
    # Tạo Spark Session
    spark = create_spark_session()


    print("🚀 patients table processing...")
<<<<<<< HEAD
    # Table Schema
    print("🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀 Printing bronze patients schema...")
=======
    # bổ sung 1 số chỉ số cần thiết đang bị thiếu
>>>>>>> 6586a69ee0b67430d94871a3046d9fa38d12ce86
    spark.sql("""DESCRIBE TABLE bronze.patients""").show()

    # Load table lên Spark DataFrame
    df_patients = spark.table("bronze.patients")
<<<<<<< HEAD
    df_patients.printSchema()
=======
>>>>>>> 6586a69ee0b67430d94871a3046d9fa38d12ce86

    # Remove duplicate records
    df_patients = df_patients.drop_duplicates()

    # Xử lý NULL
    df_patients = df_patients.dropna(subset=['subject_id', 'gender', 'anchor_age', 'anchor_year'])
<<<<<<< HEAD
    # Normalization time
    df_patients = df_patients.withColumn('dod', normalize_time('dod'))
=======
>>>>>>> 6586a69ee0b67430d94871a3046d9fa38d12ce86

    # Loại bỏ 1 số case bị thừa khoảng trắng đầu/cuối chuỗi
    string_cols = ["gender", "anchor_year_group"]

    for column_name in string_cols:
        df_patients = df_patients.withColumn(column_name, trim(col(column_name)))

<<<<<<< HEAD
    df_patients.printSchema()

    df_patients.write \
            .format("iceberg") \
            .mode("overwrite") \
=======
    df_patients.write \
            .format("iceberg") \
            .mode("append") \
>>>>>>> 6586a69ee0b67430d94871a3046d9fa38d12ce86
            .saveAsTable("silver.patients")

    # spark.table("silver.patients").show(100)
    print("📊In bronze")
    spark.sql("SELECT * FROM bronze.patients LIMIT 100").show()
    print("📊In silver")
    spark.sql("SELECT * FROM silver.patients LIMIT 100").show() # Tại sao sau khi xử lý thì thứ tự của bảng trong silver lại khác trong bronze nhỉ?
<<<<<<< HEAD
    print("🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀 Printing silver patients schema...")
    spark.sql("DESCRIBE TABLE silver.patients").show(truncate=False)
    
=======

>>>>>>> 6586a69ee0b67430d94871a3046d9fa38d12ce86
    # Shutdown Spark Session
    spark.stop()


def main():
    transform_patients()

if __name__ == "__main__":
    main()