from config import create_spark_session
from time_normalization import normalize_time
from pyspark.sql.functions import trim, col, concat, date_format, year


def transform_chartevents():
    # Tạo Spark Session
    spark = create_spark_session()


    print("🚀 chartevents table processing...")
    # Table schema
    spark.sql("""DESCRIBE TABLE bronze.chartevents""").show()

    # Load table lên Spark DataFrame
    df_chartevents = spark.table("bronze.chartevents")

    # Remove duplicate records
    # df_chartevents = df_chartevents.drop_duplicates() # -> Bảng chartevents này quá lớn mà drop_duplicate() yêu cầu phải load cả bảng lên, nhưng mà các worker không đủ ram, nên chưa biết xử lý như nào

    # Xử lý NULL
    df_chartevents = df_chartevents.dropna(subset=['subject_id', 'stay_id', 'itemid'])
    df_chartevents = df_chartevents.fillna({'warning': 0})

    # Loại bỏ 1 số case bị thừa khoảng trắng đầu/cuối chuỗi
    string_cols = ["value", "valueuom"]

    for column_name in string_cols:
        df_chartevents = df_chartevents.withColumn(column_name, trim(col(column_name)))

    # Normalization charttime
    # Vi bo du lieu MIMIC IV bi dich thoi gian nen can chuan hoa lai de cho hop ly 
    # Va tranh khoang thoi gian qua dai, dan den viec sinh ra bang DimTime sau nay qua lon. Khoang thoi gian hien tai la hon 100 nam, voi interval 1 phut / 1 ban ghi thi phai tao ra rat nhieu ban ghi
    # => Do do can chuan hoa lai cho hop ly va tranh khoang thoi gian qua dai

    df_chartevents = df_chartevents.withColumn('charttime', normalize_time('charttime'))
    
    df_chartevents = df_chartevents.withColumn('storetime', normalize_time('storetime'))

    df_chartevents.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("silver.chartevents")
    
    spark.table("silver.chartevents").show()
    print("📊In bronze")
    spark.sql("SELECT * FROM bronze.chartevents LIMIT 100").show()
    print("📊In silver")
    spark.sql("SELECT * FROM silver.chartevents LIMIT 100").show()
    
    # Test 1: 1 file
    # df1 = spark.read.parquet("bronze.chartevents.data.00000-201-cb7a543f-867b-49df-9296-1a885053ab8f-00001.parquet")
    # df1.dropDuplicates().collect()  # Xem Spark UI -> sẽ thấy ít task

    # Test 2: 3 files  
    # df3 = spark.read.parquet(
    #     *[
    #         "s3a://mimic-lakehouse/bronze/chartevents/data/00000-201-cb7a543f-867b-49df-9296-1a885053ab8f-00001.parquet",
    #         "s3a://mimic-lakehouse/bronze/chartevents/data/00001-202-cb7a543f-867b-49df-9296-1a885053ab8f-00001.parquet",
    #         "s3a://mimic-lakehouse/bronze/chartevents/data/00002-203-cb7a543f-867b-49df-9296-1a885053ab8f-00001.parquet"
    #     ]
    # )

    # df3.dropDuplicates().collect()  # Xem Spark UI -> sẽ thấy nhiều task hơn
    # Shutdown Spark Session
    spark.stop()


def main():
    transform_chartevents()

if __name__ == "__main__":
    main()