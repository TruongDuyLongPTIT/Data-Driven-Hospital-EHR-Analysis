from config import create_spark_session
from pyspark.sql.functions import trim, col

def transform_d_items():
    # Tạo Spark Session
    spark = create_spark_session()


    print("🚀 d_items table processing...")
    # Table schema
    spark.sql("""DESCRIBE TABLE bronze.d_items""").show()

    # bổ sung 1 số chỉ số cần thiết đang bị thiếu
    spark.sql("""
        UPDATE bronze.d_items
        SET lownormalvalue = 60,
            highnormalvalue = 100
        WHERE itemid = 220045 -- bổ sung khoảng giá trị bình thường cho heart rate 
    """
    )

    spark.sql("""
        UPDATE bronze.d_items
        SET lownormalvalue = 70,
            highnormalvalue = 100
        WHERE itemid = 220052 -- bổ sung khoảng giá trị bình thường cho heart rate 
    """
    )

    spark.sql("""
        UPDATE bronze.d_items
        SET lownormalvalue = 90,
            highnormalvalue = 100
        WHERE itemid = 220277 -- bổ sung khoảng giá trị bình thường cho heart rate 
    """
    )

    # Load table lên Spark DataFrame
    df_d_items = spark.table("bronze.d_items")

    # Remove duplicate records
    df_d_items = df_d_items.drop_duplicates()

    # Xử lý NULL
    df_d_items = df_d_items.dropna(subset=['itemid', 'label', 'linksto', 'category'])
    df_d_items = df_d_items.fillna({'abbreviation': 'N/A', 'unitname': 'N/A', 'param_type': 'N/A', 'lownormalvalue': -1, 'highnormalvalue': -1})

    # Loại bỏ 1 số case bị thừa khoảng trắng đầu/cuối chuỗi
    string_cols = ["label", "abbreviation", "linksto", "category", "unitname", "param_type"]

    for column_name in string_cols:
        df_d_items = df_d_items.withColumn(column_name, trim(col(column_name)))

    df_d_items.write \
            .format("iceberg") \
<<<<<<< HEAD
            .mode("overwrite") \
=======
            .mode("append") \
>>>>>>> 6586a69ee0b67430d94871a3046d9fa38d12ce86
            .saveAsTable("silver.d_items")
    
    spark.table("silver.d_items").show()

    # Shutdown Spark Session
    spark.stop()


def main():
    transform_d_items()

if __name__ == "__main__":
    main()