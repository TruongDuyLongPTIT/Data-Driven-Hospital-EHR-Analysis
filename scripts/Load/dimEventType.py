from config import create_spark_session
from datetime import datetime, timedelta

def createDimEventType():
    spark = create_spark_session()

    spark.sql("""
        CREATE OR REPLACE TABLE gold.dimEventType AS
        SELECT 
            row_number() OVER (ORDER BY itemid) as eventtypeSK,
            itemid as itemid_eventtypeDK,
            label as vital_sign_type,
            lownormalvalue,
            highnormalvalue
        FROM silver.d_items 
        WHERE itemid in (220045, 220050, 220051, 220052, 220277) -- tạm thời báo cáo chỉ cần các chỉ số: heart rate, blood pressure, và spo2 thôi, sau cần thì load thêm
    """)


    print("🚀🚀🚀🚀🚀🚀 Print dimEventType")
    df = spark.sql("""
        SELECT * FROM gold.dimEventType
        WHERE itemid_eventtypeDK in (220045, 220050, 220051, 220052, 220277)
    """)
    df.show(100)

    spark.stop()


def main():
    createDimEventType()


if __name__ == "__main__":
    main()