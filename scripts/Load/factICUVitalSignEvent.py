from config import create_spark_session
from datetime import datetime, timedelta

def createFactICUVitalSignEvent():
    spark = create_spark_session()

    spark.sql("""
        CREATE OR REPLACE TABLE gold.factICUVitalSignEvent AS
        SELECT 
            row_number() OVER (ORDER BY subject_id) as ID,
            itemid as eventtypeSK,
            subject_id as patientSK,
            stay_id as icustaySK,
            CAST(date_format(charttime, 'yyyyMMddHHmm') || '00000' AS BIGINT) AS timekey,
            valuenum as value_of_measurements, -- giá trị tương ứng của phép đo chỉ số sức khỏe
            valueuom as unit_of_measurements
        FROM silver.chartevents
        WHERE itemid IN (220045, 220050, 220051, 220052, 220277)
    """)


    # Đọc từ table
    df = spark.sql("SELECT * FROM gold.factICUVitalSignEvent WHERE eventtypeSK in (220050, 220051, 220052)")
    df.show(1000)
    spark.stop()


def main():
    createFactICUVitalSignEvent()


if __name__ == "__main__":
    main()