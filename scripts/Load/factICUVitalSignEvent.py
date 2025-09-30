from config import create_spark_session
from datetime import datetime, timedelta

def createFactICUVitalSignEvent():
    spark = create_spark_session()

    spark.sql("""
        CREATE OR REPLACE TABLE gold.factICUVitalSignEvent AS
        SELECT 
            row_number() OVER (ORDER BY subject_id) as ID,
            subject_id,
            stay_id,
            itemid,
            CAST(date_format(charttime, 'yyyyMMddHHmm') || '00000' AS BIGINT) AS timekey,
            valuenum as value_of_measurements, -- giá trị tương ứng của phép đo chỉ số sức khỏe
            valueuom as unit_of_measurements
        FROM silver.chartevents
        WHERE itemid IN (220045, 220050, 220051, 220052, 220277)
    """)


    # Đọc từ table
    df = spark.sql("""
        SELECT 
            f.ID,
            f.subject_id,
            f.stay_id,
            f.itemid,
            f.timekey,
            de.vital_sign_type,
            f.value_of_measurements,
            f.unit_of_measurements
        FROM gold.factICUVitalSignEvent f
        LEFT JOIN gold.dimEventType de ON f.itemid = de.itemid
        WHERE f.itemid IN (220050, 220051, 220052)
        LIMIT 1000
    """)
    df.show(100)
    spark.stop()


def main():
    createFactICUVitalSignEvent()


if __name__ == "__main__":
    main()