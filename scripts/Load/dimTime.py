from config import create_spark_session
from datetime import datetime, timedelta


def dimTime():
    spark = create_spark_session()
    # Tạo bảng dim_time với partitioning
    spark.sql("""
    CREATE OR REPLACE TABLE gold.dim_time 
    USING iceberg
    PARTITIONED BY (year, month)
    AS
    WITH times AS (
        SELECT explode(
            sequence(
                timestamp '2000-01-01 00:00:00.000',  -- thời gian bắt đầu
                timestamp '2025-01-01 00:00:00.000',  -- thời gian kết thúc
                interval 1 minute                 -- bước = 1 phút
            )
        ) AS full_datetime
    )
    SELECT
        -- smart key: YYYYMMDDHHMM00000
        CAST(date_format(full_datetime, 'yyyyMMddHHmm') || '00000' AS BIGINT) AS timekey,
        EXTRACT(YEAR FROM full_datetime) AS year,
        EXTRACT(MONTH FROM full_datetime) AS month,
        EXTRACT(DAY FROM full_datetime) AS day_of_month,
        date_format(full_datetime, 'HH:mm') AS hour_minute,
        EXTRACT(HOUR FROM full_datetime) AS hour,
        EXTRACT(MINUTE FROM full_datetime) AS minute,
        EXTRACT(QUARTER FROM full_datetime) AS quarter,
        EXTRACT(DAYOFWEEK FROM full_datetime) AS day_of_week,
        date_format(full_datetime, 'EEEE') AS day_name,
        date_format(full_datetime, 'MMMM') AS month_name,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM full_datetime) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE
            WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'  
            WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        full_datetime
    FROM times
    ORDER BY full_datetime
    """)
    
    print("✅ Bảng dim_time đã được tạo thành công với partitioning!")
    
    # Hiển thị sample data
    df_time = spark.table("gold.dim_time")
    print(f"📊 Tổng số records: {df_time.count():,}")
    
    print("\n📋 Sample data:")
    df_time.limit(10).show(truncate=False)
    
    # Test query performance với partition pruning
    print("\n🔍 Test partition pruning (chỉ lấy data tháng 1/2024):")
    spark.sql("""
    SELECT * FROM gold.dim_time
    WHERE year = 2024 AND month = 1
    """).show()
    
    spark.stop()



def main():
    dimTime()


if __name__ == "__main__":
    main()