from config import create_spark_session
from datetime import datetime, timedelta

def createDimICUStay():
    spark = create_spark_session()

    spark.sql("""
        CREATE OR REPLACE TABLE gold.dimICUStay AS
        SELECT 
            stay_id,
            first_careunit as ICU_room_name,
            intime,
            outtime
        FROM silver.icustays 
    """)


    print("🚀🚀🚀🚀🚀🚀 Print dimICUStay")
    df = spark.sql("""
        SELECT * FROM gold.dimICUStay
    """)
    df.show(100)

    spark.stop()


def main():
    createDimICUStay()


if __name__ == "__main__":
    main()