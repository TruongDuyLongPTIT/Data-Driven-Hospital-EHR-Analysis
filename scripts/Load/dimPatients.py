from config import create_spark_session
from datetime import datetime, timedelta

def createDimPatients():
    spark = create_spark_session()

    spark.sql("""
        CREATE OR REPLACE TABLE gold.dimPatient AS
        SELECT 
            subject_id,
            gender,
            anchor_age as age
        FROM silver.patients 
    """)


    print("🚀🚀🚀🚀🚀🚀 Print dimEventType")
    df = spark.sql("""
        SELECT * FROM gold.dimPatient
    """)
    df.show(100)

    spark.stop()


def main():
    createDimPatients()


if __name__ == "__main__":
    main()