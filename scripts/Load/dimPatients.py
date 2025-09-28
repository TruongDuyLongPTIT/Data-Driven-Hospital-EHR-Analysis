from config import create_spark_session
from datetime import datetime, timedelta

def createDimPatients():
    spark = create_spark_session()

    spark.sql("""
        CREATE OR REPLACE TABLE gold.dimPatient AS
        SELECT 
            row_number() OVER (ORDER BY subject_id) as patientSK,
            subject_id as patientDK,
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