from config import create_spark_session
from datetime import datetime, timedelta

def createFactICUVitalSignEvent():
    spark = create_spark_session()

    # Thông thường Blood Pressure dia/sys/mean (của mỗi bệnh nhân trong 1 lần nhập viện ICU) sẽ được ghi nhận cùng 1 thời điểm (mỗi lần đo cách nhau 3-4 phút)
    # Nhưng thỉnh thoảng sẽ bị chênh nhau 1 phút ví dụ: Blood Pressure dia: 13:04, Blood Pressure sys: 13:05, Blood Pressure mean: 13:05
    # Điều này khiến khi làm line-chart sẽ bị đứt gãy không liền mạch
    # Do đó, cần chuẩn hóa thành group time
    # Logic:
        # Sau khi sort theo thời gian, thì cứ lần lượt 1 nhóm 3  bản ghi sẽ có cùng thời điểm
        # Nếu trong 1 nhóm mà có 1 bản ghi chênh 1 phút với 2 bản ghi còn lại, thì sẽ chuẩn hóa thời gian (HH:mm) của 1 bản ghi đó theo 2 bản ghi kia
        # Thực ra không cần đếm tần suất xem thời gian nào xuất hiện nhiều hơn, trong nhóm 3 cứ chuẩn hóa theo cái ở giữa là auto đúng 
        # Vì, (Bản ghi giữa (thứ 2) → thời gian của nó chắc chắn giống với ít nhất 1 trong 2 bản ghi còn lại)
    # Kết quả kỳ vọng:
    #     13:04 (1 lần) → time_group = 13:05 (vì 13:05 xuất hiện 2 lần)
    #     13:05 (2 lần) → time_group = 13:05
    
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
        WHERE f.subject_id = 10045991 AND f.stay_id = 34889777 and f.itemid IN (220050, 220051, 220052)
    """)
    df.show(1000)
    spark.stop()


def main():
    createFactICUVitalSignEvent()


if __name__ == "__main__":
    main()