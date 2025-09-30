from pyspark.sql.functions import col, lit, count, when
from config import create_spark_session

def debug_which_join_fails():
    spark = create_spark_session()
    
    print("=" * 70)
    print("KIỂM TRA TỪNG BƯỚC JOIN")
    print("=" * 70)
    
    # Load chartevents với filter
    chartevents = spark.sql("""
        SELECT stay_id, itemid
        FROM silver.chartevents
        WHERE itemid IN (220045, 220050, 220051, 220052, 220277)
    """)
    
    print("\n1. Tổng số bản ghi chartevents sau filter:")
    chartevents.agg(
        count("*").alias("total"),
        count("stay_id").alias("non_null_stay_id"),
        count(when(col("stay_id").isNull(), 1)).alias("null_stay_id")
    ).show()
    
    print("\n2. Sample stay_id từ chartevents:")
    chartevents.select("stay_id").distinct().show(10)
    
    # Load dimICUStay
    dimICUStay = spark.sql("SELECT stay_id FROM gold.dimICUStay")
    
    print("\n3. Sample stay_id từ dimICUStay:")
    dimICUStay.select("stay_id").distinct().show(10)
    
    # Kiểm tra kiểu dữ liệu bằng DataFrame schema
    print("\n4. Kiểm tra kiểu dữ liệu stay_id:")
    print(f"chartevents.stay_id: {chartevents.schema['stay_id'].dataType}")
    print(f"dimICUStay.stay_id: {dimICUStay.schema['stay_id'].dataType}")
    
    # Thử join đơn giản
    print("\n5. Thử join đơn giản:")
    try:
        joined = chartevents.join(
            dimICUStay,
            on="stay_id",
            how="inner"
        )
        
        result_count = joined.count()
        print(f"✓ Join thành công! Số bản ghi: {result_count}")
        
        if result_count > 0:
            print("\nMẫu kết quả join:")
            joined.show(10)
        else:
            print("\n⚠ Join thành công nhưng không có bản ghi nào!")
            print("Kiểm tra overlap giữa 2 bảng:")
            
            chart_ids = chartevents.select("stay_id").distinct()
            dim_ids = dimICUStay.select("stay_id").distinct()
            
            overlap = chart_ids.intersect(dim_ids).count()
            print(f"Số stay_id trùng nhau: {overlap}")
            
    except Exception as e:
        print(f"✗ Join thất bại với lỗi: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_which_join_fails()